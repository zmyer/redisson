/**
 * Copyright (c) 2013-2019 Nikita Koksharov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.redisson.spring.session;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.redisson.api.RMap;
import org.redisson.api.RPatternTopic;
import org.redisson.api.RSet;
import org.redisson.api.RTopic;
import org.redisson.api.RedissonClient;
import org.redisson.api.listener.PatternMessageListener;
import org.redisson.client.codec.StringCodec;
import org.redisson.codec.CompositeCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.session.FindByIndexNameSessionRepository;
import org.springframework.session.MapSession;
import org.springframework.session.Session;
import org.springframework.session.events.SessionCreatedEvent;
import org.springframework.session.events.SessionDeletedEvent;
import org.springframework.session.events.SessionExpiredEvent;

/**
 * 
 * @author Nikita Koksharov
 *
 */
public class RedissonSessionRepository implements FindByIndexNameSessionRepository<RedissonSessionRepository.RedissonSession>, 
                                                    PatternMessageListener<String> {

    final class RedissonSession implements Session {

        private String principalName;
        private final MapSession delegate;
        private RMap<String, Object> map;

        public RedissonSession() {
            this.delegate = new MapSession();
            map = redisson.getMap(keyPrefix + delegate.getId(), new CompositeCodec(StringCodec.INSTANCE, redisson.getConfig().getCodec()));
            principalName = resolvePrincipal(delegate);

            Map<String, Object> newMap = new HashMap<String, Object>(3);
            newMap.put("session:creationTime", delegate.getCreationTime().toEpochMilli());
            newMap.put("session:lastAccessedTime", delegate.getLastAccessedTime().toEpochMilli());
            newMap.put("session:maxInactiveInterval", delegate.getMaxInactiveInterval().getSeconds());
            map.putAll(newMap);

            updateExpiration();
            
            String channelName = getEventsChannelName(delegate.getId());
            RTopic topic = redisson.getTopic(channelName, StringCodec.INSTANCE);
            topic.publish(delegate.getId());
        }

        private void updateExpiration() {
            if (delegate.getMaxInactiveInterval().getSeconds() >= 0) {
                map.expire(delegate.getMaxInactiveInterval().getSeconds(), TimeUnit.SECONDS);
            }
        }
        
        public RedissonSession(String sessionId) {
            this.delegate = new MapSession(sessionId);
            map = redisson.getMap(keyPrefix + sessionId, new CompositeCodec(StringCodec.INSTANCE, redisson.getConfig().getCodec()));
            principalName = resolvePrincipal(delegate);
        }
        
        public void delete() {
            map.delete();
        }

        public boolean load() {
            Set<Entry<String, Object>> entrySet = map.readAllEntrySet();
            for (Entry<String, Object> entry : entrySet) {
                if ("session:creationTime".equals(entry.getKey())) {
                    delegate.setCreationTime(Instant.ofEpochMilli((Long) entry.getValue()));
                } else if ("session:lastAccessedTime".equals(entry.getKey())) {
                    delegate.setLastAccessedTime(Instant.ofEpochMilli((Long) entry.getValue()));
                } else if ("session:maxInactiveInterval".equals(entry.getKey())) {
                    delegate.setMaxInactiveInterval(Duration.ofSeconds((Long) entry.getValue()));
                } else {
                    delegate.setAttribute(entry.getKey(), entry.getValue());
                }
            }
            return !entrySet.isEmpty();
        }
        
        @Override
        public String getId() {
            return delegate.getId();
        }

        @Override
        public <T> T getAttribute(String attributeName) {
            return delegate.getAttribute(attributeName);
        }

        @Override
        public Set<String> getAttributeNames() {
            return delegate.getAttributeNames();
        }

        @Override
        public void setAttribute(String attributeName, Object attributeValue) {
            if (attributeValue == null) {
                removeAttribute(attributeName);
                return;
            }

            delegate.setAttribute(attributeName, attributeValue);

            if (map != null) {
                map.fastPut(attributeName, attributeValue);
                
                String principalSessionAttr = getSessionAttrNameKey(PRINCIPAL_NAME_INDEX_NAME);
                String securityPrincipalSessionAttr = getSessionAttrNameKey(SPRING_SECURITY_CONTEXT);
                
                if (attributeName.equals(principalSessionAttr)
                        || attributeName.equals(securityPrincipalSessionAttr)) {
                    // remove old
                    if (principalName != null) {
                        RSet<String> set = getPrincipalSet(principalName);
                        set.remove(getId());
                    }
                    
                    principalName = resolvePrincipal(this);
                    if (principalName != null) {
                        RSet<String> set = getPrincipalSet(principalName);
                        set.add(getId());
                    }
                }
            }
        }
        
        public void clearPrincipal() {
            principalName = resolvePrincipal(this);
            if (principalName != null) {
                RSet<String> set = getPrincipalSet(principalName);
                set.remove(getId());
            }
        }

        @Override
        public void removeAttribute(String attributeName) {
            delegate.removeAttribute(attributeName);

            if (map != null) {
                map.fastRemove(attributeName);
            }
        }

        @Override
        public Instant getCreationTime() {
            return delegate.getCreationTime();
        }

        @Override
        public void setLastAccessedTime(Instant lastAccessedTime) {
            delegate.setLastAccessedTime(lastAccessedTime);

            if (map != null) {
                map.fastPut("session:lastAccessedTime", lastAccessedTime.toEpochMilli());
                updateExpiration();
            }
        }

        @Override
        public Instant getLastAccessedTime() {
            return delegate.getLastAccessedTime();
        }

        @Override
        public void setMaxInactiveInterval(Duration interval) {
            delegate.setMaxInactiveInterval(interval);

            if (map != null) {
                map.fastPut("session:maxInactiveInterval", interval.getSeconds());
                updateExpiration();
            }
        }

        @Override
        public Duration getMaxInactiveInterval() {
            return delegate.getMaxInactiveInterval();
        }

        @Override
        public boolean isExpired() {
            return delegate.isExpired();
        }

        @Override
        public String changeSessionId() {
            String id = delegate.changeSessionId();
            if (redisson.getConfig().isClusterConfig()) {
                Map<String, Object> oldState = map.readAllMap();
                map.delete();
                map = redisson.getMap(keyPrefix + id, map.getCodec());
                map.putAll(oldState);
            } else {
                map.rename(keyPrefix + id);
            }
            return id;
        }

    }

    private static final Logger log = LoggerFactory.getLogger(RedissonSessionRepository.class);
    
    private static final String SPRING_SECURITY_CONTEXT = "SPRING_SECURITY_CONTEXT";
    
    private static final SpelExpressionParser SPEL_PARSER = new SpelExpressionParser();
    
    private RedissonClient redisson;
    private ApplicationEventPublisher eventPublisher;
    private RPatternTopic deletedTopic;
    private RPatternTopic expiredTopic;
    private RPatternTopic createdTopic;
    
    private String keyPrefix = "spring:session:";
    private Integer defaultMaxInactiveInterval;
    
    public RedissonSessionRepository(RedissonClient redissonClient, ApplicationEventPublisher eventPublisher) {
        this.redisson = redissonClient;
        this.eventPublisher = eventPublisher;
        
        deletedTopic = redisson.getPatternTopic("__keyevent@*:del", StringCodec.INSTANCE);
        deletedTopic.addListener(String.class, this);
        expiredTopic = redisson.getPatternTopic("__keyevent@*:expired", StringCodec.INSTANCE);
        expiredTopic.addListener(String.class, this);
        createdTopic = redisson.getPatternTopic(getEventsChannelPrefix() + "*", StringCodec.INSTANCE);
        createdTopic.addListener(String.class, this);
    }
    
    @Override
    public void onMessage(CharSequence pattern, CharSequence channel, String body) {
        if (createdTopic.getPatternNames().contains(pattern.toString())) {
            RedissonSession session = findById(body);
            if (session != null) {
                publishEvent(new SessionCreatedEvent(this, session));
            }
        } else if (deletedTopic.getPatternNames().contains(pattern.toString())) {
            if (!body.startsWith(keyPrefix)) {
                return;
            }
            
            String id = body.split(keyPrefix)[1];
            RedissonSession session = new RedissonSession(id);
            if (session.load()) {
                session.clearPrincipal();
            }
            publishEvent(new SessionDeletedEvent(this, session));
        } else if (expiredTopic.getPatternNames().contains(pattern.toString())) {
            if (!body.startsWith(keyPrefix)) {
                return;
            }

            String id = body.split(keyPrefix)[1];
            RedissonSession session = new RedissonSession(id);
            if (session.load()) {
                session.clearPrincipal();
            }
            publishEvent(new SessionExpiredEvent(this, session));
        }
    }
    
    private void publishEvent(ApplicationEvent event) {
        try {
            eventPublisher.publishEvent(event);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public void setDefaultMaxInactiveInterval(int defaultMaxInactiveInterval) {
        this.defaultMaxInactiveInterval = defaultMaxInactiveInterval;
    }

    @Override
    public RedissonSession createSession() {
        RedissonSession session = new RedissonSession();
        if (defaultMaxInactiveInterval != null) {
            session.setMaxInactiveInterval(Duration.ofSeconds(defaultMaxInactiveInterval));
        }
        return session;
    }

    @Override
    public void save(RedissonSession session) {
        // session changes are stored in real-time
    }

    @Override
    public RedissonSession findById(String id) {
        RedissonSession session = new RedissonSession(id);
        if (!session.load() || session.isExpired()) {
            return null;
        }
        return session;
    }

    @Override
    public void deleteById(String id) {
        RedissonSession session = findById(id);
        if (session == null) {
            return;
        }
        
        session.clearPrincipal();
        session.delete();
    }
    
    public void setKeyPrefix(String keyPrefix) {
        this.keyPrefix = keyPrefix;
    }
    
    String resolvePrincipal(Session session) {
        String principalName = session.getAttribute(PRINCIPAL_NAME_INDEX_NAME);
        if (principalName != null) {
            return principalName;
        }
        
        Object auth = session.getAttribute(SPRING_SECURITY_CONTEXT);
        if (auth == null) {
            return null;
        }
        
        Expression expression = SPEL_PARSER.parseExpression("authentication?.name");
        return expression.getValue(auth, String.class);
    }

    String getEventsChannelName(String sessionId) {
        return getEventsChannelPrefix() + sessionId;
    }
    
    String getEventsChannelPrefix() {
        return keyPrefix + "created:event:"; 
    }
    
    String getPrincipalKey(String principalName) {
        return keyPrefix + "index:" + FindByIndexNameSessionRepository.PRINCIPAL_NAME_INDEX_NAME + ":" + principalName;
    }
    
    String getSessionAttrNameKey(String name) {
        return "session-attr:" + name;
    }
    
    @Override
    public Map<String, RedissonSession> findByIndexNameAndIndexValue(String indexName, String indexValue) {
        if (!PRINCIPAL_NAME_INDEX_NAME.equals(indexName)) {
            return Collections.emptyMap();
        }
        
        RSet<String> set = getPrincipalSet(indexValue);
        
        Set<String> sessionIds = set.readAll();
        Map<String, RedissonSession> result = new HashMap<String, RedissonSession>();
        for (String id : sessionIds) {
            RedissonSession session = findById(id);
            if (session != null) {
                result.put(id, session);
            }
        }
        return result;
    }

    private RSet<String> getPrincipalSet(String indexValue) {
        String principalKey = getPrincipalKey(indexValue);
        return redisson.getSet(principalKey, StringCodec.INSTANCE);
    }

}
