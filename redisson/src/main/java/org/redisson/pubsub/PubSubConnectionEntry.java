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
package org.redisson.pubsub;

import java.util.Collection;
import java.util.Collections;
import java.util.EventListener;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.redisson.PubSubMessageListener;
import org.redisson.PubSubPatternMessageListener;
import org.redisson.client.BaseRedisPubSubListener;
import org.redisson.client.ChannelName;
import org.redisson.client.RedisPubSubConnection;
import org.redisson.client.RedisPubSubListener;
import org.redisson.client.SubscribeListener;
import org.redisson.client.codec.Codec;
import org.redisson.client.protocol.pubsub.PubSubType;

/**
 * 
 * @author Nikita Koksharov
 *
 */
public class PubSubConnectionEntry {

    private final AtomicInteger subscribedChannelsAmount;
    private final RedisPubSubConnection conn;

    private final ConcurrentMap<ChannelName, SubscribeListener> subscribeChannelListeners = new ConcurrentHashMap<ChannelName, SubscribeListener>();
    private final ConcurrentMap<ChannelName, Queue<RedisPubSubListener<?>>> channelListeners = new ConcurrentHashMap<ChannelName, Queue<RedisPubSubListener<?>>>();

    public PubSubConnectionEntry(RedisPubSubConnection conn, int subscriptionsPerConnection) {
        super();
        this.conn = conn;
        this.subscribedChannelsAmount = new AtomicInteger(subscriptionsPerConnection);
    }

    public int countListeners() {
        return channelListeners.size();
    }
    
    public boolean hasListeners(ChannelName channelName) {
        return channelListeners.containsKey(channelName);
    }

    public Collection<RedisPubSubListener<?>> getListeners(ChannelName channelName) {
        Collection<RedisPubSubListener<?>> result = channelListeners.get(channelName);
        if (result == null) {
            return Collections.emptyList();
        }
        return result;
    }

    public void addListener(ChannelName channelName, RedisPubSubListener<?> listener) {
        if (listener == null) {
            return;
        }
        
        Queue<RedisPubSubListener<?>> queue = channelListeners.get(channelName);
        if (queue == null) {
            queue = new ConcurrentLinkedQueue<RedisPubSubListener<?>>();
            Queue<RedisPubSubListener<?>> oldQueue = channelListeners.putIfAbsent(channelName, queue);
            if (oldQueue != null) {
                queue = oldQueue;
            }
        }

        boolean deleted = false;
        synchronized (queue) {
            if (channelListeners.get(channelName) != queue) {
                deleted = true;
            } else {
                queue.add(listener);
            }
        }
        if (deleted) {
            addListener(channelName, listener);
            return;
        }

        conn.addListener(listener);
    }

    public boolean removeAllListeners(ChannelName channelName) {
        Queue<RedisPubSubListener<?>> listeners = channelListeners.get(channelName);
        for (RedisPubSubListener<?> listener : listeners) {
            removeListener(channelName, listener);
        }
        return listeners.isEmpty();
    }
    
    // TODO optimize
    public boolean removeListener(ChannelName channelName, EventListener msgListener) {
        Queue<RedisPubSubListener<?>> listeners = channelListeners.get(channelName);
        for (RedisPubSubListener<?> listener : listeners) {
            if (listener instanceof PubSubMessageListener) {
                if (((PubSubMessageListener)listener).getListener() == msgListener) {
                    removeListener(channelName, listener);
                    return true;
                }
            }
            if (listener instanceof PubSubPatternMessageListener) {
                if (((PubSubPatternMessageListener)listener).getListener() == msgListener) {
                    removeListener(channelName, listener);
                    return true;
                }
            }
        }
        return false;
    }
    
    public boolean removeListener(ChannelName channelName, int listenerId) {
        Queue<RedisPubSubListener<?>> listeners = channelListeners.get(channelName);
        for (RedisPubSubListener<?> listener : listeners) {
            if (System.identityHashCode(listener) == listenerId) {
                removeListener(channelName, listener);
                return true;
            }
        }
        return false;
    }

    public void removeListener(ChannelName channelName, RedisPubSubListener<?> listener) {
        Queue<RedisPubSubListener<?>> queue = channelListeners.get(channelName);
        synchronized (queue) {
            if (queue.remove(listener) && queue.isEmpty()) {
                channelListeners.remove(channelName);
            }
        }
        conn.removeListener(listener);
    }

    public int tryAcquire() {
        while (true) {
            int value = subscribedChannelsAmount.get();
            if (value == 0) {
                return -1;
            }
            
            if (subscribedChannelsAmount.compareAndSet(value, value - 1)) {
                return value - 1;
            }
        }
    }

    public int release() {
        return subscribedChannelsAmount.incrementAndGet();
    }

    public void subscribe(Codec codec, ChannelName channelName) {
        conn.subscribe(codec, channelName);
    }

    public void psubscribe(Codec codec, ChannelName pattern) {
        conn.psubscribe(codec, pattern);
    }

    public SubscribeListener getSubscribeFuture(ChannelName channel, PubSubType type) {
        SubscribeListener listener = subscribeChannelListeners.get(channel);
        if (listener == null) {
            listener = new SubscribeListener(channel, type);
            SubscribeListener oldSubscribeListener = subscribeChannelListeners.putIfAbsent(channel, listener);
            if (oldSubscribeListener != null) {
                listener = oldSubscribeListener;
            } else {
                conn.addListener(listener);
            }
        }
        return listener;
    }
    
    public void unsubscribe(final ChannelName channel, final RedisPubSubListener<?> listener) {
        conn.addListener(new BaseRedisPubSubListener() {
            @Override
            public boolean onStatus(PubSubType type, CharSequence ch) {
                if (type == PubSubType.UNSUBSCRIBE && channel.equals(ch)) {
                    conn.removeListener(this);
                    removeListeners(channel);
                    if (listener != null) {
                        listener.onStatus(type, ch);
                    }
                    return true;
                }
                return false;
            }

        });
        conn.unsubscribe(channel);
    }

    private void removeListeners(ChannelName channel) {
        conn.removeDisconnectListener(channel);
        SubscribeListener s = subscribeChannelListeners.remove(channel);
        conn.removeListener(s);
        Queue<RedisPubSubListener<?>> queue = channelListeners.get(channel);
        if (queue != null) {
            synchronized (queue) {
                channelListeners.remove(channel);
            }
            for (RedisPubSubListener<?> listener : queue) {
                conn.removeListener(listener);
            }
        }
    }

    public void punsubscribe(final ChannelName channel, final RedisPubSubListener<?> listener) {
        conn.addListener(new BaseRedisPubSubListener() {
            @Override
            public boolean onStatus(PubSubType type, CharSequence ch) {
                if (type == PubSubType.PUNSUBSCRIBE && channel.equals(ch)) {
                    conn.removeListener(this);
                    removeListeners(channel);
                    if (listener != null) {
                        listener.onStatus(type, ch);
                    }
                    return true;
                }
                return false;
            }
        });
        conn.punsubscribe(channel);
    }

    public RedisPubSubConnection getConnection() {
        return conn;
    }

}
