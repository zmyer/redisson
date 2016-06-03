/**
 * Copyright 2014 Nikita Koksharov, Nickolay Borbit
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
package org.redisson;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.redisson.client.RedisConnection;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.connection.ConnectionListener;
import org.redisson.connection.ConnectionManager;
import org.redisson.connection.RedisClientEntry;
import org.redisson.core.Node;
import org.redisson.core.NodeType;
import org.redisson.core.NodesGroup;
import org.redisson.core.RFuture;

public class RedisNodes<N extends Node> implements NodesGroup<N> {

    private final ConnectionManager connectionManager;

    public RedisNodes(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    @Override
    public Collection<N> getNodes(NodeType type) {
        Collection<N> clients = (Collection<N>) connectionManager.getClients();
        List<N> result = new ArrayList<N>();
        for (N node : clients) {
            if (node.getType().equals(type)) {
                result.add(node);
            }
        }
        return result;
    }


    @Override
    public Collection<N> getNodes() {
        return (Collection<N>) connectionManager.getClients();
    }

    @Override
    public boolean pingAll() {
        List<RedisClientEntry> clients = new ArrayList<RedisClientEntry>(connectionManager.getClients());
        final Map<RedisConnection, RFuture<String>> result = new HashMap<>(clients.size());
        final CountDownLatch latch = new CountDownLatch(clients.size());
        for (RedisClientEntry entry : clients) {
            RFuture<RedisConnection> f = entry.getClient().connectAsync();
            f.thenAccept(c -> {
                RedissonFuture<RedisConnection> connectionFuture = connectionManager.newPromise();
                connectionManager.getConnectListener().onConnect(connectionFuture, c, null, connectionManager.getConfig());
                connectionFuture.handle((r, cause) -> {
                    RFuture<String> future = c.async(RedisCommands.PING);
                    result.put(c, future);
                    latch.countDown();
                    return null;
                });
            }).exceptionally(cause -> {
                latch.countDown();
                return null;
            });
        }

        boolean res = true;
        try {
            long time = System.currentTimeMillis();
            latch.await(connectionManager.getConfig().getConnectTimeout(), TimeUnit.MILLISECONDS);

            if (System.currentTimeMillis() - time >= connectionManager.getConfig().getConnectTimeout()) {
                for (RedisConnection conn : result.keySet()) {
                    conn.closeAsync();
                }
                return false;
            }
    
            time = System.currentTimeMillis();
            for (Entry<RedisConnection, RFuture<String>> entry : result.entrySet()) {
                RFuture<String> f = entry.getValue();
                f.await(connectionManager.getConfig().getPingTimeout(), TimeUnit.MILLISECONDS);
                if (!"PONG".equals(f.getNow())) {
                    res = false;
                }
                entry.getKey().closeAsync();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }


        // true and no futures missed during client connection
        return res && result.size() == clients.size();
    }

    @Override
    public int addConnectionListener(ConnectionListener connectionListener) {
        return connectionManager.getConnectionEventsHub().addListener(connectionListener);
    }

    @Override
    public void removeConnectionListener(int listenerId) {
        connectionManager.getConnectionEventsHub().removeListener(listenerId);
    }

}
