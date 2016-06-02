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
package org.redisson.connection;

import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.redisson.MasterSlaveServersConfig;
import org.redisson.RedissonFuture;
import org.redisson.client.RedisClient;
import org.redisson.client.RedisConnection;
import org.redisson.client.RedisPubSubConnection;
import org.redisson.cluster.ClusterSlotRange;
import org.redisson.connection.pool.PubSubConnectionPool;
import org.redisson.connection.pool.SinglePubSubConnectionPool;
import org.redisson.core.NodeType;
import org.redisson.core.RFuture;

public class SingleEntry extends MasterSlaveEntry {

    final PubSubConnectionPool pubSubConnectionHolder;

    public SingleEntry(Set<ClusterSlotRange> slotRanges, ConnectionManager connectionManager, MasterSlaveServersConfig config) {
        super(slotRanges, connectionManager, config);
        pubSubConnectionHolder = new SinglePubSubConnectionPool(config, connectionManager, this);
    }

    @Override
    public RFuture<Void> setupMasterEntry(String host, int port) {
        RedisClient masterClient = connectionManager.createClient(NodeType.MASTER, host, port);
        masterEntry = new ClientConnectionsEntry(masterClient,
                config.getMasterConnectionMinimumIdleSize(),
                config.getMasterConnectionPoolSize(),
                config.getSlaveConnectionMinimumIdleSize(),
                config.getSlaveSubscriptionConnectionPoolSize(), connectionManager, NodeType.MASTER);
        final RedissonFuture<Void> res = connectionManager.newPromise();
        RFuture<Void> f = writeConnectionHolder.add(masterEntry);
        RFuture<Void> s = pubSubConnectionHolder.add(masterEntry);
        
        CompletableFuture.allOf((CompletableFuture)f, (CompletableFuture)s)
        .thenAccept(res::complete)
        .exceptionally(cause -> {
            res.completeExceptionally(cause);
            return null;
        });
        return res;
    }

    @Override
    RFuture<RedisPubSubConnection> nextPubSubConnection() {
        return pubSubConnectionHolder.get();
    }

    @Override
    public void returnPubSubConnection(PubSubConnectionEntry entry) {
        pubSubConnectionHolder.returnConnection(masterEntry, entry.getConnection());
    }

    @Override
    public RFuture<RedisConnection> connectionReadOp(InetSocketAddress addr) {
        return super.connectionWriteOp();
    }

    @Override
    public RFuture<RedisConnection> connectionReadOp() {
        return super.connectionWriteOp();
    }

    @Override
    public void releaseRead(RedisConnection сonnection) {
        super.releaseWrite(сonnection);
    }

}
