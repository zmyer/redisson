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
package org.redisson.command;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.redisson.RedissonShutdownException;
import org.redisson.api.BatchOptions;
import org.redisson.api.BatchOptions.ExecutionMode;
import org.redisson.api.BatchResult;
import org.redisson.api.RFuture;
import org.redisson.client.RedisAskException;
import org.redisson.client.RedisConnection;
import org.redisson.client.RedisLoadingException;
import org.redisson.client.RedisMovedException;
import org.redisson.client.RedisResponseTimeoutException;
import org.redisson.client.RedisTimeoutException;
import org.redisson.client.RedisTryAgainException;
import org.redisson.client.WriteRedisConnectionException;
import org.redisson.client.codec.Codec;
import org.redisson.client.codec.StringCodec;
import org.redisson.client.protocol.BatchCommandData;
import org.redisson.client.protocol.CommandData;
import org.redisson.client.protocol.CommandsData;
import org.redisson.client.protocol.RedisCommand;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.connection.ConnectionManager;
import org.redisson.connection.MasterSlaveEntry;
import org.redisson.connection.NodeSource;
import org.redisson.connection.NodeSource.Redirect;
import org.redisson.misc.CountableListener;
import org.redisson.misc.LogHelper;
import org.redisson.misc.RPromise;
import org.redisson.misc.RedissonPromise;
import org.redisson.pubsub.AsyncSemaphore;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.internal.PlatformDependent;

/**
 * 
 * @author Nikita Koksharov
 *
 */
public class CommandBatchService extends CommandAsyncService {

    public static class ConnectionEntry {

        boolean firstCommand = true;
        RFuture<RedisConnection> connectionFuture;
        
        public RFuture<RedisConnection> getConnectionFuture() {
            return connectionFuture;
        }
        
        public void setConnectionFuture(RFuture<RedisConnection> connectionFuture) {
            this.connectionFuture = connectionFuture;
        }

        public boolean isFirstCommand() {
            return firstCommand;
        }

        public void setFirstCommand(boolean firstCommand) {
            this.firstCommand = firstCommand;
        }
        
    }
    
    public static class Entry {

        Deque<BatchCommandData<?, ?>> commands = new LinkedBlockingDeque<BatchCommandData<?,?>>();
        volatile boolean readOnlyMode = true;

        public Deque<BatchCommandData<?, ?>> getCommands() {
            return commands;
        }

        public void setReadOnlyMode(boolean readOnlyMode) {
            this.readOnlyMode = readOnlyMode;
        }

        public boolean isReadOnlyMode() {
            return readOnlyMode;
        }
        

        public void clearErrors() {
            for (BatchCommandData<?, ?> commandEntry : commands) {
                commandEntry.clearError();
            }
        }

    }

    private final AtomicInteger index = new AtomicInteger();

    private ConcurrentMap<MasterSlaveEntry, Entry> commands = PlatformDependent.newConcurrentHashMap();
    private ConcurrentMap<MasterSlaveEntry, ConnectionEntry> connections = PlatformDependent.newConcurrentHashMap();
    
    private BatchOptions options;
    
    private Map<RFuture<?>, List<CommandBatchService>> nestedServices = PlatformDependent.newConcurrentHashMap();

    private AtomicBoolean executed = new AtomicBoolean();

    public CommandBatchService(ConnectionManager connectionManager) {
        super(connectionManager);
    }
    
    public CommandBatchService(ConnectionManager connectionManager, BatchOptions options) {
        super(connectionManager);
        this.options = options;
    }
    
    public BatchOptions getOptions() {
        return options;
    }

    public void add(RFuture<?> future, List<CommandBatchService> services) {
        nestedServices.put(future, services);
    }
    
    @Override
    public <V, R> void async(boolean readOnlyMode, NodeSource nodeSource,
            Codec codec, RedisCommand<V> command, Object[] params, RPromise<R> mainPromise, int attempt, boolean ignoreRedirect) {
        if (nodeSource.getEntry() != null) {
            Entry entry = commands.get(nodeSource.getEntry());
            if (entry == null) {
                entry = new Entry();
                Entry oldEntry = commands.putIfAbsent(nodeSource.getEntry(), entry);
                if (oldEntry != null) {
                    entry = oldEntry;
                }
            }
            
            if (!readOnlyMode) {
                entry.setReadOnlyMode(false);
            }
            
            Object[] batchParams = null;
            if (!isRedisBasedQueue()) {
                batchParams = params;
            }
            Codec codecToUse = getCodec(codec);
            BatchCommandData<V, R> commandData = new BatchCommandData<V, R>(mainPromise, codecToUse, command, batchParams, index.incrementAndGet());
            entry.getCommands().add(commandData);
        }
        
        if (!isRedisBasedQueue()) {
            return;
        }
        
        if (!readOnlyMode && this.options.getExecutionMode() == ExecutionMode.REDIS_READ_ATOMIC) {
            throw new IllegalStateException("Data modification commands can't be used with queueStore=REDIS_READ_ATOMIC");
        }
        
        super.async(readOnlyMode, nodeSource, codec, command, params, mainPromise, attempt, true);
    }
    
    AsyncSemaphore semaphore = new AsyncSemaphore(0);
    
    @Override
    protected <R> RPromise<R> createPromise() {
        if (isRedisBasedQueue()) {
            return new BatchPromise<R>(executed);
        }
        return super.createPromise();
    }
    
    @Override
    protected <V, R> void releaseConnection(NodeSource source, RFuture<RedisConnection> connectionFuture,
            boolean isReadOnly, RPromise<R> attemptPromise, AsyncDetails<V, R> details) {
        if (!isRedisBasedQueue() || RedisCommands.EXEC.getName().equals(details.getCommand().getName())) {
            super.releaseConnection(source, connectionFuture, isReadOnly, attemptPromise, details);
        }
    }
    
    @Override
    protected <V, R> void handleSuccess(final AsyncDetails<V, R> details, RPromise<R> promise, RedisCommand<?> command, R res) {
        if (RedisCommands.EXEC.getName().equals(command.getName())) {
            super.handleSuccess(details, promise, command, res);
            return;
        }
        if (RedisCommands.DISCARD.getName().equals(command.getName())) {
            super.handleSuccess(details, promise, command, null);
            if (executed.compareAndSet(false, true)) {
                details.getConnectionFuture().getNow().forceFastReconnectAsync().addListener(new FutureListener<Void>() {
                    @Override
                    public void operationComplete(Future<Void> future) throws Exception {
                        CommandBatchService.super.releaseConnection(details.getSource(), details.getConnectionFuture(), details.isReadOnlyMode(), details.getAttemptPromise(), details);
                    }
                });
            }
            return;
        }

        if (isRedisBasedQueue()) {
            BatchPromise<R> batchPromise = (BatchPromise<R>) promise;
            RPromise<R> sentPromise = (RPromise<R>) batchPromise.getSentPromise();
            super.handleSuccess(details, sentPromise, command, null);
            semaphore.release();
        }
    }
    
    @Override
    protected <V, R> void handleError(final AsyncDetails<V, R> details, RPromise<R> promise, Throwable cause) {
        if (isRedisBasedQueue() && promise instanceof BatchPromise) {
            BatchPromise<R> batchPromise = (BatchPromise<R>) promise;
            RPromise<R> sentPromise = (RPromise<R>) batchPromise.getSentPromise();
            sentPromise.tryFailure(cause);
            promise.tryFailure(cause);
            if (executed.compareAndSet(false, true)) {
                details.getConnectionFuture().getNow().forceFastReconnectAsync().addListener(new FutureListener<Void>() {
                    @Override
                    public void operationComplete(Future<Void> future) throws Exception {
                        CommandBatchService.super.releaseConnection(details.getSource(), details.getConnectionFuture(), details.isReadOnlyMode(), details.getAttemptPromise(), details);
                    }
                });
            }
            semaphore.release();
            return;
        }

        super.handleError(details, promise, cause);
    }
    
    @Override
    protected <R, V> void sendCommand(AsyncDetails<V, R> details, RedisConnection connection) {
        if (!isRedisBasedQueue()) {
            super.sendCommand(details, connection);
            return;
        }
        
        ConnectionEntry connectionEntry = connections.get(details.getSource().getEntry());
        
        if (details.getSource().getRedirect() == Redirect.ASK) {
            List<CommandData<?, ?>> list = new ArrayList<CommandData<?, ?>>(2);
            RPromise<Void> promise = new RedissonPromise<Void>();
            list.add(new CommandData<Void, Void>(promise, details.getCodec(), RedisCommands.ASKING, new Object[]{}));
            if (connectionEntry.isFirstCommand()) {
                list.add(new CommandData<Void, Void>(promise, details.getCodec(), RedisCommands.MULTI, new Object[]{}));
                connectionEntry.setFirstCommand(false);
            }
            list.add(new CommandData<V, R>(details.getAttemptPromise(), details.getCodec(), details.getCommand(), details.getParams()));
            RPromise<Void> main = new RedissonPromise<Void>();
            ChannelFuture future = connection.send(new CommandsData(main, list, true));
            details.setWriteFuture(future);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("acquired connection for command {} and params {} from slot {} using node {}... {}",
                        details.getCommand(), LogHelper.toString(details.getParams()), details.getSource(), connection.getRedisClient().getAddr(), connection);
            }
            
            if (connectionEntry.isFirstCommand()) {
                List<CommandData<?, ?>> list = new ArrayList<CommandData<?, ?>>(2);
                list.add(new CommandData<Void, Void>(new RedissonPromise<Void>(), details.getCodec(), RedisCommands.MULTI, new Object[]{}));
                list.add(new CommandData<V, R>(details.getAttemptPromise(), details.getCodec(), details.getCommand(), details.getParams()));
                RPromise<Void> main = new RedissonPromise<Void>();
                ChannelFuture future = connection.send(new CommandsData(main, list, true));
                connectionEntry.setFirstCommand(false);
                details.setWriteFuture(future);
            } else {
                if (RedisCommands.EXEC.getName().equals(details.getCommand().getName())) {
                    Entry entry = commands.get(details.getSource().getEntry());

                    List<CommandData<?, ?>> list = new LinkedList<CommandData<?, ?>>();

                    if (options.isSkipResult()) {
                        list.add(new CommandData<Void, Void>(new RedissonPromise<Void>(), details.getCodec(), RedisCommands.CLIENT_REPLY, new Object[]{ "OFF" }));
                    }
                    
                    list.add(new CommandData<V, R>(details.getAttemptPromise(), details.getCodec(), details.getCommand(), details.getParams()));
                    
                    if (options.isSkipResult()) {
                        list.add(new CommandData<Void, Void>(new RedissonPromise<Void>(), details.getCodec(), RedisCommands.CLIENT_REPLY, new Object[]{ "ON" }));
                    }
                    if (options.getSyncSlaves() > 0) {
                        BatchCommandData<?, ?> waitCommand = new BatchCommandData(RedisCommands.WAIT, 
                                new Object[] { this.options.getSyncSlaves(), this.options.getSyncTimeout() }, index.incrementAndGet());
                        list.add(waitCommand);
                        entry.getCommands().add(waitCommand);
                    }

                    RPromise<Void> main = new RedissonPromise<Void>();
                    ChannelFuture future = connection.send(new CommandsData(main, list, new ArrayList(entry.getCommands())));
                    details.setWriteFuture(future);
                } else {
                    RPromise<Void> main = new RedissonPromise<Void>();
                    List<CommandData<?, ?>> list = new LinkedList<CommandData<?, ?>>();
                    list.add(new CommandData<V, R>(details.getAttemptPromise(), details.getCodec(), details.getCommand(), details.getParams()));
                    ChannelFuture future = connection.send(new CommandsData(main, list, true));
                    details.setWriteFuture(future);
                }
            }
        }
    }
    
    @Override
    protected <V> RFuture<RedisConnection> getConnection(boolean readOnlyMode, NodeSource source,
            RedisCommand<V> command) {
        if (!isRedisBasedQueue()) {
            return super.getConnection(readOnlyMode, source, command);
        }
        
        ConnectionEntry entry = connections.get(source.getEntry());
        if (entry == null) {
            entry = new ConnectionEntry();
            ConnectionEntry oldEntry = connections.putIfAbsent(source.getEntry(), entry);
            if (oldEntry != null) {
                entry = oldEntry;
            }
        }

        
        if (entry.getConnectionFuture() != null) {
            return entry.getConnectionFuture();
        }
        
        synchronized (this) {
            if (entry.getConnectionFuture() != null) {
                return entry.getConnectionFuture();
            }
        
            RFuture<RedisConnection> connectionFuture;
            if (this.options.getExecutionMode() == ExecutionMode.REDIS_WRITE_ATOMIC) {
                connectionFuture = connectionManager.connectionWriteOp(source, null);
            } else {
                connectionFuture = connectionManager.connectionReadOp(source, null);
            }
            connectionFuture.syncUninterruptibly();
            entry.setConnectionFuture(connectionFuture);
            return connectionFuture;
        }
    }

    public BatchResult<?> execute() {
        RFuture<BatchResult<?>> f = executeAsync(BatchOptions.defaults());
        return get(f);
    }
    
    public BatchResult<?> execute(BatchOptions options) {
        RFuture<BatchResult<?>> f = executeAsync(options);
        return get(f);
    }

    public RFuture<Void> executeAsyncVoid() {
        final RedissonPromise<Void> promise = new RedissonPromise<Void>();
        RFuture<BatchResult<?>> res = executeAsync(BatchOptions.defaults());
        res.addListener(new FutureListener<BatchResult<?>>() {
            @Override
            public void operationComplete(Future<BatchResult<?>> future) throws Exception {
                if (future.isSuccess()) {
                    promise.trySuccess(null);
                } else {
                    promise.tryFailure(future.cause());
                }
            }
        });
        return promise;
    }
    
    public RFuture<List<?>> executeAsync() {
        return executeAsync(BatchOptions.defaults());
    }
    
    public <R> RFuture<R> executeAsync(BatchOptions options) {
        if (executed.get()) {
            throw new IllegalStateException("Batch already executed!");
        }
        
        if (commands.isEmpty()) {
            executed.set(true);
            BatchResult<Object> result = new BatchResult<Object>(Collections.emptyList(), 0);
            return (RFuture<R>) RedissonPromise.newSucceededFuture(result);
        }
        
        if (this.options == null) {
            this.options = options;
        }
        
        if (isRedisBasedQueue()) {
            int permits = 0;
            for (Entry entry : commands.values()) {
                permits += entry.getCommands().size();
            };
            
            final RPromise<R> resultPromise = new RedissonPromise<R>();
            semaphore.acquire(new Runnable() {
                @Override
                public void run() {
                    for (Entry entry : commands.values()) {
                        for (BatchCommandData<?, ?> command : entry.getCommands()) {
                            if (command.getPromise().isDone() && !command.getPromise().isSuccess()) {
                                resultPromise.tryFailure(command.getPromise().cause());
                                break;
                            }
                        }
                    }
                    
                    if (resultPromise.isDone()) {
                        return;
                    }
                    
                    final RPromise<Map<MasterSlaveEntry, List<Object>>> mainPromise = new RedissonPromise<Map<MasterSlaveEntry, List<Object>>>();
                    final Map<MasterSlaveEntry, List<Object>> result = new ConcurrentHashMap<MasterSlaveEntry, List<Object>>();
                    final CountableListener<Map<MasterSlaveEntry, List<Object>>> listener = new CountableListener<Map<MasterSlaveEntry, List<Object>>>(mainPromise, result);
                    listener.setCounter(connections.size());
                    for (final Map.Entry<MasterSlaveEntry, Entry> entry : commands.entrySet()) {
                        final RPromise<List<Object>> execPromise = new RedissonPromise<List<Object>>();
                        async(entry.getValue().isReadOnlyMode(), new NodeSource(entry.getKey()), connectionManager.getCodec(), RedisCommands.EXEC, 
                                new Object[] {}, execPromise, 0, false);
                        execPromise.addListener(new FutureListener<List<Object>>() {
                            @Override
                            public void operationComplete(Future<List<Object>> future) throws Exception {
                                if (!future.isSuccess()) {
                                    mainPromise.tryFailure(future.cause());
                                    return;
                                }

                                BatchCommandData<?, Integer> lastCommand = (BatchCommandData<?, Integer>) entry.getValue().getCommands().peekLast();
                                result.put(entry.getKey(), future.getNow());
                                if (RedisCommands.WAIT.getName().equals(lastCommand.getCommand().getName())) {
                                    lastCommand.getPromise().addListener(new FutureListener<Integer>() {
                                        @Override
                                        public void operationComplete(Future<Integer> ft) throws Exception {
                                            if (!ft.isSuccess()) {
                                                mainPromise.tryFailure(ft.cause());
                                                return;
                                            }
                                            
                                            execPromise.addListener(listener);
                                        }
                                    });
                                } else {
                                    execPromise.addListener(listener);
                                }
                            }
                        });
                    }
                    
                    mainPromise.addListener(new FutureListener<Map<MasterSlaveEntry, List<Object>>>() {
                        @Override
                        public void operationComplete(Future<Map<MasterSlaveEntry, List<Object>>> future) throws Exception {
                            executed.set(true);
                            if (!future.isSuccess()) {
                                resultPromise.tryFailure(future.cause());
                                return;
                            }
                            
                            try {
                                for (java.util.Map.Entry<MasterSlaveEntry, List<Object>> entry : future.getNow().entrySet()) {
                                    Entry commandEntry = commands.get(entry.getKey());
                                    Iterator<Object> resultIter = entry.getValue().iterator();
                                    for (BatchCommandData<?, ?> data : commandEntry.getCommands()) {
                                        if (data.getCommand().getName().equals(RedisCommands.EXEC.getName())) {
                                            break;
                                        }
                                        RPromise<Object> promise = (RPromise<Object>) data.getPromise();
                                        promise.trySuccess(resultIter.next());
                                    }
                                }
                                
                                List<BatchCommandData> entries = new ArrayList<BatchCommandData>();
                                for (Entry e : commands.values()) {
                                    entries.addAll(e.getCommands());
                                }
                                Collections.sort(entries);
                                List<Object> responses = new ArrayList<Object>(entries.size());
                                int syncedSlaves = 0;
                                for (BatchCommandData<?, ?> commandEntry : entries) {
                                    if (isWaitCommand(commandEntry)) {
                                        syncedSlaves += (Integer) commandEntry.getPromise().getNow();
                                    } else if (!commandEntry.getCommand().getName().equals(RedisCommands.MULTI.getName())
                                            && !commandEntry.getCommand().getName().equals(RedisCommands.EXEC.getName())) {
                                        Object entryResult = commandEntry.getPromise().getNow();
                                        entryResult = tryHandleReference(entryResult);
                                        responses.add(entryResult);
                                    }
                                }
                                BatchResult<Object> result = new BatchResult<Object>(responses, syncedSlaves);
                                resultPromise.trySuccess((R)result);
                            } catch (Exception e) {
                                resultPromise.tryFailure(e);
                            }
                            
                            commands = null;
                        }
                    });
                }
            }, permits);
            return resultPromise;
        }

        if (this.options.getExecutionMode() != ExecutionMode.IN_MEMORY) {
            for (Entry entry : commands.values()) {
                BatchCommandData<?, ?> multiCommand = new BatchCommandData(RedisCommands.MULTI, new Object[] {}, index.incrementAndGet());
                entry.getCommands().addFirst(multiCommand);
                BatchCommandData<?, ?> execCommand = new BatchCommandData(RedisCommands.EXEC, new Object[] {}, index.incrementAndGet());
                entry.getCommands().add(execCommand);
            }
        }
        
        if (this.options.isSkipResult()) {
            for (Entry entry : commands.values()) {
                BatchCommandData<?, ?> offCommand = new BatchCommandData(RedisCommands.CLIENT_REPLY, new Object[] { "OFF" }, index.incrementAndGet());
                entry.getCommands().addFirst(offCommand);
                BatchCommandData<?, ?> onCommand = new BatchCommandData(RedisCommands.CLIENT_REPLY, new Object[] { "ON" }, index.incrementAndGet());
                entry.getCommands().add(onCommand);
            }
        }
        
        if (this.options.getSyncSlaves() > 0) {
            for (Entry entry : commands.values()) {
                BatchCommandData<?, ?> waitCommand = new BatchCommandData(RedisCommands.WAIT, 
                                    new Object[] { this.options.getSyncSlaves(), this.options.getSyncTimeout() }, index.incrementAndGet());
                entry.getCommands().add(waitCommand);
            }
        }
        
        RPromise<R> resultPromise;
        final RPromise<Void> voidPromise = new RedissonPromise<Void>();
        if (this.options.isSkipResult()) {
            voidPromise.addListener(new FutureListener<Void>() {
                @Override
                public void operationComplete(Future<Void> future) throws Exception {
//                    commands = null;
                    executed.set(true);
                    nestedServices.clear();
                }
            });
            resultPromise = (RPromise<R>) voidPromise;
        } else {
            final RPromise<Object> promise = new RedissonPromise<Object>();
            voidPromise.addListener(new FutureListener<Void>() {
                @Override
                public void operationComplete(Future<Void> future) throws Exception {
                    executed.set(true);
                    if (!future.isSuccess()) {
                        promise.tryFailure(future.cause());
                        commands = null;
                        nestedServices.clear();
                        return;
                    }
                    
                    List<BatchCommandData> entries = new ArrayList<BatchCommandData>();
                    for (Entry e : commands.values()) {
                        entries.addAll(e.getCommands());
                    }
                    Collections.sort(entries);
                    List<Object> responses = new ArrayList<Object>(entries.size());
                    int syncedSlaves = 0;
                    for (BatchCommandData<?, ?> commandEntry : entries) {
                        if (isWaitCommand(commandEntry)) {
                            syncedSlaves = (Integer) commandEntry.getPromise().getNow();
                        } else if (!commandEntry.getCommand().getName().equals(RedisCommands.MULTI.getName())
                                && !commandEntry.getCommand().getName().equals(RedisCommands.EXEC.getName())) {
                            Object entryResult = commandEntry.getPromise().getNow();
                            entryResult = tryHandleReference(entryResult);
                            responses.add(entryResult);
                        }
                    }
                    
                    BatchResult<Object> result = new BatchResult<Object>(responses, syncedSlaves);
                    promise.trySuccess(result);
                    
                    commands = null;
                    nestedServices.clear();
                }
            });
            resultPromise = (RPromise<R>) promise;
        }

        final AtomicInteger slots = new AtomicInteger(commands.size());
        
        for (java.util.Map.Entry<RFuture<?>, List<CommandBatchService>> entry : nestedServices.entrySet()) {
            slots.incrementAndGet();
            for (CommandBatchService service : entry.getValue()) {
                service.executeAsync();
            }
            
            entry.getKey().addListener(new FutureListener<Object>() {
                @Override
                public void operationComplete(Future<Object> future) throws Exception {
                    handle(voidPromise, slots, future);
                }
            });
        }
        
        for (java.util.Map.Entry<MasterSlaveEntry, Entry> e : commands.entrySet()) {
            execute(e.getValue(), new NodeSource(e.getKey()), voidPromise, slots, 0, this.options);
        }
        return resultPromise;
    }

    protected boolean isRedisBasedQueue() {
        return options != null && (this.options.getExecutionMode() == ExecutionMode.REDIS_READ_ATOMIC || this.options.getExecutionMode() == ExecutionMode.REDIS_WRITE_ATOMIC);
    }

    private void execute(final Entry entry, final NodeSource source, final RPromise<Void> mainPromise, final AtomicInteger slots, 
            final int attempt, final BatchOptions options) {
        if (mainPromise.isCancelled()) {
            free(entry);
            return;
        }

        if (!connectionManager.getShutdownLatch().acquire()) {
            free(entry);
            mainPromise.tryFailure(new RedissonShutdownException("Redisson is shutdown"));
            return;
        }

        final RPromise<Void> attemptPromise = new RedissonPromise<Void>();

        final AsyncDetails details = new AsyncDetails();
        details.init(null, attemptPromise,
                entry.isReadOnlyMode(), source, null, null, null, mainPromise, attempt);

        final RFuture<RedisConnection> connectionFuture;
        if (entry.isReadOnlyMode()) {
            connectionFuture = connectionManager.connectionReadOp(source, null);
        } else {
            connectionFuture = connectionManager.connectionWriteOp(source, null);
        }
        
        final int attempts;
        if (options.getRetryAttempts() > 0) {
            attempts = options.getRetryAttempts();
        } else {
            attempts = connectionManager.getConfig().getRetryAttempts();
        }

        final long interval;
        if (options.getRetryInterval() > 0) {
            interval = options.getRetryInterval();
        } else {
            interval = connectionManager.getConfig().getRetryInterval();
        }
        
        final FutureListener<Void> mainPromiseListener = new FutureListener<Void>() {
            @Override
            public void operationComplete(Future<Void> future) throws Exception {
                if (future.isCancelled() && connectionFuture.cancel(false)) {
                    log.debug("Connection obtaining canceled for batch");
                    details.getTimeout().cancel();
                    if (attemptPromise.cancel(false)) {
                        free(entry);
                    }
                }
            }
        };

        final TimerTask retryTimerTask = new TimerTask() {
            @Override
            public void run(Timeout t) throws Exception {
                if (attemptPromise.isDone()) {
                    return;
                }

                if (connectionFuture.cancel(false)) {
                    if (details.getException() == null) {
                        details.setException(new RedisTimeoutException("Unable to get connection! Try to increase 'nettyThreads' and 'connection pool' settings or set decodeInExecutor = true and increase 'threads' setting"
                                    + "Node source: " + source + " after " + attempts + " retry attempts"));
                    }
                } else {
                    if (connectionFuture.isSuccess()) {
                        if (details.getWriteFuture() == null || !details.getWriteFuture().isDone()) {
                            if (details.getAttempt() == attempts) {
                                if (details.getWriteFuture() != null && details.getWriteFuture().cancel(false)) {
                                    if (details.getException() == null) {
                                        details.setException(new RedisTimeoutException("Unable to send batch after " + attempts + " retry attempts"));
                                    }
                                    attemptPromise.tryFailure(details.getException());
                                }
                                return;
                            }
                            details.incAttempt();
                            Timeout timeout = connectionManager.newTimeout(this, interval, TimeUnit.MILLISECONDS);
                            details.setTimeout(timeout);
                            return;
                        }
                        
                        if (details.getWriteFuture().isDone() && details.getWriteFuture().isSuccess()) {
                            return;
                        }
                    }
                }

                if (mainPromise.isCancelled()) {
                    if (attemptPromise.cancel(false)) {
                        free(entry);
                    }
                    return;
                }

                if (attempt == attempts) {
                    if (details.getException() == null) {
                        details.setException(new RedisTimeoutException("Batch command execution timeout"));
                    }
                    attemptPromise.tryFailure(details.getException());
                    return;
                }
                if (!attemptPromise.cancel(false)) {
                    return;
                }

                int count = attempt + 1;
                mainPromise.removeListener(mainPromiseListener);
                execute(entry, source, mainPromise, slots, count, options);
            }
        };

        Timeout timeout = connectionManager.newTimeout(retryTimerTask, interval, TimeUnit.MILLISECONDS);
        details.setTimeout(timeout);
        mainPromise.addListener(mainPromiseListener);

        connectionFuture.addListener(new FutureListener<RedisConnection>() {
            @Override
            public void operationComplete(Future<RedisConnection> connFuture) throws Exception {
                checkConnectionFuture(entry, source, mainPromise, attemptPromise, details, connectionFuture, options.isSkipResult(), 
                        options.getResponseTimeout(), attempts, options.getExecutionMode(), slots);
            }
        });

        attemptPromise.addListener(new FutureListener<Void>() {
            @Override
            public void operationComplete(Future<Void> future) throws Exception {
                details.getTimeout().cancel();
                if (future.isCancelled()) {
                    return;
                }

                mainPromise.removeListener(mainPromiseListener);
                
                if (future.cause() instanceof RedisMovedException) {
                    RedisMovedException ex = (RedisMovedException)future.cause();
                    entry.clearErrors();
                    NodeSource nodeSource = new NodeSource(ex.getSlot(), ex.getUrl(), Redirect.MOVED);
                    execute(entry, nodeSource, mainPromise, slots, attempt, options);
                    return;
                }
                if (future.cause() instanceof RedisAskException) {
                    RedisAskException ex = (RedisAskException)future.cause();
                    entry.clearErrors();
                    NodeSource nodeSource = new NodeSource(ex.getSlot(), ex.getUrl(), Redirect.ASK);
                    execute(entry, nodeSource, mainPromise, slots, attempt, options);
                    return;
                }
                if (future.cause() instanceof RedisLoadingException
                        || future.cause() instanceof RedisTryAgainException) {
                    if (details.getAttempt() < connectionManager.getConfig().getRetryAttempts()) {
                        entry.clearErrors();
                        connectionManager.newTimeout(new TimerTask() {
                            @Override
                            public void run(Timeout timeout) throws Exception {
                                execute(entry, source, mainPromise, slots, attempt + 1, options);
                            }
                        }, Math.min(connectionManager.getConfig().getTimeout(), 1000), TimeUnit.MILLISECONDS);
                        return;
                    }
                }

                free(entry);
                
                handle(mainPromise, slots, future);
            }
        });
    }

    protected void free(final Entry entry) {
        for (BatchCommandData<?, ?> command : entry.getCommands()) {
            free(command.getParams());
        }
    }

    private void checkWriteFuture(final Entry entry, final RPromise<Void> attemptPromise, final AsyncDetails details,
            final RedisConnection connection, ChannelFuture future, long responseTimeout, int attempts, final AtomicInteger slots, final RPromise<Void> mainPromise) {
        if (future.isCancelled() || attemptPromise.isDone()) {
            return;
        }
        
        if (!future.isSuccess()) {
            details.setException(new WriteRedisConnectionException("Can't write command batch to channel: " + future.channel(), future.cause()));
            if (details.getAttempt() == attempts) {
                if (!attemptPromise.tryFailure(details.getException())) {
                    log.error(details.getException().getMessage());
                }
            }
            return;
        }
        
        details.getTimeout().cancel();
        
        TimerTask timerTask = new TimerTask() {
            @Override
            public void run(Timeout timeout) throws Exception {
                if (details.getAttempt() < connectionManager.getConfig().getRetryAttempts()) {
                    if (!details.getAttemptPromise().cancel(false)) {
                        return;
                    }

                    int count = details.getAttempt() + 1;
                    if (log.isDebugEnabled()) {
                        log.debug("attempt {} for command {} and params {}",
                                count, details.getCommand(), LogHelper.toString(details.getParams()));
                    }
                    details.removeMainPromiseListener();
                    execute(entry, details.getSource(), mainPromise, slots, count, options);
                    return;
                }

                attemptPromise.tryFailure(
                        new RedisResponseTimeoutException("Redis server response timeout during command batch execution. Channel: " + connection.getChannel()));
            }
        };
        
        long timeout = connectionManager.getConfig().getTimeout();
        if (responseTimeout > 0) {
            timeout = responseTimeout;
        }
        Timeout timeoutTask = connectionManager.newTimeout(timerTask, timeout, TimeUnit.MILLISECONDS);
        details.setTimeout(timeoutTask);
    }

    private void checkConnectionFuture(final Entry entry, final NodeSource source,
            final RPromise<Void> mainPromise, final RPromise<Void> attemptPromise, final AsyncDetails details,
            RFuture<RedisConnection> connFuture, final boolean noResult, final long responseTimeout, final int attempts, 
            ExecutionMode executionMode, final AtomicInteger slots) {
        if (connFuture.isCancelled()) {
            connectionManager.getShutdownLatch().release();
            return;
        }

        if (!connFuture.isSuccess()) {
            connectionManager.getShutdownLatch().release();
            details.setException(convertException(connFuture));
            return;
        }

        if (attemptPromise.isDone() || mainPromise.isDone()) {
            releaseConnection(source, connFuture, details.isReadOnlyMode(), attemptPromise, details);
            return;
        }
        
        final RedisConnection connection = connFuture.getNow();
        boolean isAtomic = executionMode != ExecutionMode.IN_MEMORY;
        boolean isQueued = executionMode == ExecutionMode.REDIS_READ_ATOMIC || executionMode == ExecutionMode.REDIS_WRITE_ATOMIC;

        List<CommandData<?, ?>> list = new LinkedList<CommandData<?, ?>>();
        if (source.getRedirect() == Redirect.ASK) {
            RPromise<Void> promise = new RedissonPromise<Void>();
            list.add(new CommandData<Void, Void>(promise, StringCodec.INSTANCE, RedisCommands.ASKING, new Object[] {}));
        } 
        for (BatchCommandData<?, ?> c : entry.getCommands()) {
            if (c.getPromise().isSuccess() && !isWaitCommand(c) && !isAtomic) {
                // skip successful commands
                continue;
            }
            list.add(c);
        }
        
        ChannelFuture future = connection.send(new CommandsData(attemptPromise, list, noResult, isAtomic, isQueued));
        details.setWriteFuture(future);

        details.getWriteFuture().addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                checkWriteFuture(entry, attemptPromise, details, connection, future, responseTimeout, attempts, slots, mainPromise);
            }
        });

        releaseConnection(source, connFuture, entry.isReadOnlyMode(), attemptPromise, details);
    }

    protected boolean isWaitCommand(BatchCommandData<?, ?> c) {
        return c.getCommand().getName().equals(RedisCommands.WAIT.getName());
    }

    protected void handle(final RPromise<Void> mainPromise, final AtomicInteger slots, Future<?> future) {
        if (future.isSuccess()) {
            if (slots.decrementAndGet() == 0) {
                mainPromise.trySuccess(null);
            }
        } else {
            mainPromise.tryFailure(future.cause());
        }
    }
    
    @Override
    protected boolean isEvalCacheActive() {
        return false;
    }
    

}
