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
package org.redisson;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.redisson.api.RFuture;
import org.redisson.api.RRateLimiter;
import org.redisson.api.RateIntervalUnit;
import org.redisson.api.RateType;
import org.redisson.client.codec.LongCodec;
import org.redisson.client.protocol.RedisCommand;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.misc.RPromise;
import org.redisson.misc.RedissonPromise;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;

/**
 * 
 * @author Nikita Koksharov
 *
 */
public class RedissonRateLimiter extends RedissonObject implements RRateLimiter {

    public RedissonRateLimiter(CommandAsyncExecutor commandExecutor, String name) {
        super(commandExecutor, name);
    }
    
    String getValueName() {
        return suffixName(getName(), "value");
    }
    
    String getClientValueName() {
        return suffixName(getValueName(), commandExecutor.getConnectionManager().getId().toString());
    }
    
    @Override
    public boolean tryAcquire() {
        return tryAcquire(1);
    }
    
    @Override
    public RFuture<Boolean> tryAcquireAsync() {
        return tryAcquireAsync(1L);
    }
    
    @Override
    public boolean tryAcquire(long permits) {
        return get(tryAcquireAsync(RedisCommands.EVAL_NULL_BOOLEAN, permits));
    }
    
    @Override
    public RFuture<Boolean> tryAcquireAsync(long permits) {
        return tryAcquireAsync(RedisCommands.EVAL_NULL_BOOLEAN, permits);
    }

    @Override
    public void acquire() {
        get(acquireAsync());
    }
    
    @Override
    public RFuture<Void> acquireAsync() {
        return acquireAsync(1);
    }

    @Override
    public void acquire(long permits) {
        get(acquireAsync(permits));
    }

    @Override
    public RFuture<Void> acquireAsync(long permits) {
        final RPromise<Void> promise = new RedissonPromise<Void>();
        tryAcquireAsync(permits, -1, null).addListener(new FutureListener<Boolean>() {
            @Override
            public void operationComplete(Future<Boolean> future) throws Exception {
                if (!future.isSuccess()) {
                    promise.tryFailure(future.cause());
                    return;
                }
                
                promise.trySuccess(null);
            }
        });
        return promise;
    }

    @Override
    public boolean tryAcquire(long timeout, TimeUnit unit) {
        return get(tryAcquireAsync(timeout, unit));
    }

    @Override
    public RFuture<Boolean> tryAcquireAsync(long timeout, TimeUnit unit) {
        return tryAcquireAsync(1, timeout, unit);
    }
    
    @Override
    public boolean tryAcquire(long permits, long timeout, TimeUnit unit) {
        return get(tryAcquireAsync(permits, timeout, unit));
    }
    
    @Override
    public RFuture<Boolean> tryAcquireAsync(long permits, long timeout, TimeUnit unit) {
        RPromise<Boolean> promise = new RedissonPromise<Boolean>();
        long timeoutInMillis = -1;
        if (timeout > 0) {
            timeoutInMillis = unit.toMillis(timeout);
        }
        tryAcquireAsync(permits, promise, timeoutInMillis);
        return promise;
    }
    
    private void tryAcquireAsync(final long permits, final RPromise<Boolean> promise, final long timeoutInMillis) {
        final long start = System.currentTimeMillis();
        RFuture<Long> future = tryAcquireAsync(RedisCommands.EVAL_LONG, permits);
        future.addListener(new FutureListener<Long>() {
            @Override
            public void operationComplete(Future<Long> future) throws Exception {
                if (!future.isSuccess()) {
                    promise.tryFailure(future.cause());
                    return;
                }
                
                Long delay = future.getNow();
                if (delay == null) {
                    promise.trySuccess(true);
                    return;
                }
                
                if (timeoutInMillis == -1) {
                    commandExecutor.getConnectionManager().getGroup().schedule(new Runnable() {
                        @Override
                        public void run() {
                            tryAcquireAsync(permits, promise, timeoutInMillis);
                        }
                    }, delay, TimeUnit.MILLISECONDS);
                    return;
                }
                
                long elapsed = System.currentTimeMillis() - start;
                final long remains = timeoutInMillis - elapsed;
                if (remains <= 0) {
                    promise.trySuccess(false);
                    return;
                }
                if (remains < delay) {
                    commandExecutor.getConnectionManager().getGroup().schedule(new Runnable() {
                        @Override
                        public void run() {
                            promise.trySuccess(false);
                        }
                    }, remains, TimeUnit.MILLISECONDS);
                } else {
                    final long start = System.currentTimeMillis();
                    commandExecutor.getConnectionManager().getGroup().schedule(new Runnable() {
                        @Override
                        public void run() {
                            long elapsed = System.currentTimeMillis() - start;
                            if (remains <= elapsed) {
                                promise.trySuccess(false);
                                return;
                            }
                            
                            tryAcquireAsync(permits, promise, remains - elapsed);
                        }
                    }, delay, TimeUnit.MILLISECONDS);
                }
            }
        });
    }
    
    private <T> RFuture<T> tryAcquireAsync(RedisCommand<T> command, Long value) {
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, command,
                "local rate = redis.call('hget', KEYS[1], 'rate');"
              + "local interval = redis.call('hget', KEYS[1], 'interval');"
              + "local type = redis.call('hget', KEYS[1], 'type');"
              + "assert(rate ~= false and interval ~= false and type ~= false, 'RateLimiter is not initialized')"
              
              + "local valueName = KEYS[2];"
              + "if type == '1' then "
                  + "valueName = KEYS[3];"
              + "end;"
              
              + "local currentValue = redis.call('get', valueName); "
              + "if currentValue ~= false then "
                     + "if tonumber(currentValue) < tonumber(ARGV[1]) then "
                         + "return redis.call('pttl', valueName); "
                     + "else "
                         + "redis.call('decrby', valueName, ARGV[1]); "
                         + "return nil; "
                     + "end; "
              + "else "
                     + "redis.call('set', valueName, rate, 'px', interval); "
                     + "redis.call('decrby', valueName, ARGV[1]); "
                     + "return nil; "
              + "end;",
                Arrays.<Object>asList(getName(), getValueName(), getClientValueName()), 
                value, commandExecutor.getConnectionManager().getId().toString());
    }

    @Override
    public boolean trySetRate(RateType type, long rate, long rateInterval, RateIntervalUnit unit) {
        return get(trySetRateAsync(type, rate, rateInterval, unit));
    }

    @Override
    public RFuture<Boolean> trySetRateAsync(RateType type, long rate, long rateInterval, RateIntervalUnit unit) {
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "redis.call('hsetnx', KEYS[1], 'rate', ARGV[1]);"
              + "redis.call('hsetnx', KEYS[1], 'interval', ARGV[2]);"
              + "return redis.call('hsetnx', KEYS[1], 'type', ARGV[3]);",
                Collections.<Object>singletonList(getName()), rate, unit.toMillis(rateInterval), type.ordinal());
    }
    
}
