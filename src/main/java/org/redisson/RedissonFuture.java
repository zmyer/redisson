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

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.redisson.core.RFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

public class RedissonFuture<T> extends CompletableFuture<T> implements RFuture<T>, Future<T> {

    private Set<GenericFutureListener> allListeners;
    
    private volatile boolean uncancellable;
    
    private static final Logger log = LoggerFactory.getLogger(RedissonFuture.class); 
    
    @Override
    public boolean isSuccess() {
        return isDone() && !isCompletedExceptionally();
    }

    @Override
    public boolean isCancellable() {
        return !isDone();
    }

    @Override
    public Throwable cause() {
        try {
            getNow(null);
        } catch (CompletionException e) {
            return e.getCause();
        }
        return null;
    }

    @Override
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        try {
            get(timeout, unit);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof CancellationException) {
                throw (CancellationException)e.getCause();
            }
            throw new CompletionException(e.getCause());
        } catch (TimeoutException e) {
            return false;
        }
        return isDone();
    }

    @Override
    public boolean await(long timeoutMillis) throws InterruptedException {
        return await(timeoutMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean awaitUninterruptibly(long timeout, TimeUnit unit) {
        try {
            return await(timeout, unit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    @Override
    public boolean awaitUninterruptibly(long timeoutMillis) {
        try {
            return await(timeoutMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    @Override
    public T getNow() {
        return getNow(null);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        synchronized (this) {
            if (uncancellable) {
                return false;
            }
            return super.cancel(mayInterruptIfRunning);
        }
    }
    
    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Future<T> addListener(GenericFutureListener listener) {
        createListeners();
        allListeners.add(listener);
        
        handle((r, ex) -> {
            if (!allListeners.contains(listener)) {
                return null;
            }
            try {
                listener.operationComplete(RedissonFuture.this);
            } catch (Exception e) {
                log.error("An exception was thrown by " + listener.getClass().getName() + ".operationComplete()", e);
            }
            return null;
        });
        
        return this;
    }

    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Future<T> addListeners(GenericFutureListener<? extends Future<? super T>>... listeners) {
        createListeners();
        allListeners.addAll(Arrays.asList(listeners));

        for (GenericFutureListener listener : listeners) {
            handle((r, ex) -> {
                if (!allListeners.contains(listener)) {
                    return null;
                }
                try {
                    listener.operationComplete(RedissonFuture.this);
                } catch (Exception e) {
                    log.error("An exception was thrown by " + listener.getClass().getName() + ".operationComplete()", e);
                }
                return null;
            });
        }
        
        return this;
    }

    private void createListeners() {
        if (allListeners == null) {
            synchronized (this) {
                if (allListeners == null) {
                    allListeners = Collections.newSetFromMap(new ConcurrentHashMap<GenericFutureListener, Boolean>());
                }
            }
        }
    }

    
    public Future<T> removeListener(GenericFutureListener<? extends Future<? super T>> listener) {
        if (allListeners == null) {
            return this;
        }
        
        allListeners.remove(listener);
        return this;
    }

    public Future<T> removeListeners(GenericFutureListener<? extends Future<? super T>>... listeners) {
        if (allListeners == null) {
            return this;
        }
        
        allListeners.removeAll(Arrays.asList(listeners));
        return this;
    }

    @Override
    public Future<T> await() throws InterruptedException {
        try {
            get();
        } catch (ExecutionException e) {
            // skip
        }
        return this;
    }

    @Override
    public Future<T> awaitUninterruptibly() {
        try {
            join();
        } catch (CancellationException | CompletionException e) {
            // skip
        }
        return this;
    }

    @Override
    public Future<T> sync() throws InterruptedException {
        try {
            get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof CancellationException) {
                throw (CancellationException)e.getCause();
            }
            throw new CompletionException(e.getCause());
        }
        return this;
    }

    @Override
    public Future<T> syncUninterruptibly() {
        join();
        return this;
    }

}
