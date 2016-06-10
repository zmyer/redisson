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
package org.redisson.core;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import io.netty.util.concurrent.GenericFutureListener;

/**
 * Result of asynchronous operation
 * 
 * @author Nikita Koksharov
 *
 * @param <T>
 */
public interface RFuture<T> extends Future<T>, CompletionStage<T> {

    /**
     * Returns {@code true} if and only if the I/O operation was completed
     * successfully.
     */
    boolean isSuccess();

    /**
     * Returns the cause of the failed I/O operation if the I/O operation has
     * failed.
     *
     * @return the cause of the failure.
     *         {@code null} if succeeded or this future is not
     *         completed yet.
     */
    Throwable cause();
    
    /**
     * Returns the result value (or throws any encountered exception)
     * if completed, else returns the given valueIfAbsent.
     *
     * @param valueIfAbsent the value to return if not completed
     * @return the result value, if completed, else the given valueIfAbsent
     * @throws CancellationException if the computation was cancelled
     * @throws CompletionException if this future completed
     * exceptionally or a completion computation threw an exception
     */
    T getNow(T valueIfAbsent);
 
    /**
     * Return the result without blocking. If the future is not done yet this will return {@code null}.
     *
     * As it is possible that a {@code null} value is used to mark the future as successful you also need to check
     * if the future is really done with {@link #isDone()} and not relay on the returned {@code null} value.
     */
    T getNow();
 
    /**
     * Waits for this future to be completed within the
     * specified time limit.
     *
     * @return {@code true} if and only if the future was completed within
     *         the specified time limit
     *
     * @throws InterruptedException
     *         if the current thread was interrupted
     */
    boolean await(long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * Waits for this future to be completed within the
     * specified time limit.
     *
     * @return {@code true} if and only if the future was completed within
     *         the specified time limit
     *
     * @throws InterruptedException
     *         if the current thread was interrupted
     */
    boolean await(long timeoutMillis) throws InterruptedException;
    
    /**
     * Returns the result value when complete, or throws an
     * (unchecked) exception if completed exceptionally. To better
     * conform with the use of common functional forms, if a
     * computation involved in the completion of this
     * CompletableFuture threw an exception, this method throws an
     * (unchecked) {@link CompletionException} with the underlying
     * exception as its cause.
     *
     * @return the result value
     * @throws CancellationException if the computation was cancelled
     * @throws CompletionException if this future completed
     * exceptionally or a completion computation threw an exception
     */
    T join();

    /**
     * Use {@link #thenApply} or {@link #exceptionally} or {@link #handle} methods instead
     */
    @Deprecated
    io.netty.util.concurrent.Future<T> addListener(GenericFutureListener<? extends io.netty.util.concurrent.Future<? super T>> listener);

    /**
     * Use {@link #thenApply} or {@link #exceptionally} or {@link #handle} methods instead
     */
    @Deprecated
    io.netty.util.concurrent.Future<T> addListeners(GenericFutureListener<? extends io.netty.util.concurrent.Future<? super T>>... listeners);

    @Deprecated
    io.netty.util.concurrent.Future<T> removeListener(GenericFutureListener<? extends io.netty.util.concurrent.Future<? super T>> listener);

    @Deprecated
    io.netty.util.concurrent.Future<T> removeListeners(GenericFutureListener<? extends io.netty.util.concurrent.Future<? super T>>... listeners);

    @Deprecated
    boolean isCancellable();
    
    @Deprecated
    io.netty.util.concurrent.Future<T> sync() throws InterruptedException;

    /**
     * Use {@link #join()} method instead
     */
    @Deprecated
    io.netty.util.concurrent.Future<T> syncUninterruptibly();

    @Deprecated
    io.netty.util.concurrent.Future<T> await() throws InterruptedException;

    @Deprecated
    io.netty.util.concurrent.Future<T> awaitUninterruptibly();
    
    @Deprecated
    boolean awaitUninterruptibly(long timeout, TimeUnit unit);

    @Deprecated
    boolean awaitUninterruptibly(long timeoutMillis);

}
