package org.nodel;

/* 
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. 
 */

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

import org.nodel.diagnostics.Diagnostics;
import org.nodel.diagnostics.SharableMeasurementProvider;

/**
 * Holds thread-related general purpose or utility methods.
 */
public class Threads {
    
    public final static int THREAD_CAP = 64;
    
    /**
     * (signal / lock)
     */
    private final static Object s_lock = new Object();
    
    /**
     * For thread names
     */
    private final static AtomicLong s_threadNumber = new AtomicLong(1);
    
    /**
     * The number of threads in use.
     * (locked around 'staticLock')
     */
    private static int s_inUse = 0;
    
    /**
     * Exceptionless, 'synchronized' way to sleep 
     */
    public static void safeWait(Object signal, long millis) {
        synchronized (signal) {
            try {
                signal.wait(millis);
            } catch (InterruptedException e) {
                // (ignore)
            }
        }
    } // (method)

    /**
     * Exceptionless, 'synchronized' way to sleep. Must already be 'synchronized'.
     */
    public static void safeWaitOnSync(Object signal, long millis) {
        try {
            signal.wait(millis);
        } catch (InterruptedException e) {
            // (ignore)
        }
    } // (method)
    
    /**
     * Non-'synchronized' way to sleep.
     * @throws UnexpectedInterruptedException
     */
    public static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException exc) {
            throw new UnexpectedInterruptedException(exc);
        }
    } // (method)    

    /**
     * Sleeps 'synchronized' throwing UnexpectedInterruptionException if unexpectedly
     * interrupted.
     */
    public static void wait(Object signal, long millis) {
        try {
            synchronized (signal) {
                signal.wait(millis);
            }
        } catch (InterruptedException exc) {
            throw new UnexpectedInterruptedException(exc);
        }
    } // (method)
    
    /**
     * Sleeps 'synchronized' throwing UnexpectedInterruptionException if unexpectedly
     * interrupted.
     */
    public static void wait(Object signal) {
        try {
            synchronized (signal) {
                signal.wait();
            }
        } catch (InterruptedException exc) {
            throw new UnexpectedInterruptedException(exc);
        }
    } // (method)    

    /**
     * Sleeps 'synchronized' throwing UnexpectedInterruptionException if unexpectedly
     * interrupted. Must already be 'synchronized'.
     */
    public static void waitOnSync(Object signal, long millis) {
        try {
            signal.wait(millis);
        } catch (InterruptedException exc) {
            throw new UnexpectedInterruptedException(exc);
        }
    } // (method)
    

    /**
     * Sleeps 'synchronized' throwing UnexpectedInterruptionException if unexpectedly
     * interrupted. Must already be 'synchronized'.
     */
    public static void waitOnSync(Object signal) {
        try {
            signal.wait();
        } catch (InterruptedException exc) {
            throw new UnexpectedInterruptedException(exc);
        }
    } // (method)

    public static class AsyncResult<T> {

        /**
         * Used as lock / signal
         */
        private Object signal = new Object();

        /**
         * Required to support 'null' results
         */
        private boolean complete = false;

        /**
         * The result (if no exception)
         */
        private T result;

        /**
         * The exception.
         */
        public Exception exc;

        /**
         * Sets the value and flag. (assumes locked)
         */
        private void setResult(T result, Exception exc) {
            this.result = result;
            this.exc = exc;

            this.complete = true;

            this.signal.notifyAll();
        }

        /**
         * Returns the result.
         */
        public T getResult() {
            return this.result;
        }

        /**
         * Returns the the exception.
         */
        public Exception getException() {
            return this.exc;
        }

        /**
         * Returns whether this action is complete, i.e. a result or exception
         * occurred.
         */
        public boolean isComplete() {
            return this.complete;
        }

        /**
         * Waits for the operation to complete.
         */
        public void waitForComplete() {
            synchronized (this.signal) {
                while (!this.complete) {
                    Threads.waitOnSync(signal);
                }
            }
        } // (method)
        
        /**
         * Waits for the operation to complete (through result or exception) or returns after a timeout.
         */
        public void waitForCompleteOrTimeout(long timeout) {
            synchronized (this.signal) {
                if (!this.complete) {
                    Threads.waitOnSync(signal, timeout);
                }
            }
        }
        
        /**
         * Waits for a result or throws the exception that occurred.
         */
        public T waitForResultOrThrowException() throws Exception {
            waitForComplete();
            
            if (this.exc != null)
                throw this.exc;
            
            return this.result;
        } // (method)
        
        /**
         * Waits for a result or throws the exception that occurred.
         */
        public T waitForResultOrThrowRuntimeException() throws RuntimeException {
            waitForComplete();
            
            if (this.exc != null)
                throw new RuntimeException(this.exc);
            
            return this.result;
        } // (method)           

    } // (class)
    
    /**
     * An executor that uses fresh threads each time.
     */
    private static Handler.H1<Runnable> s_freshThreadExecutor = new Handler.H1<Runnable>() {

        @Override
        public void handle(Runnable runnable) {
            createShortThread(String.format("nodeloneoff_%d", s_threadNumber.getAndIncrement()), runnable).start();
        }
        
    };

    /**
     * Allows a call to be dealt with asynchronously with success for failure dealt with on current thread.
     * (uses a new thread each time)
     **/
    public static <U> AsyncResult<U> executeAsync(final Callable<U> callable) {
        return executeAsync(callable, null, null);
    }

    /**
     * Allows a call to be dealt with asynchronously with success for failure dealt with on current thread.
     */
    public static <U> AsyncResult<U> executeAsync(Handler.H1<Runnable> executor, final Callable<U> callable) {
        return executeAsync(executor, callable, null, null);
    }

    /**
     * Allows a call to be dealt with asynchronously, with separate threaded call-backs for success or failures (exception thrown).
     * (uses a new thread each time)
     */
    public static <T> AsyncResult<T> executeAsync(final Callable<T> callable, final Handler.H1<T> success, final Handler.H1<Exception> failure) {
        return executeAsync(s_freshThreadExecutor, callable, success, failure);
    }

    /**
     * Allows a call to be dealt with asynchronously, with separate threaded call-backs for success or failures (exception thrown).
     */
    public static <T> AsyncResult<T> executeAsync(final Handler.H1<Runnable> executor, final Callable<T> callable, final Handler.H1<T> success, final Handler.H1<Exception> failure) {
        final AsyncResult<T> op = new AsyncResult<T>();

        // enforce upper bound on fresh-thread usage
        if (executor == s_freshThreadExecutor) {
            synchronized (s_lock) {
                while (s_inUse > THREAD_CAP)
                    waitOnSync(s_lock);

                s_inUse++;
            }
        }

        // a note on synchronization:
        // 'success' and 'failure' handlers will be called before
        // 'waitForXXXX' methods return.
        synchronized (op.signal) {
            executor.handle(new Runnable() {

                @Override
                public void run() {
                    try {
                        T result = callable.call();

                        synchronized (op.signal) {
                            op.setResult(result, null);

                            if (success != null)
                                success.handle(result);
                        }
                    } catch (Exception exc) {
                        synchronized (op.signal) {
                            op.setResult(null, exc);

                            if (failure != null)
                                failure.handle(exc);
                        }
                    }

                    if (executor == s_freshThreadExecutor) {
                        synchronized (s_lock) {
                            s_inUse--;

                            s_lock.notifyAll();
                        }
                    }
                } // (method)
                
            });
        }

        return op;
    } // (method)


    // THREAD INSTRUMENTATION
    // ALL NEW THREADS ACROSS THE FRAMEWORK SHOULD BE CREATED THROUGH THE BELOW METHODS TO ENSURE PROPER INSTRUMENTATION.

    private static final SharableMeasurementProvider s_counter_LongCreated = Diagnostics.shared().registerSharableCounter("N Threading.Long thread creations", true);

    private static final SharableMeasurementProvider s_counter_LongAlive = Diagnostics.shared().registerSharableCounter("N Threading.Long threads alive", false);

    /**
     * Creates a long-living daemon thread with instrumentation, one expected to be alive for a long time, e.g. a thread handling a server socket. During stable operation, there should be a low number of these i.e. low churn.
     */
    public static Thread createLongThread(String name, Runnable runnable) {
        Thread thread = new Thread(new Runnable() {

            @Override
            public void run() {
                s_counter_LongAlive.incr();
                try {
                    runnable.run();
                } finally {
                    s_counter_LongAlive.decr();
                }
            }

        }, name);
        thread.setDaemon(true);
        s_counter_LongCreated.incr();
        return thread;
    }

    private static final SharableMeasurementProvider s_counter_ShortCreated = Diagnostics.shared().registerSharableCounter("N Threading.Short thread creations", true);

    private static final SharableMeasurementProvider s_counter_ShortAlive = Diagnostics.shared().registerSharableCounter("N Threading.Short threads alive", false);

    /**
     * Creates a short-living daemon thread with instrumentation, one expected to be alive for a short time, e.g. a thread handling a one-off task. During stable operation these could still be high in number, churn high.
     */
    public static Thread createShortThread(String name, Runnable runnable) {
        Thread thread = new Thread(new Runnable() {

            @Override
            public void run() {
                s_counter_ShortAlive.incr();
                try {
                    runnable.run();
                } finally {
                    s_counter_ShortAlive.decr();
                }
            }

        }, name);
        thread.setDaemon(true);
        s_counter_ShortCreated.incr();
        return thread;
    }


    private static final SharableMeasurementProvider s_counter_PoolCreated = Diagnostics.shared().registerSharableCounter("N Threading.Pool thread creations", true);

    private static final SharableMeasurementProvider s_counter_PoolAlive = Diagnostics.shared().registerSharableCounter("N Threading.Pool threads alive", false);

    /**
     * Creates a thread to be used in a thread-pool with instrumentation
     */
    public static Thread createPoolThread(String name, Runnable runnable) {
        Thread thread = new Thread(new Runnable() {

            @Override
            public void run() {
                s_counter_PoolAlive.incr();
                try {
                    runnable.run();
                } finally {
                    s_counter_PoolAlive.decr();
                }
            }

        }, name);
        thread.setDaemon(true);
        s_counter_PoolCreated.incr();
        return thread;
    }    


} // (class)
