/*
 * Copyright 2012-2024 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.crunchydata.core.threading;

/**
 * Utility class for thread synchronization.
 *
 * <p>This class provides synchronized methods for threads to wait and notify each other.</p>
 *
 * <p>It includes flags to indicate the status of source and target operations, as well as a counter for completed loader threads.</p>
 *
 * <p>Usage example:</p>
 * <pre>
 * {@code
 * ThreadSync sync = new ThreadSync();
 *
 * // In a thread
 * sync.ObserverWait();
 *
 * // In another thread
 * sync.ObserverNotify();
 * }
 * </pre>
 *
 * @see java.lang.Object#wait()
 * @see java.lang.Object#notifyAll()
 * @see java.lang.Exception#getMessage()
 *
 * @author Brian Pace
 */
public class ThreadSync {

    public volatile boolean sourceComplete = false;
    public volatile boolean targetComplete = false;

    public volatile boolean sourceWaiting = false;
    public volatile boolean targetWaiting = false;

    public volatile int loaderThreadComplete = 0;

    /**
     * Increase the number of threads complete.
     */
    public synchronized void incrementLoaderThreadComplete() {
        loaderThreadComplete++;
    }

    /**
     * Causes the current thread to wait until it is notified.
     * This method must be called from a synchronized context.
     */
    public synchronized void observerWait() {
            try {
                wait();
            } catch (Exception e) {
                System.out.println(e.getMessage());
                Thread.currentThread().interrupt(); // Restore interrupted status
            }
    }

    /**
     * Wakes up all threads that are waiting on this object's monitor.
     * This method must be called from a synchronized context.
     */
    public synchronized void observerNotify() {
        try {
            notifyAll();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

}
