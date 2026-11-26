/*
 * Copyright (c) 2025, WSO2 LLC. (http://www.wso2.org).
 *
 *  WSO2 LLC. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package io.ballerina.lib.solace.common;

import io.ballerina.lib.solace.ModuleUtils;
import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.creators.ErrorCreator;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import static io.ballerina.lib.solace.common.MessageFieldConstants.PAYLOAD_KEY;
import static io.ballerina.lib.solace.common.MessageFieldConstants.REDELIVERED_KEY;

/**
 * Utility class for common operations like error creation and virtual thread execution.
 */
public class CommonUtils {

    private static final String SOLACE_ERROR = "Error";

    /**
     * Creates a Ballerina error from a message and Java exception.
     */
    public static BError createError(String message, Throwable cause) {
        String errorMsg = message;
        if (cause != null) {
            errorMsg = message + ": " + cause.getMessage();
        }
        return ErrorCreator.createError(ModuleUtils.getModule(), SOLACE_ERROR, StringUtils.fromString(errorMsg), null,
                null);
    }

    /**
     * Creates a Ballerina Solace error that keeps the given Ballerina error as its cause.
     */
    public static BError createErrorWithCause(String message, BError cause) {
        return ErrorCreator.createError(ModuleUtils.getModule(), SOLACE_ERROR, StringUtils.fromString(message), cause,
                null);
    }

    /**
     * Creates a Ballerina error from just a message.
     */
    public static BError createError(String message) {
        return ErrorCreator.createError(ModuleUtils.getModule(), SOLACE_ERROR,
                StringUtils.fromString(message), null, null);
    }

    /**
     * Creates a named Ballerina Solace error subtype.
     */
    public static BError createError(String errorType, String message) {
        return ErrorCreator.createError(ModuleUtils.getModule(), errorType,
                StringUtils.fromString(message), null, null);
    }

    /**
     * Runs a blocking operation after yielding the calling strand, so other strands, including non-isolated ones that
     * share its lock, keep running while it waits.
     */
    public static Object executeBlocking(Environment env, RunnableWithException task) {
        return executeBlocking(env, () -> {
            task.run();
            return null;
        });
    }

    public static Object executeBlocking(Environment env, CompletableWithException task) {
        AtomicBoolean completed = new AtomicBoolean();
        AtomicReference<Object> taskResult = new AtomicReference<>();
        try {
            return env.yieldAndRun(() -> {
                Object result;
                try {
                    result = task.run();
                } catch (Exception e) {
                    result = createError("Error during blocking operation", e);
                }
                taskResult.set(result);
                completed.set(true);
                return result;
            });
        } catch (BError panic) {
            // The task's own failures are returned above, so this is the runtime cancelling the strand. The runtime
            // raises it once the task has finished, if the task started at all.
            throw new StrandCancelledException(panic, completed.get(), taskResult.get());
        }
    }

    /**
     * Wraps a task so it holds the transacted session's lock while it runs, since JCSMP requires serial access to a
     * {@code TransactedSession} and its producers and flows. A null lock, for a non-transacted session, adds nothing.
     */
    public static CompletableWithException serialized(ReentrantLock lock, CompletableWithException task) {
        if (lock == null) {
            return task;
        }
        return () -> {
            lock.lock();
            try {
                return task.run();
            } finally {
                lock.unlock();
            }
        };
    }

    public static RunnableWithException serialized(ReentrantLock lock, RunnableWithException task) {
        if (lock == null) {
            return task;
        }
        return () -> {
            lock.lock();
            try {
                task.run();
            } finally {
                lock.unlock();
            }
        };
    }

    /**
     * Runs a close after yielding the strand. If the strand is cancelled before the close runs, it still runs, so the
     * session is not leaked, and the cancellation is then rethrown.
     */
    public static BError yieldToClose(Environment env, Supplier<BError> close) {
        AtomicBoolean ran = new AtomicBoolean();
        try {
            return env.yieldAndRun(() -> {
                ran.set(true);
                return close.get();
            });
        } catch (BError panic) {
            if (!ran.get()) {
                // The strand still holds the lock shared by non-isolated strands here, so close off the strand.
                Thread.ofVirtual().start(close::get);
            }
            throw panic;
        }
    }

    /**
     * Carries the runtime's panic out of a caller's generic exception handler, which must rethrow it.
     */
    public static final class StrandCancelledException extends RuntimeException {
        private final BError panic;
        private final boolean taskCompleted;
        private final transient Object taskResult;

        StrandCancelledException(BError panic, boolean taskCompleted, Object taskResult) {
            super(panic);
            this.panic = panic;
            this.taskCompleted = taskCompleted;
            this.taskResult = taskResult;
        }

        public BError panic() {
            return panic;
        }

        /**
         * Whether the blocking task ran to completion before the cancellation was raised.
         */
        public boolean taskCompleted() {
            return taskCompleted;
        }

        /**
         * The blocking task's result, such as a message it received, or null if it did not complete.
         */
        public Object taskResult() {
            return taskResult;
        }
    }

    /**
     * Runs a best-effort cleanup action, silently discarding any exception. Use when a secondary failure during
     * cleanup should not surface to the caller (e.g. releasing resources already created before an init() failure).
     */
    public static void closeQuietly(RunnableWithException action) {
        try {
            action.run();
        } catch (Exception e) {
            // Best-effort cleanup - ignore.
        }
    }

    /**
     * Runs a cleanup action and returns any exception instead of throwing it, so callers can attempt several
     * independent cleanup steps without one failure blocking the rest.
     *
     * @return the caught exception, or null if the action succeeded
     */
    public static Exception attemptClose(RunnableWithException action) {
        try {
            action.run();
            return null;
        } catch (Exception e) {
            return e;
        }
    }

    /**
     * Computes the best-effort byte size of a Ballerina Solace message's payload, for observability metrics.
     *
     * @param message the Ballerina message record
     * @return the payload size in bytes, or 0 if it cannot be determined
     */
    @SuppressWarnings("unchecked")
    public static int getPayloadSize(BMap<BString, Object> message) {
        if (message == null) {
            return 0;
        }
        Object payload = message.get(PAYLOAD_KEY);
        if (payload instanceof BArray arr) {
            return arr.size();
        }
        if (payload instanceof BString str) {
            return str.getValue().getBytes(StandardCharsets.UTF_8).length;
        }
        return 0;
    }

    /**
     * Reports whether a received message is flagged by the broker as a redelivery.
     *
     * @param message the Ballerina message record
     * @return true if the broker marked this message as redelivered
     */
    public static boolean isRedelivered(BMap<BString, Object> message) {
        if (message == null) {
            return false;
        }
        return Boolean.TRUE.equals(message.get(REDELIVERED_KEY));
    }

    /**
     * Functional interface for operations that can throw checked exceptions.
     */
    @FunctionalInterface
    public interface RunnableWithException {

        void run() throws Exception;
    }

    @FunctionalInterface
    public interface CompletableWithException {

        Object run() throws Exception;
    }
}
