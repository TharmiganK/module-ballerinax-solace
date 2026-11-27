/*
 * Copyright (c) 2026, WSO2 LLC. (http://www.wso2.org).
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

package io.ballerina.lib.solace.listener;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.XMLMessage;
import com.solacesystems.jcsmp.XMLMessageListener;
import io.ballerina.lib.solace.common.BallerinaSolaceDatabindingException;
import io.ballerina.lib.solace.common.CommonUtils;
import io.ballerina.lib.solace.consumer.MessageConverter;
import io.ballerina.lib.solace.observability.SolaceMetricsUtil;
import io.ballerina.lib.solace.observability.SolaceObserverContext;
import io.ballerina.lib.solace.observability.SolaceTracingUtil;
import io.ballerina.runtime.api.Runtime;
import io.ballerina.runtime.api.concurrent.StrandMetadata;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.observability.ObservabilityConstants;
import io.ballerina.runtime.observability.ObserveUtils;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static io.ballerina.lib.solace.observability.SolaceObservabilityConstants.CONTEXT_CONSUMER;
import static io.ballerina.lib.solace.observability.SolaceObservabilityConstants.ERROR_TYPE_ACKNOWLEDGE;
import static io.ballerina.lib.solace.observability.SolaceObservabilityConstants.ERROR_TYPE_DISPATCH;
import static io.ballerina.lib.solace.observability.SolaceObservabilityConstants.ERROR_TYPE_NACK;
import static io.ballerina.lib.solace.observability.SolaceObservabilityConstants.ERROR_TYPE_RECEIVE;

/**
 * JCSMP asynchronous message listener that bridges broker delivery to a Ballerina service.
 * <p>
 * The broker pushes each message to {@link #onReceive(BytesXMLMessage)} on a JCSMP consumer-dispatch thread. To avoid
 * blocking that thread - which JCSMP also uses to process control responses such as transacted commits and to drive
 * redelivery - the message is converted to a Ballerina record on the delivery thread (so its payload is copied before
 * the buffer can be reused) and the service invocation plus any settlement (ack / nack / commit / rollback) are handed
 * off to a dedicated single-threaded executor. The single thread preserves per-flow message ordering while keeping the
 * delivery thread free.
 */
final class SolaceMessageListener implements XMLMessageListener {

    private static final String ON_MESSAGE = "onMessage";
    private static final String ON_ERROR = "onError";
    private static final PrintStream ERR_OUT = System.err;

    private final Runtime runtime;
    private final Service nativeService;
    private final BObject caller;
    private final boolean autoAck;
    private final String url;
    private final String vpn;
    private final String destination;
    private final String destinationKind;
    private final ExecutorService dispatcher;
    private final boolean nonTransactedFlow;
    private volatile boolean stopping;
    private volatile boolean skipQueued;

    SolaceMessageListener(Runtime runtime, Service nativeService, BObject caller, boolean autoAck,
                          boolean nonTransactedFlow, String url, String vpn, String destination,
                          String destinationKind) {
        this.runtime = runtime;
        this.nativeService = nativeService;
        this.caller = caller;
        this.autoAck = autoAck;
        this.url = url;
        this.vpn = vpn;
        this.destination = destination;
        this.destinationKind = destinationKind;
        this.nonTransactedFlow = nonTransactedFlow;
        this.dispatcher = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "solace-listener-dispatch");
            thread.setDaemon(true);
            return thread;
        });
    }

    @Override
    public void onReceive(BytesXMLMessage message) {
        // Convert on the JCSMP delivery thread (copies the payload, safe for direct messages), then hand off so the
        // delivery thread is never blocked by the service call or a blocking settlement.
        BMap<BString, Object> ballerinaMessage;
        try {
            ballerinaMessage = MessageConverter.toBallerinaMessage(message,
                    ValueCreator.createTypedescValue(nativeService.getMessagePayloadType()));
        } catch (BallerinaSolaceDatabindingException e) {
            // The message could not be bound to the service's declared type - a receive-side failure.
            submitUnlessSkipped(() -> {
                dispatchError(CommonUtils.createError(e.getMessage()), null, ERROR_TYPE_RECEIVE);
                settleUndelivered(message, XMLMessage.Outcome.REJECTED);
            });
            return;
        } catch (Throwable t) {
            // The broker delivered a message the connector could not decode - a receive-side failure. Converting the
            // same message fails the same way, so only a JVM Error (such as running out of memory) is requeued.
            submitUnlessSkipped(() -> {
                dispatchError(CommonUtils.createError("Failed to convert message",
                        t instanceof Exception e ? e : new Exception(t)), null, ERROR_TYPE_RECEIVE);
                settleUndelivered(message, t instanceof Exception ? XMLMessage.Outcome.REJECTED
                        : XMLMessage.Outcome.FAILED);
            });
            return;
        }
        SolaceMetricsUtil.reportConsume(url, vpn, destination, destinationKind,
                CommonUtils.getPayloadSize(ballerinaMessage), CommonUtils.isRedelivered(ballerinaMessage));
        Map<String, String> traceContext = SolaceTracingUtil.extractTraceContextHeaders(ballerinaMessage);
        submitUnlessSkipped(() -> deliver(message, ballerinaMessage, traceContext));
    }

    private void deliver(BytesXMLMessage message, BMap<BString, Object> ballerinaMessage,
                         Map<String, String> traceContext) {
        long startNanos = System.nanoTime();
        boolean processed = false;
        try {
            Object result = invokeOnMessage(ballerinaMessage, traceContext);
            if (result instanceof BError bError) {
                dispatchError(bError, traceContext, ERROR_TYPE_DISPATCH);
            } else {
                processed = true;
            }
        } catch (BError bError) {
            dispatchError(bError, traceContext, ERROR_TYPE_DISPATCH);
        } catch (Throwable t) {
            dispatchError(CommonUtils.createError("Failed to dispatch message to service",
                    t instanceof Exception e ? e : new Exception(t)), traceContext, ERROR_TYPE_DISPATCH);
        } finally {
            if (autoAck) {
                settle(message, processed ? XMLMessage.Outcome.ACCEPTED : XMLMessage.Outcome.FAILED,
                        traceContext);
            }
            SolaceMetricsUtil.reportProcessDuration(url, vpn, destination, destinationKind,
                    System.nanoTime() - startNanos);
        }
    }

    /**
     * Settles a message that never reached {@code onMessage} on a non-transacted guaranteed flow, in either ack mode:
     * REJECTED when converting it fails the same way every time, FAILED when a possibly transient JVM error stopped it.
     */
    private void settleUndelivered(BytesXMLMessage message, XMLMessage.Outcome outcome) {
        if (nonTransactedFlow) {
            SolaceMetricsUtil.reportConsume(url, vpn, destination, destinationKind,
                    message.getContentLength() + message.getAttachmentContentLength(), message.getRedelivered());
            settle(message, outcome, null);
        }
    }

    /**
     * Settles a message on behalf of the service and counts it like {@code caller->ack()}/{@code nack()}. A failure
     * is reported as an ack or nack error, matching the outcome, since it is the broker's rather than the service's.
     */
    private void settle(BytesXMLMessage message, XMLMessage.Outcome outcome, Map<String, String> traceContext) {
        try {
            if (outcome == XMLMessage.Outcome.ACCEPTED) {
                message.ackMessage();
                SolaceMetricsUtil.reportAck(caller);
            } else {
                message.settle(outcome);
                SolaceMetricsUtil.reportNack(caller, outcome == XMLMessage.Outcome.FAILED);
            }
        } catch (Throwable t) {
            dispatchError(CommonUtils.createError("Failed to settle message",
                    t instanceof Exception e ? e : new Exception(t)), traceContext,
                    outcome == XMLMessage.Outcome.ACCEPTED ? ERROR_TYPE_ACKNOWLEDGE : ERROR_TYPE_NACK);
        }
    }

    @Override
    public void onException(JCSMPException exception) {
        // A flow-level failure raised by JCSMP itself - a genuine receive-side error.
        submit(() -> dispatchError(CommonUtils.createError("Solace consumer flow error", exception),
                null, ERROR_TYPE_RECEIVE));
    }

    private Object invokeOnMessage(BMap<BString, Object> ballerinaMessage, Map<String, String> traceContext) {
        StrandMetadata metadata =
                new StrandMetadata(nativeService.isOnMessageMethodIsolated(), tracingProperties(traceContext));
        if (nativeService.hasCaller()) {
            return runtime.callMethod(nativeService.getConsumerService(), ON_MESSAGE, metadata, ballerinaMessage,
                    caller);
        }
        return runtime.callMethod(nativeService.getConsumerService(), ON_MESSAGE, metadata, ballerinaMessage);
    }

    private void dispatchError(BError error, Map<String, String> traceContext, String errorType) {
        SolaceMetricsUtil.reportConsumerError(url, vpn, destination, destinationKind, errorType);
        if (nativeService.getOnError().isEmpty()) {
            ERR_OUT.println("Unexpected error occurred while message processing: " + error.getMessage());
            error.printStackTrace();
            return;
        }
        try {
            StrandMetadata metadata =
                    new StrandMetadata(nativeService.isOnErrorMethodIsolated(), tracingProperties(traceContext));
            runtime.callMethod(nativeService.getConsumerService(), ON_ERROR, metadata, error);
        } catch (Throwable t) {
            // Surface secondary failures from the error handler instead of dropping them silently.
            ERR_OUT.println("Unexpected error occurred while invoking the 'onError' method: "
                    + (t.getMessage() != null ? t.getMessage() : t));
        }
    }

    private Map<String, Object> tracingProperties(Map<String, String> traceContext) {
        if (!ObserveUtils.isTracingEnabled()) {
            return null;
        }
        SolaceObserverContext ctx = new SolaceObserverContext(CONTEXT_CONSUMER, url, destination)
                .withVpn(vpn)
                .withDestinationKind(destinationKind);
        if (traceContext != null && !traceContext.isEmpty()) {
            ctx.addProperty(ObservabilityConstants.PROPERTY_TRACE_PROPERTIES, traceContext);
        }
        Map<String, Object> properties = new HashMap<>();
        properties.put(ObservabilityConstants.KEY_OBSERVER_CONTEXT, ctx);
        return properties;
    }

    /**
     * Submits the handling of a received message, which is skipped if it is still queued when the service stops, so
     * the broker redelivers the message instead of the stopping listener handling it.
     */
    private void submitUnlessSkipped(Runnable task) {
        submit(() -> {
            if (!(stopping && skipQueued)) {
                task.run();
            }
        });
    }

    private void submit(Runnable task) {
        try {
            dispatcher.execute(task);
        } catch (RejectedExecutionException ignored) {
            // The listener is stopping; drop late deliveries (unsettled guaranteed messages are redelivered).
        }
    }

    /**
     * Stops dispatching. A graceful shutdown waits, without a time limit, for the in-flight {@code onMessage} and, when
     * the broker would not redeliver them (direct topic, transacted session), for the queued messages too.
     */
    void shutdown(boolean graceful) {
        skipQueued = !graceful || nonTransactedFlow;
        stopping = true;
        dispatcher.shutdown();
        if (!graceful) {
            return;
        }
        try {
            boolean terminated = false;
            while (!terminated) {
                terminated = dispatcher.awaitTermination(1, TimeUnit.MINUTES);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for onMessage to complete", e);
        }
    }
}
