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

package io.ballerina.lib.solace.consumer;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.XMLMessage;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.transaction.TransactedSession;
import io.ballerina.lib.solace.common.BallerinaSolaceDatabindingException;
import io.ballerina.lib.solace.common.CommonUtils;
import io.ballerina.lib.solace.config.ConfigurationUtils;
import io.ballerina.lib.solace.config.ConsumerConfiguration;
import io.ballerina.lib.solace.config.ConsumerSubscriptionConfig;
import io.ballerina.lib.solace.config.QueueConsumerConfig;
import io.ballerina.lib.solace.config.TopicConsumerConfig;
import io.ballerina.lib.solace.observability.SolaceMetricsUtil;
import io.ballerina.lib.solace.observability.SolaceSessionEventHandler;
import io.ballerina.lib.solace.observability.SolaceTracingUtil;
import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BDecimal;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BTypedesc;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static io.ballerina.lib.solace.common.Constants.NATIVE_CLOSED;
import static io.ballerina.lib.solace.common.Constants.NATIVE_CLOSING;
import static io.ballerina.lib.solace.common.Constants.NATIVE_CONSUMER_FLOW_STATE_TRACKER;
import static io.ballerina.lib.solace.common.Constants.NATIVE_CONSUMER;
import static io.ballerina.lib.solace.common.Constants.NATIVE_DESTINATION;
import static io.ballerina.lib.solace.common.Constants.NATIVE_FLOW;
import static io.ballerina.lib.solace.common.Constants.NATIVE_SETTLE_UNDELIVERED;
import static io.ballerina.lib.solace.common.Constants.NATIVE_SESSION;
import static io.ballerina.lib.solace.common.Constants.NATIVE_SUBSCRIPTION_TYPE;
import static io.ballerina.lib.solace.common.Constants.NATIVE_TRANSACTED;
import static io.ballerina.lib.solace.common.Constants.NATIVE_TX_LOCK;
import static io.ballerina.lib.solace.common.Constants.NATIVE_TX_SESSION;
import static io.ballerina.lib.solace.common.Constants.NATIVE_EVENT_HANDLER;
import static io.ballerina.lib.solace.common.Constants.NATIVE_URL;
import static io.ballerina.lib.solace.common.Constants.NATIVE_VPN;
import static io.ballerina.lib.solace.common.MessageFieldConstants.PAYLOAD_KEY;
import static io.ballerina.lib.solace.consumer.ConsumerUtils.SUBSCRIPTION_TYPE_DIRECT_TOPIC;
import static io.ballerina.lib.solace.consumer.ConsumerUtils.SUBSCRIPTION_TYPE_DURABLE_TOPIC;
import static io.ballerina.lib.solace.consumer.ConsumerUtils.SUBSCRIPTION_TYPE_QUEUE;
import static io.ballerina.lib.solace.consumer.ConsumerUtils.createDirectTopicConsumer;
import static io.ballerina.lib.solace.consumer.ConsumerUtils.createDurableTopicConsumer;
import static io.ballerina.lib.solace.consumer.ConsumerUtils.createQueueConsumer;
import static io.ballerina.lib.solace.observability.SolaceMetricsUtil.reportConsumerFailure;
import static io.ballerina.lib.solace.observability.SolaceObservabilityConstants.CONTEXT_CONSUMER;
import static io.ballerina.lib.solace.observability.SolaceObservabilityConstants.ERROR_TYPE_ACKNOWLEDGE;
import static io.ballerina.lib.solace.observability.SolaceObservabilityConstants.ERROR_TYPE_CLOSE;
import static io.ballerina.lib.solace.observability.SolaceObservabilityConstants.ERROR_TYPE_COMMIT;
import static io.ballerina.lib.solace.observability.SolaceObservabilityConstants.ERROR_TYPE_NACK;
import static io.ballerina.lib.solace.observability.SolaceObservabilityConstants.ERROR_TYPE_RECEIVE;
import static io.ballerina.lib.solace.observability.SolaceObservabilityConstants.ERROR_TYPE_ROLLBACK;
import static io.ballerina.lib.solace.observability.SolaceObservabilityConstants.UNKNOWN;

/**
 * Consumer actions - main entry point for Ballerina MessageConsumer interop.
 */
public class ConsumerActions {

    private static final String INACTIVE_FLOW_ERROR = "InactiveFlowError";
    private static final String FLOW_DOWN_ERROR = "FlowDownError";
    // Returned by a receive task that the consumer's close ended, so the caller does not count it as a receive error.
    private static final Object CONSUMER_CLOSED = new Object();
    private static final int NO_WAIT = -1;

    /**
     * Initialize the consumer with connection URL and configuration. Creates either a transacted or non-transacted
     * consumer based on configuration.
     *
     * @param consumer the Ballerina consumer object
     * @param url      the broker URL
     * @param config   the consumer configuration
     * @return null on success, BError on failure
     */
    public static BError init(BObject consumer, BString url, BMap<BString, Object> config) {
        JCSMPSession session = null;
        TransactedSession txSession = null;
        String messageVpn = UNKNOWN;
        try {
            // Parse configuration
            ConsumerConfiguration consumerConfig = new ConsumerConfiguration(config);
            ConsumerSubscriptionConfig subscriptionConfig = consumerConfig.subscriptionConfig();
            boolean isTransacted = consumerConfig.connectionConfig().transacted();
            messageVpn = consumerConfig.connectionConfig().messageVpn();

            // Build JCSMP properties from configuration
            JCSMPProperties jcsmpProps =
                    ConfigurationUtils.buildJCSMPProperties(url.getValue(), consumerConfig.connectionConfig());
            ConfigurationUtils.applyReceiveTimestampProperty(jcsmpProps, consumerConfig.generateReceiveTimestamps(),
                    consumerConfig.calculateMessageExpiration());

            // Create and connect base JCSMP session.
            SolaceSessionEventHandler eventHandler =
                    new SolaceSessionEventHandler(CONTEXT_CONSUMER, url.getValue(), messageVpn);
            session = JCSMPFactory.onlyInstance().createSession(jcsmpProps, null, eventHandler);
            session.connect();
            eventHandler.markConnected();

            // Validate: Direct topic subscriptions cannot be transacted
            if (isTransacted && subscriptionConfig instanceof TopicConsumerConfig topicConfig &&
                    !topicConfig.isDurable()) {
                cleanupOnInitFailure(session, null);
                return CommonUtils.createError("Transacted mode is not supported for direct topic subscriptions. " +
                        "Use DURABLE endpoint type for guaranteed delivery with transactions.");
            }

            // Create TransactedSession if in transacted mode
            txSession = isTransacted ? session.createTransactedSession() : null;
            final JCSMPSession finalSession = session;
            final TransactedSession finalTxSession = txSession;

            // Store session references and transacted flag
            consumer.addNativeData(NATIVE_SESSION, session);
            consumer.addNativeData(NATIVE_TX_SESSION, txSession);
            consumer.addNativeData(NATIVE_TX_LOCK, txSession != null ? new ReentrantLock() : null);
            consumer.addNativeData(NATIVE_TRANSACTED, isTransacted);
            boolean directTopic = subscriptionConfig instanceof TopicConsumerConfig topicSubscription
                    && !topicSubscription.isDurable();
            consumer.addNativeData(NATIVE_SETTLE_UNDELIVERED, !isTransacted && !directTopic
                    && subscriptionConfig.ackMode() == AcknowledgementMode.CLIENT_ACK);
            consumer.addNativeData(NATIVE_CLOSED, false);
            consumer.addNativeData(NATIVE_URL, url.getValue());
            consumer.addNativeData(NATIVE_VPN, messageVpn);
            consumer.addNativeData(NATIVE_EVENT_HANDLER, eventHandler);

            // Create appropriate consumer based on subscription type
            if (subscriptionConfig instanceof QueueConsumerConfig queueConfig) {
                FlowReceiverFactory factory = isTransacted
                        ? (props, handler) -> finalTxSession.createFlow(null, props, null, handler)
                        : (props, handler) -> finalSession.createFlow(null, props, null, handler);
                createQueueConsumer(consumer, factory, queueConfig, isTransacted);
            } else if (subscriptionConfig instanceof TopicConsumerConfig topicConfig) {
                topicConfig.validate();
                if (topicConfig.isDurable()) {
                    FlowReceiverFactory factory = isTransacted
                            ? (props, handler) -> finalTxSession.createFlow(null, props, null, handler)
                            : (props, handler) -> finalSession.createFlow(null, props, null, handler);
                    createDurableTopicConsumer(consumer, factory, topicConfig, isTransacted);
                } else {
                    createDirectTopicConsumer(consumer, session, topicConfig);
                }
            } else {
                cleanupOnInitFailure(session, txSession);
                return CommonUtils.createError("Unknown subscription configuration type");
            }

        } catch (Exception e) {
            cleanupOnInitFailure(session, txSession);
            SolaceMetricsUtil.reportConnectionError(CONTEXT_CONSUMER, url.getValue(), messageVpn);
            return CommonUtils.createError("Failed to initialize consumer", e);
        }
        SolaceMetricsUtil.reportNewConsumer(consumer);
        return null;
    }

    /**
     * Best-effort cleanup of resources already created by init() when a later step fails, since nothing else
     * will ever call close() on a consumer whose init() returned an error.
     */
    private static void cleanupOnInitFailure(JCSMPSession session, TransactedSession txSession) {
        if (txSession != null) {
            CommonUtils.closeQuietly(txSession::close);
        }
        if (session != null) {
            CommonUtils.closeQuietly(session::closeSession);
        }
    }


    /**
     * Receive a message with timeout.
     *
     * @param env       the Ballerina environment, for tracing and to yield the strand while waiting
     * @param consumer  the Ballerina consumer object
     * @param timeout   the timeout in seconds, or {@code null} to never expire
     * @param bTypedesc the caller-declared expected message type
     * @return the received message, null if timeout, or BError on failure
     */
    public static Object receive(Environment env, BObject consumer, Object timeout, BTypedesc bTypedesc) {
        SolaceTracingUtil.traceResourceInvocation(env, consumer);
        Boolean closed = (Boolean) consumer.getNativeData(NATIVE_CLOSED);
        if (closed != null && closed) {
            return reportConsumerFailure(consumer, ERROR_TYPE_RECEIVE, "Consumer is closed");
        }
        BigDecimal timeoutDecimal = timeout instanceof BDecimal bDecimal ? bDecimal.decimalValue() : BigDecimal.ZERO;
        long timeoutMs = timeoutDecimal.multiply(BigDecimal.valueOf(1000)).longValue();
        String subscriptionType = (String) consumer.getNativeData(NATIVE_SUBSCRIPTION_TYPE);

        // Read before yielding: a close() from another strand clears these while this one waits.
        FlowReceiver flowReceiver = (FlowReceiver) consumer.getNativeData(NATIVE_FLOW);
        XMLMessageConsumer xmlConsumer = (XMLMessageConsumer) consumer.getNativeData(NATIVE_CONSUMER);
        ReentrantLock txLock = (ReentrantLock) consumer.getNativeData(NATIVE_TX_LOCK);
        try {
            Object result = CommonUtils.executeBlocking(env, () -> {
                if (txLock == null) {
                    return receiveOne(consumer, subscriptionType, flowReceiver, xmlConsumer, (int) timeoutMs,
                            bTypedesc);
                }
                // Wait for the transacted session only as long as this receive may wait, then receive for the rest.
                long startNanos = System.nanoTime();
                if (timeoutMs <= 0) {
                    txLock.lock();
                } else if (!txLock.tryLock(timeoutMs, TimeUnit.MILLISECONDS)) {
                    return emptyReceiveResult(consumer);
                }
                try {
                    long remainingMs = timeoutMs <= 0 ? 0
                            : Math.max(1, timeoutMs - TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos));
                    return receiveOne(consumer, subscriptionType, flowReceiver, xmlConsumer, (int) remainingMs,
                            bTypedesc);
                } finally {
                    txLock.unlock();
                }
            });

            if (result == CONSUMER_CLOSED) {
                return consumerClosedError();
            }
            if (result instanceof ReceiveFailure failure) {
                if (isClosing(consumer)) {
                    return consumerClosedError();
                }
                SolaceMetricsUtil.reportConsumerError(consumer, ERROR_TYPE_RECEIVE);
                if (SUBSCRIPTION_TYPE_DIRECT_TOPIC.equals(subscriptionType)) {
                    return CommonUtils.createError("Failed to receive message", failure.cause());
                }
                if (shouldMapToFlowDownError(true, flowState(consumer))) {
                    return flowDownError();
                }
                return CommonUtils.createError("Failed to receive message", failure.cause());
            }
            if (result instanceof BError bError) {
                SolaceMetricsUtil.reportConsumerError(consumer, ERROR_TYPE_RECEIVE);
                return bError;
            }
            reportReceiveOutcome(consumer, result);
            return result;
        } catch (CommonUtils.StrandCancelledException e) {
            requeueCancelledReceive(consumer, e.taskResult());
            throw e.panic();
        } catch (Exception e) {
            SolaceMetricsUtil.reportConsumerError(consumer, ERROR_TYPE_RECEIVE);
            return CommonUtils.createError("Failed to receive message", e);
        }
    }

    /**
     * Takes one message and converts it, waiting up to {@code waitMs} milliseconds: 0 waits indefinitely and
     * {@link #NO_WAIT} does not wait.
     */
    private static Object receiveOne(BObject consumer, String subscriptionType, FlowReceiver flowReceiver,
                                     XMLMessageConsumer xmlConsumer, int waitMs, BTypedesc bTypedesc) {
        BytesXMLMessage message = null;
        try {
            if (SUBSCRIPTION_TYPE_QUEUE.equals(subscriptionType) ||
                    SUBSCRIPTION_TYPE_DURABLE_TOPIC.equals(subscriptionType)) {
                if (flowReceiver == null) {
                    return isClosing(consumer) ? CONSUMER_CLOSED
                            : CommonUtils.createError("Consumer flow not initialized");
                }
                message = waitMs == NO_WAIT ? flowReceiver.receiveNoWait() : flowReceiver.receive(waitMs);
            } else if (SUBSCRIPTION_TYPE_DIRECT_TOPIC.equals(subscriptionType)) {
                if (xmlConsumer == null) {
                    return isClosing(consumer) ? CONSUMER_CLOSED : CommonUtils.createError("Consumer not initialized");
                }
                message = waitMs == NO_WAIT ? xmlConsumer.receiveNoWait() : xmlConsumer.receive(waitMs);
            }
        } catch (Exception e) {
            return new ReceiveFailure(e);
        }
        if (message == null) {
            if (waitMs != NO_WAIT) {
                return emptyReceiveResult(consumer);
            }
            return isClosing(consumer) ? CONSUMER_CLOSED : null;
        }
        try {
            return MessageConverter.toBallerinaMessage(message, bTypedesc);
        } catch (BallerinaSolaceDatabindingException e) {
            return settleUndelivered(consumer, message, XMLMessage.Outcome.REJECTED,
                    CommonUtils.createError(e.getMessage()));
        } catch (Throwable t) {
            // Converting the same message fails the same way, so only a JVM Error is requeued.
            return settleUndelivered(consumer, message,
                    t instanceof Exception ? XMLMessage.Outcome.REJECTED : XMLMessage.Outcome.FAILED,
                    CommonUtils.createError("Failed to receive message", t));
        }
    }

    /**
     * Returns a message that a cancelled strand had already received to the queue, so the broker redelivers it. Only a
     * non-transacted CLIENT_ACK flow can do this: AUTO_ACK has acknowledged the message, and on a transacted consumer
     * it stays in the transaction.
     */
    @SuppressWarnings("unchecked")
    private static void requeueCancelledReceive(BObject consumer, Object taskResult) {
        if (!(taskResult instanceof BMap<?, ?> received)
                || !Boolean.TRUE.equals(consumer.getNativeData(NATIVE_SETTLE_UNDELIVERED))) {
            return;
        }
        XMLMessage message = MessageConverter.extractNativeMessage((BMap<BString, Object>) received);
        if (message != null) {
            CommonUtils.closeQuietly(() -> message.settle(XMLMessage.Outcome.FAILED));
        }
    }

    private static Object emptyReceiveResult(BObject consumer) {
        if (isClosing(consumer)) {
            return CONSUMER_CLOSED;
        }
        String errorType = flowStateErrorType(flowState(consumer));
        if (INACTIVE_FLOW_ERROR.equals(errorType)) {
            return CommonUtils.createError(errorType, "Consumer flow is inactive and cannot receive messages");
        }
        return FLOW_DOWN_ERROR.equals(errorType) ? flowDownError() : null;
    }

    static String flowStateErrorType(ConsumerFlowStateTracker.State state) {
        return switch (state) {
            case INACTIVE -> INACTIVE_FLOW_ERROR;
            case DOWN -> FLOW_DOWN_ERROR;
            default -> null;
        };
    }

    static boolean shouldMapToFlowDownError(boolean flowReceiveFailed, ConsumerFlowStateTracker.State state) {
        return flowReceiveFailed && state == ConsumerFlowStateTracker.State.DOWN;
    }

    private static BError flowDownError() {
        return CommonUtils.createError(FLOW_DOWN_ERROR, "Consumer flow is down");
    }

    private static ConsumerFlowStateTracker.State flowState(BObject consumer) {
        ConsumerFlowStateTracker tracker = (ConsumerFlowStateTracker) consumer.getNativeData(
                NATIVE_CONSUMER_FLOW_STATE_TRACKER);
        return tracker == null ? ConsumerFlowStateTracker.State.UNKNOWN : tracker.state();
    }

    private record ReceiveFailure(Exception cause) {
    }

    private static boolean isClosing(BObject consumer) {
        return Boolean.TRUE.equals(consumer.getNativeData(NATIVE_CLOSING))
                || Boolean.TRUE.equals(consumer.getNativeData(NATIVE_CLOSED));
    }

    private static BError consumerClosedError() {
        return CommonUtils.createError("Consumer is closed");
    }

    /**
     * Settles a non-transacted CLIENT_ACK message that the caller never gets: REJECTED when converting it fails the
     * same way every time, FAILED when a possibly transient JVM error stopped the conversion.
     */
    private static BError settleUndelivered(BObject consumer, BytesXMLMessage message, XMLMessage.Outcome outcome,
                                            BError receiveError) {
        if (!Boolean.TRUE.equals(consumer.getNativeData(NATIVE_SETTLE_UNDELIVERED))) {
            return receiveError;
        }
        SolaceMetricsUtil.reportConsume(consumer, message.getContentLength() + message.getAttachmentContentLength(),
                message.getRedelivered());
        try {
            message.settle(outcome);
            SolaceMetricsUtil.reportNack(consumer, outcome == XMLMessage.Outcome.FAILED);
            return receiveError;
        } catch (Exception e) {
            SolaceMetricsUtil.reportConsumerError(consumer, ERROR_TYPE_NACK);
            return CommonUtils.createErrorWithCause("Failed to settle a message that could not be received: "
                    + e.getMessage(), receiveError);
        }
    }

    /**
     * Receive a message without waiting.
     *
     * @param env       the Ballerina environment, for tracing and to yield the strand while waiting
     * @param consumer  the Ballerina consumer object
     * @param bTypedesc the caller-declared expected message type
     * @return the received message, null if none available, or BError on failure
     */
    public static Object receiveNoWait(Environment env, BObject consumer, BTypedesc bTypedesc) {
        SolaceTracingUtil.traceResourceInvocation(env, consumer);
        Boolean closed = (Boolean) consumer.getNativeData(NATIVE_CLOSED);
        if (closed != null && closed) {
            return reportConsumerFailure(consumer, ERROR_TYPE_RECEIVE, "Consumer is closed");
        }
        String subscriptionType = (String) consumer.getNativeData(NATIVE_SUBSCRIPTION_TYPE);
        FlowReceiver flowReceiver = (FlowReceiver) consumer.getNativeData(NATIVE_FLOW);
        XMLMessageConsumer xmlConsumer = (XMLMessageConsumer) consumer.getNativeData(NATIVE_CONSUMER);
        ReentrantLock txLock = (ReentrantLock) consumer.getNativeData(NATIVE_TX_LOCK);
        try {
            Object result = CommonUtils.executeBlocking(env, () -> {
                if (txLock == null) {
                    return receiveOne(consumer, subscriptionType, flowReceiver, xmlConsumer, NO_WAIT, bTypedesc);
                }
                // A receive or commit in progress on the transacted session means no message is available now.
                if (!txLock.tryLock()) {
                    return null;
                }
                try {
                    return receiveOne(consumer, subscriptionType, flowReceiver, xmlConsumer, NO_WAIT, bTypedesc);
                } finally {
                    txLock.unlock();
                }
            });

            if (result == CONSUMER_CLOSED) {
                return consumerClosedError();
            }
            if (result instanceof ReceiveFailure failure) {
                if (isClosing(consumer)) {
                    return consumerClosedError();
                }
                SolaceMetricsUtil.reportConsumerError(consumer, ERROR_TYPE_RECEIVE);
                return CommonUtils.createError("Failed to receive message", failure.cause());
            }
            if (result instanceof BError bError) {
                SolaceMetricsUtil.reportConsumerError(consumer, ERROR_TYPE_RECEIVE);
                return bError;
            }
            reportReceiveOutcome(consumer, result);
            return result;
        } catch (CommonUtils.StrandCancelledException e) {
            requeueCancelledReceive(consumer, e.taskResult());
            throw e.panic();
        } catch (Exception e) {
            SolaceMetricsUtil.reportConsumerError(consumer, ERROR_TYPE_RECEIVE);
            return CommonUtils.createError("Failed to receive message", e);
        }
    }

    /**
     * Records the outcome of a successful receive call.
     */
    @SuppressWarnings("unchecked")
    private static void reportReceiveOutcome(BObject consumer, Object result) {
        if (result == null) {
            SolaceMetricsUtil.reportEmptyReceive(consumer);
            return;
        }
        BMap<BString, Object> receivedMessage = (BMap<BString, Object>) result;
        SolaceMetricsUtil.reportConsume(consumer, getPayloadSize(receivedMessage),
                CommonUtils.isRedelivered(receivedMessage));
    }

    /**
     * Acknowledge a message.
     *
     * @param env      the Ballerina environment, used to yield the strand while waiting
     * @param consumer the Ballerina consumer object
     * @param message  the Ballerina message to acknowledge
     * @return null on success, BError on failure
     */
    public static BError acknowledge(Environment env, BObject consumer, BMap<BString, Object> message) {
        try {
            Boolean closed = (Boolean) consumer.getNativeData(NATIVE_CLOSED);
            if (closed != null && closed) {
                return reportConsumerFailure(consumer, ERROR_TYPE_ACKNOWLEDGE, "Consumer is closed");
            }

            XMLMessage nativeMessage = MessageConverter.extractNativeMessage(message);
            if (nativeMessage == null) {
                return reportConsumerFailure(consumer, ERROR_TYPE_ACKNOWLEDGE,
                        "Cannot acknowledge: native message not found");
            }

            Object result = CommonUtils.executeBlocking(env, nativeMessage::ackMessage);
            if (result instanceof BError) {
                SolaceMetricsUtil.reportConsumerError(consumer, ERROR_TYPE_ACKNOWLEDGE);
                return (BError) result;
            }
            SolaceMetricsUtil.reportAck(consumer);
            return null;
        } catch (CommonUtils.StrandCancelledException e) {
            throw e.panic();
        } catch (Exception e) {
            SolaceMetricsUtil.reportConsumerError(consumer, ERROR_TYPE_ACKNOWLEDGE);
            return CommonUtils.createError("Failed to acknowledge message", e);
        }
    }

    /**
     * Negatively acknowledge a message (NACK).
     *
     * @param env      the Ballerina environment, used to yield the strand while waiting
     * @param consumer the Ballerina consumer object
     * @param message  the Ballerina message to NACK
     * @param requeue  if true, use FAILED outcome (requeue); if false, use REJECTED outcome (DMQ)
     * @return null on success, BError on failure
     */
    public static BError nack(Environment env, BObject consumer, BMap<BString, Object> message, boolean requeue) {
        try {
            Boolean closed = (Boolean) consumer.getNativeData(NATIVE_CLOSED);
            if (closed != null && closed) {
                return reportConsumerFailure(consumer, ERROR_TYPE_NACK, "Consumer is closed");
            }

            XMLMessage nativeMessage = MessageConverter.extractNativeMessage(message);
            if (nativeMessage == null) {
                return reportConsumerFailure(consumer, ERROR_TYPE_NACK, "Cannot NACK: native message not found");
            }

            // Use settle() with appropriate outcome
            Object result = CommonUtils.executeBlocking(env, () -> {
                XMLMessage.Outcome outcome = requeue ? XMLMessage.Outcome.FAILED : XMLMessage.Outcome.REJECTED;
                nativeMessage.settle(outcome);
                return null;
            });
            if (result instanceof BError) {
                SolaceMetricsUtil.reportConsumerError(consumer, ERROR_TYPE_NACK);
                return (BError) result;
            }
            SolaceMetricsUtil.reportNack(consumer, requeue);
            return null;
        } catch (CommonUtils.StrandCancelledException e) {
            throw e.panic();
        } catch (Exception e) {
            SolaceMetricsUtil.reportConsumerError(consumer, ERROR_TYPE_NACK);
            return CommonUtils.createError("Failed to NACK message", e);
        }
    }

    /**
     * Commit the current transaction. Only valid for transacted consumers (when connectionConfig.transacted = true).
     *
     * @param env      the Ballerina environment, used to yield the strand while waiting
     * @param consumer the Ballerina consumer object
     * @return null on success, BError on failure
     */
    public static BError commit(Environment env, BObject consumer) {
        try {
            Boolean closed = (Boolean) consumer.getNativeData(NATIVE_CLOSED);
            if (closed != null && closed) {
                return reportConsumerFailure(consumer, ERROR_TYPE_COMMIT, "Consumer is closed");
            }

            Boolean transacted = (Boolean) consumer.getNativeData(NATIVE_TRANSACTED);
            if (transacted == null || !transacted) {
                return reportConsumerFailure(consumer, ERROR_TYPE_COMMIT,
                        "commit() can only be called on transacted consumers. " +
                                "Set connectionConfig.transacted = true to enable transactions.");
            }

            TransactedSession txSession = (TransactedSession) consumer.getNativeData(NATIVE_TX_SESSION);
            if (txSession == null) {
                return reportConsumerFailure(consumer, ERROR_TYPE_COMMIT, "TransactedSession not initialized");
            }

            // Commit transaction on TransactedSession (blocking operation)
            ReentrantLock txLock = (ReentrantLock) consumer.getNativeData(NATIVE_TX_LOCK);
            Object result = CommonUtils.executeBlocking(env, CommonUtils.serialized(txLock, txSession::commit));

            if (result instanceof BError) {
                SolaceMetricsUtil.reportConsumerError(consumer, ERROR_TYPE_COMMIT);
                return (BError) result;
            }

            return null;
        } catch (CommonUtils.StrandCancelledException e) {
            throw e.panic();
        } catch (Exception e) {
            SolaceMetricsUtil.reportConsumerError(consumer, ERROR_TYPE_COMMIT);
            return CommonUtils.createError("Failed to commit transaction", e);
        }
    }

    /**
     * Rollback the current transaction. Only valid for transacted consumers (when connectionConfig.transacted = true).
     *
     * @param env      the Ballerina environment, used to yield the strand while waiting
     * @param consumer the Ballerina consumer object
     * @return null on success, BError on failure
     */
    public static BError rollback(Environment env, BObject consumer) {
        try {
            Boolean closed = (Boolean) consumer.getNativeData(NATIVE_CLOSED);
            if (closed != null && closed) {
                return reportConsumerFailure(consumer, ERROR_TYPE_ROLLBACK, "Consumer is closed");
            }

            Boolean transacted = (Boolean) consumer.getNativeData(NATIVE_TRANSACTED);
            if (transacted == null || !transacted) {
                return reportConsumerFailure(consumer, ERROR_TYPE_ROLLBACK,
                        "rollback() can only be called on transacted consumers. " +
                                "Set connectionConfig.transacted = true to enable transactions.");
            }

            TransactedSession txSession = (TransactedSession) consumer.getNativeData(NATIVE_TX_SESSION);
            if (txSession == null) {
                return reportConsumerFailure(consumer, ERROR_TYPE_ROLLBACK, "TransactedSession not initialized");
            }

            // Rollback transaction on TransactedSession (blocking operation)
            ReentrantLock txLock = (ReentrantLock) consumer.getNativeData(NATIVE_TX_LOCK);
            Object result = CommonUtils.executeBlocking(env, CommonUtils.serialized(txLock, txSession::rollback));

            if (result instanceof BError) {
                SolaceMetricsUtil.reportConsumerError(consumer, ERROR_TYPE_ROLLBACK);
                return (BError) result;
            }

            return null;
        } catch (CommonUtils.StrandCancelledException e) {
            throw e.panic();
        } catch (Exception e) {
            SolaceMetricsUtil.reportConsumerError(consumer, ERROR_TYPE_ROLLBACK);
            return CommonUtils.createError("Failed to rollback transaction", e);
        }
    }

    /**
     * Close the consumer and release resources. Closes flow receiver/consumer, transacted session (if any), and base
     * session in that order.
     *
     * @param env      the Ballerina environment, for tracing and to yield the strand while waiting
     * @param consumer the Ballerina consumer object
     * @return null on success, BError on failure
     */
    public static BError close(Environment env, BObject consumer) {
        SolaceTracingUtil.traceResourceInvocation(env, consumer);
        return CommonUtils.yieldToClose(env, () -> closeConsumer(consumer));
    }

    private static BError closeConsumer(BObject consumer) {
        // close now yields, so two strands can reach here together; only the first one closes.
        synchronized (consumer) {
            if (Boolean.TRUE.equals(consumer.getNativeData(NATIVE_CLOSED))) {
                return null;
            }
            return closeConsumerResources(consumer);
        }
    }

    private static BError closeConsumerResources(BObject consumer) {
        // Set before closing so a receive woken by the close reports "Consumer is closed".
        consumer.addNativeData(NATIVE_CLOSING, true);
        String subscriptionType = (String) consumer.getNativeData(NATIVE_SUBSCRIPTION_TYPE);
        FlowReceiver flowReceiver = (FlowReceiver) consumer.getNativeData(NATIVE_FLOW);
        XMLMessageConsumer xmlConsumer = (XMLMessageConsumer) consumer.getNativeData(NATIVE_CONSUMER);
        TransactedSession txSession = (TransactedSession) consumer.getNativeData(NATIVE_TX_SESSION);
        JCSMPSession session = (JCSMPSession) consumer.getNativeData(NATIVE_SESSION);

        // Attempt to close every resource independently so one failure doesn't block the rest.
        Exception firstError = null;
        if (SUBSCRIPTION_TYPE_QUEUE.equals(subscriptionType) ||
                SUBSCRIPTION_TYPE_DURABLE_TOPIC.equals(subscriptionType)) {
            if (flowReceiver != null) {
                firstError = CommonUtils.attemptClose(flowReceiver::stop);
                Exception e = CommonUtils.attemptClose(flowReceiver::close);
                firstError = firstError == null ? e : firstError;
            }
        } else if (SUBSCRIPTION_TYPE_DIRECT_TOPIC.equals(subscriptionType)) {
            if (xmlConsumer != null) {
                firstError = CommonUtils.attemptClose(xmlConsumer::stop);
                Exception e = CommonUtils.attemptClose(xmlConsumer::close);
                firstError = firstError == null ? e : firstError;
            }
        }

        if (txSession != null) {
            Exception e = CommonUtils.attemptClose(txSession::close);
            firstError = firstError == null ? e : firstError;
        }

        if (session != null) {
            Exception e = CommonUtils.attemptClose(session::closeSession);
            firstError = firstError == null ? e : firstError;
        }

        // Mark as closed and clear native data regardless of partial failures above.
        consumer.addNativeData(NATIVE_CLOSED, true);
        consumer.addNativeData(NATIVE_FLOW, null);
        consumer.addNativeData(NATIVE_CONSUMER, null);
        consumer.addNativeData(NATIVE_TX_SESSION, null);
        consumer.addNativeData(NATIVE_TRANSACTED, null);
        consumer.addNativeData(NATIVE_SESSION, null);

        SolaceSessionEventHandler.markDisconnected(consumer);

        if (firstError != null) {
            SolaceMetricsUtil.reportConsumerError(consumer, ERROR_TYPE_CLOSE);
            return CommonUtils.createError("Failed to close consumer", firstError);
        }

        SolaceMetricsUtil.reportConsumerClose(consumer);
        return null;
    }

    /**
     * Returns the resolved name of the destination (queue or topic) this consumer is bound to. For a TEMPORARY
     * queue created without a name hint, this is the broker-generated name.
     *
     * @param consumer the Ballerina consumer object
     * @return the destination name
     */
    public static BString destinationName(BObject consumer) {
        String destinationName = (String) consumer.getNativeData(NATIVE_DESTINATION);
        return StringUtils.fromString(destinationName);
    }

    @SuppressWarnings("unchecked")
    private static int getPayloadSize(BMap<BString, Object> message) {
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
}
