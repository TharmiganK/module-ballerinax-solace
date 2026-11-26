/*
 * Copyright (c) 2026, WSO2 LLC. (http://www.wso2.org).
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.ballerina.lib.solace.config;

import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;
import org.testng.annotations.Test;

import java.math.BigDecimal;

import static org.testng.Assert.assertEquals;

/**
 * Unit tests for TopicConsumerConfig.validate() that run without a broker.
 * Covers validation of messageSelector on TEMPORARY topics and endpointName on DURABLE topics.
 */
public class TopicConsumerConfigTest {

    private static final BString TOPIC_NAME = StringUtils.fromString("test/topic");
    private static final BString ACK_MODE_AUTO = StringUtils.fromString("AUTO_ACK");
    private static final BString ACK_MODE_CLIENT = StringUtils.fromString("CLIENT_ACK");
    private static final Object RECONNECT_INTERVAL = ValueCreator.createDecimalValue(new BigDecimal("3.0"));

    /**
     * Helper to create a valid TopicConsumerConfig map with required fields.
     * All other fields are optional and can be added as needed.
     */
    private static BMap<BString, Object> baseConfig() {
        BMap<BString, Object> config = ValueCreator.createMapValue();
        config.put(topicNameKey(), TOPIC_NAME);
        config.put(ackModeKey(), ACK_MODE_AUTO);
        config.put(reconnectRetryIntervalKey(), RECONNECT_INTERVAL);
        return config;
    }

    private static BString topicNameKey() {
        return StringUtils.fromString("topicName");
    }

    private static BString ackModeKey() {
        return StringUtils.fromString("ackMode");
    }

    private static BString reconnectRetryIntervalKey() {
        return StringUtils.fromString("reconnectRetryInterval");
    }

    private static BString messageSelectorKey() {
        return StringUtils.fromString("messageSelector");
    }

    private static BString durabilityKey() {
        return StringUtils.fromString("durability");
    }

    private static BString endpointNameKey() {
        return StringUtils.fromString("endpointName");
    }

    @Test
    public void testTemporaryTopicWithNonEmptySelectorThrows() {
        BMap<BString, Object> config = baseConfig();
        config.put(messageSelectorKey(), StringUtils.fromString("header > 10"));
        // durability defaults to TEMPORARY

        try {
            new TopicConsumerConfig(config).validate();
            org.testng.Assert.fail("Expected IllegalArgumentException for non-empty selector on TEMPORARY topic");
        } catch (IllegalArgumentException ex) {
            assertEquals(ex.getMessage(), "messageSelector is not supported for direct topic subscriptions");
        }
    }

    @Test
    public void testTemporaryTopicWithEmptySelectorPasses() {
        BMap<BString, Object> config = baseConfig();
        config.put(messageSelectorKey(), StringUtils.fromString(""));
        // durability defaults to TEMPORARY

        // Should not throw
        new TopicConsumerConfig(config).validate();
    }

    @Test
    public void testTemporaryTopicWithAbsentSelectorPasses() {
        BMap<BString, Object> config = baseConfig();
        // selector key not present
        // durability defaults to TEMPORARY

        // Should not throw
        new TopicConsumerConfig(config).validate();
    }

    @Test
    public void testDurableTopicWithSelectorAndEndpointNamePasses() {
        BMap<BString, Object> config = baseConfig();
        config.put(messageSelectorKey(), StringUtils.fromString("header > 10"));
        config.put(durabilityKey(), StringUtils.fromString("DURABLE"));
        config.put(endpointNameKey(), StringUtils.fromString("test-endpoint"));

        // Should not throw
        new TopicConsumerConfig(config).validate();
    }

    @Test
    public void testDurableTopicWithSelectorMissingEndpointNameThrows() {
        BMap<BString, Object> config = baseConfig();
        config.put(messageSelectorKey(), StringUtils.fromString("header > 10"));
        config.put(durabilityKey(), StringUtils.fromString("DURABLE"));
        // endpointName missing

        try {
            new TopicConsumerConfig(config).validate();
            org.testng.Assert.fail("Expected IllegalArgumentException for missing endpointName on DURABLE topic");
        } catch (IllegalArgumentException ex) {
            assertEquals(ex.getMessage(), "endpointName is required when the topic is DURABLE");
        }
    }
}
