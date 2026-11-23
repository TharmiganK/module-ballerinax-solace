// Copyright (c) 2026 WSO2 LLC. (http://www.wso2.com).
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import ballerina/http;
import ballerina/lang.runtime;
import ballerina/test;

const string SEMP_URL = "http://localhost:8080";
const string RECONNECT_CONSUMER_TOPIC = "test/reconnect/direct/consumer";
const string RECONNECT_LISTENER_TOPIC = "test/reconnect/direct/listener";
const string RECONNECT_CONSUMER_CLIENT = "reconnect-direct-consumer";
const string RECONNECT_LISTENER_CLIENT = "reconnect-direct-listener";
const decimal RECONNECT_SETTLE_TIME = 5;

final http:Client sempClient = check new (SEMP_URL, auth = {username: "admin", password: "admin"});

// Forces the broker to drop the client's session; the connector then reconnects it.
isolated function disconnectClient(string clientName) returns error? {
    http:Response response = check sempClient->put(
            string `/SEMP/v2/action/msgVpns/${MESSAGE_VPN}/clients/${clientName}/disconnect`, {});
    test:assertEquals(response.statusCode, 200, "SEMP disconnect should succeed");
}

@test:Config {groups: ["reconnect"]}
function testDirectConsumerReceivesAfterReconnect() returns error? {
    MessageProducer producer = check new (BROKER_URL, {...connectionConfig()});
    MessageConsumer consumer = check new (BROKER_URL, {
        messageVpn: MESSAGE_VPN,
        auth: {username: BROKER_USERNAME, password: BROKER_PASSWORD},
        clientName: RECONNECT_CONSUMER_CLIENT,
        retryConfig: {reconnectRetries: 20, reconnectRetryWait: 1},
        subscriptionConfig: {topicName: RECONNECT_CONSUMER_TOPIC}
    });

    check producer->send({payload: "before"}, {topicName: RECONNECT_CONSUMER_TOPIC});
    StringPayloadMessage? before = check consumer->receive(DEFAULT_RECEIVE_TIMEOUT);

    check disconnectClient(RECONNECT_CONSUMER_CLIENT);
    runtime:sleep(RECONNECT_SETTLE_TIME);

    check producer->send({payload: "after"}, {topicName: RECONNECT_CONSUMER_TOPIC});
    StringPayloadMessage? after = check consumer->receive(DEFAULT_RECEIVE_TIMEOUT);
    check consumer->close();
    check producer->close();

    test:assertEquals(before?.payload, "before");
    test:assertEquals(after?.payload, "after", "The direct subscription should be restored after the reconnect");
}

final Recorder reconnectRecorder = new;

Service reconnectDirectService = @ServiceConfig {
    topicName: RECONNECT_LISTENER_TOPIC
} service object {
    remote function onMessage(StringPayloadMessage message) {
        reconnectRecorder.add(message.payload);
    }
};

@test:Config {groups: ["reconnect"]}
function testDirectServiceReceivesAfterReconnect() returns error? {
    MessageProducer producer = check new (BROKER_URL, {...connectionConfig()});
    Listener directListener = check new (BROKER_URL, {
        messageVpn: MESSAGE_VPN,
        auth: {username: BROKER_USERNAME, password: BROKER_PASSWORD},
        clientName: RECONNECT_LISTENER_CLIENT,
        retryConfig: {reconnectRetries: 20, reconnectRetryWait: 1}
    });
    check directListener.attach(reconnectDirectService);
    check directListener.'start();

    check producer->send({payload: "before"}, {topicName: RECONNECT_LISTENER_TOPIC});
    waitForMessages(reconnectRecorder, 1);

    check disconnectClient(RECONNECT_LISTENER_CLIENT);
    runtime:sleep(RECONNECT_SETTLE_TIME);

    check producer->send({payload: "after"}, {topicName: RECONNECT_LISTENER_TOPIC});
    waitForMessages(reconnectRecorder, 2);
    error? stopped = directListener.gracefulStop();
    check producer->close();

    test:assertTrue(reconnectRecorder.contains("before"));
    test:assertTrue(reconnectRecorder.contains("after"),
            "The direct subscription should be restored after the reconnect");
    test:assertTrue(stopped is (), "gracefulStop should succeed after a reconnect");
}
