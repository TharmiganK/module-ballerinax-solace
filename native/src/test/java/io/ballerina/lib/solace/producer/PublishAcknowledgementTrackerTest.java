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

package io.ballerina.lib.solace.producer;

import com.solacesystems.jcsmp.JCSMPException;
import org.testng.annotations.Test;

import java.time.Duration;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class PublishAcknowledgementTrackerTest {

    private static PublishAcknowledgementTracker.DrainResult close(PublishAcknowledgementTracker tracker)
            throws InterruptedException {
        tracker.beginClose();
        return tracker.awaitSettlement(Duration.ZERO);
    }

    @Test
    public void testPlainPublishRejectionIsCountedByClose() throws InterruptedException {
        PublishAcknowledgementTracker tracker = new PublishAcknowledgementTracker();
        long key = tracker.register();
        tracker.reject(key, new JCSMPException("queue full"));

        PublishAcknowledgementTracker.DrainResult result = close(tracker);
        assertEquals(result.rejectedCount(), 1);
        assertEquals(result.firstRejection(), "queue full");
    }

    @Test
    public void testClaimedAwaitedRejectionIsNotCountedAgainByClose() throws InterruptedException {
        PublishAcknowledgementTracker tracker = new PublishAcknowledgementTracker();
        PublishAcknowledgementTracker.AwaitedPublish publish = tracker.registerAwaited();
        tracker.reject(publish.key(), new JCSMPException("queue full"));
        assertTrue(publish.confirmation().isCompletedExceptionally());
        tracker.claimRejection(publish.key());

        PublishAcknowledgementTracker.DrainResult result = close(tracker);
        assertEquals(result.rejectedCount(), 0);
        assertNull(result.firstRejection());
    }

    @Test
    public void testAwaitedRejectionSettledBeforeCancellationIsCountedByClose() throws InterruptedException {
        PublishAcknowledgementTracker tracker = new PublishAcknowledgementTracker();
        PublishAcknowledgementTracker.AwaitedPublish publish = tracker.registerAwaited();
        // The rejection settles, then the awaiting strand is cancelled before it claims the rejection.
        tracker.reject(publish.key(), new JCSMPException("queue full"));
        tracker.detachAwaiter(publish.key());

        PublishAcknowledgementTracker.DrainResult result = close(tracker);
        assertEquals(result.rejectedCount(), 1);
        assertEquals(result.firstRejection(), "queue full");
    }

    @Test
    public void testAwaitedRejectionSettledBeforeCloseSnapshotIsCountedEvenIfNeverClaimed()
            throws InterruptedException {
        PublishAcknowledgementTracker tracker = new PublishAcknowledgementTracker();
        PublishAcknowledgementTracker.AwaitedPublish publish = tracker.registerAwaited();
        // Close takes its snapshot between the rejection and the cancelled strand's cleanup.
        tracker.reject(publish.key(), new JCSMPException("queue full"));
        PublishAcknowledgementTracker.DrainResult result = close(tracker);
        tracker.detachAwaiter(publish.key());

        assertEquals(result.rejectedCount(), 1);
    }

    @Test
    public void testRejectionAfterAwaiterIsDetachedIsCountedByClose() throws InterruptedException {
        PublishAcknowledgementTracker tracker = new PublishAcknowledgementTracker();
        PublishAcknowledgementTracker.AwaitedPublish publish = tracker.registerAwaited();
        assertTrue(tracker.detachAwaiter(publish.key()));
        tracker.reject(publish.key(), new JCSMPException("queue full"));

        PublishAcknowledgementTracker.DrainResult result = close(tracker);
        assertEquals(result.rejectedCount(), 1);
        assertEquals(result.unconfirmedCount(), 0);
    }
}
