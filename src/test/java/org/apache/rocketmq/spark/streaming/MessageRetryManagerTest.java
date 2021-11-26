/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.spark.streaming;

import org.apache.rocketmq.common.message.MessageExt;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;


public class MessageRetryManagerTest {
    MessageRetryManager messageRetryManager;
    Map<String,MessageSet> cache;
    BlockingQueue<MessageSet> queue;

    @BeforeAll
    public void prepare() {
        cache = new ConcurrentHashMap<>(10);
        queue = new LinkedBlockingDeque<>(10);
        int maxRetry = 3;
        int ttl = 2000;
        messageRetryManager = new DefaultMessageRetryManager(queue, maxRetry, ttl);
        ((DefaultMessageRetryManager)messageRetryManager).setCache(cache);
    }

    @Test
    public void testRetryLogics() {
        List<MessageExt> data = new ArrayList<>();
        //ack
        MessageSet messageSet = new MessageSet(data);
        messageRetryManager.mark(messageSet);
        Assertions.assertEquals(1, cache.size());
        Assertions.assertTrue(cache.containsKey(messageSet.getId()));

        messageRetryManager.ack(messageSet.getId());
        Assertions.assertEquals(0, cache.size());
        Assertions.assertFalse(cache.containsKey(messageSet.getId()));


        //fail need retry: retries < maxRetry
        messageSet = new MessageSet(data);
        messageRetryManager.mark(messageSet);
        Assertions.assertEquals(1, cache.size());
        Assertions.assertTrue(cache.containsKey(messageSet.getId()));

        messageRetryManager.fail(messageSet.getId());
        Assertions.assertEquals(0, cache.size());
        Assertions.assertFalse(cache.containsKey(messageSet.getId()));
        Assertions.assertEquals(1, messageSet.getRetries());
        Assertions.assertEquals(1, queue.size());
        Assertions.assertEquals(messageSet, queue.poll());


        //fail need not retry: retries >= maxRetry
        messageSet = new MessageSet(data);
        messageRetryManager.mark(messageSet);
        messageRetryManager.fail(messageSet.getId());
        Assertions.assertEquals(0, cache.size());
        Assertions.assertFalse(cache.containsKey(messageSet.getId()));

        messageRetryManager.mark(messageSet);
        messageRetryManager.fail(messageSet.getId());
        Assertions.assertEquals(2, messageSet.getRetries());
        messageRetryManager.mark(messageSet);
        messageRetryManager.fail(messageSet.getId());
        Assertions.assertEquals(3, messageSet.getRetries());

        Assertions.assertFalse(messageRetryManager.needRetry(messageSet));
        messageRetryManager.mark(messageSet);
        messageRetryManager.fail(messageSet.getId());
        Assertions.assertEquals(0, cache.size());
        Assertions.assertEquals(3, queue.size());
        Assertions.assertEquals(messageSet, queue.poll());


        //fail: no ack/fail received in ttl
        messageSet = new MessageSet(data);
        messageRetryManager.mark(messageSet);
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Assertions.assertEquals(0, cache.size());
        Assertions.assertFalse(cache.containsKey(messageSet.getId()));

    }
}
