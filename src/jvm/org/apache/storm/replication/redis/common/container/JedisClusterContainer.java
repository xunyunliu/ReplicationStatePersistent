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
package org.apache.storm.replication.redis.common.container;

import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisCommands;

import java.io.Closeable;


/**
 * Container for managing JedisCluster.
 * <p/>
 * Note that JedisCluster doesn't need to be pooled since it's thread-safe and it stores pools internally.
 */
public class JedisClusterContainer implements JedisCommandsInstanceContainer, Closeable {

    private JedisCluster jedisCluster;

    /**
     * Constructor
     * @param jedisCluster JedisCluster instance
     */
    public JedisClusterContainer(JedisCluster jedisCluster) {
        this.jedisCluster = jedisCluster;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JedisCommands getInstance() {
        return this.jedisCluster;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void returnInstance(JedisCommands jedisCommands) {
        // do nothing
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        this.jedisCluster.close();
    }
}
