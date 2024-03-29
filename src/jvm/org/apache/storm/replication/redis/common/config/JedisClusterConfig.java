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
package org.apache.storm.replication.redis.common.config;

import com.google.common.base.Preconditions;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Protocol;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;


/**
 * Configuration for JedisCluster.
 */
public class JedisClusterConfig implements Serializable {
    private Set<InetSocketAddress> nodes;
    private int timeout;
    private int maxRedirections;

    /**
     * Constructor
     * <p/>
     * You can use JedisClusterConfig.Builder() for leaving some fields to apply default value.
     * <p/>
     * Note that list of node is mandatory, and when you didn't set nodes, it throws NullPointerException.
     *
     * @param nodes list of node information for JedisCluster
     * @param timeout socket / connection timeout
     * @param maxRedirections limit of redirections - how much we'll follow MOVED or ASK
     * @throws NullPointerException when you didn't set nodes
     */
    public JedisClusterConfig(Set<InetSocketAddress> nodes, int timeout, int maxRedirections) {
        Preconditions.checkNotNull(nodes, "Node information should be presented");

        this.nodes = nodes;
        this.timeout = timeout;
        this.maxRedirections = maxRedirections;
    }

    /**
     * Returns nodes.
     * @return list of node information
     */
    public Set<HostAndPort> getNodes() {
        Set<HostAndPort> ret = new HashSet<>();
        for (InetSocketAddress node : nodes) {
            ret.add(new HostAndPort(node.getHostName(), node.getPort()));
        }
        return ret;
    }

    /**
     * Returns socket / connection timeout.
     * @return socket / connection timeout
     */
    public int getTimeout() {
        return timeout;
    }

    /**
     * Returns limit of redirection.
     * @return limit of redirection
     */
    public int getMaxRedirections() {
        return maxRedirections;
    }

    /**
     * Builder for initializing JedisClusterConfig.
     */
    public static class Builder {
        private Set<InetSocketAddress> nodes;
        private int timeout = Protocol.DEFAULT_TIMEOUT;
        private int maxRedirections = 5;

        /**
         * Sets list of node.
         * @param nodes list of node
         * @return Builder itself
         */
        public Builder setNodes(Set<InetSocketAddress> nodes) {
            this.nodes = nodes;
            return this;
        }

        /**
         * Sets socket / connection timeout.
         * @param timeout socket / connection timeout
         * @return Builder itself
         */
        public Builder setTimeout(int timeout) {
            this.timeout = timeout;
            return this;
        }

        /**
         * Sets limit of redirection.
         * @param maxRedirections limit of redirection
         * @return Builder itself
         */
        public Builder setMaxRedirections(int maxRedirections) {
            this.maxRedirections = maxRedirections;
            return this;
        }

        /**
         * Builds JedisClusterConfig.
         * @return JedisClusterConfig
         */
        public JedisClusterConfig build() {
            return new JedisClusterConfig(nodes, timeout, maxRedirections);
        }
    }
}
