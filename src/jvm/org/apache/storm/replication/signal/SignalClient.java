package org.apache.storm.replication.signal;

import java.io.IOException;

import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;

public class SignalClient {

    private static final Logger LOG = LoggerFactory.getLogger(SignalClient.class);

    //client used for manipulating zookeeper
    private CuratorFramework client = null;
    
    //the name of receiver component
    private String name;

    public SignalClient(String zkConnectString, String name) {
        this.name = name;
        this.client = CuratorFrameworkFactory.builder().namespace("replication-signals").connectString(zkConnectString)
		        .retryPolicy(new RetryOneTime(500)).build();
        LOG.debug("created Curator client");
    }

    public void start() {
        this.client.start();
    }

    public void close() {
        this.client.close();
    }

    public void send(byte[] signal) throws Exception {
        Stat stat = this.client.checkExists().forPath(this.name);
        if (stat == null) {
            String path = this.client.create().creatingParentsIfNeeded().forPath(this.name);
            LOG.info("Created: " + path);
        }
        this.client.setData().forPath(this.name, signal);
    }

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        SignalClient sc = new SignalClient("localhost:2000", "test-signal-spout");
        sc.start();
        try {
            sc.send("Hello Signal Spout!".getBytes());
        } finally {
            sc.close();
        }
    }

}
