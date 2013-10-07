package com.aliyun.ace4j.shared.coordinate.sharding.impl;

import java.io.File;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.zookeeper.CreateMode;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.curator.retry.RetryUntilElapsed;
import com.netflix.curator.test.TestingServer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author ding.lid
 */
public class CreatorHelper {
    public static final String SHARDING_BASE_DIR = "/sharding";

    public static ShardingServiceImpl createShardingService(String zkConnectString, String shardingBaseDir, String key) throws Exception {
        ShardingServiceImpl shardingService = new ShardingServiceImpl();
        shardingService.setKeys(key);
        shardingService.setShardingBaseDir(shardingBaseDir);
        shardingService.setZkAddress(zkConnectString);
        shardingService.init();

        return shardingService;
    }

    public static TestingServer createZkServerWithNodes(List<String[]> nodes) throws Exception {
        return createZkServerWithNodes(-1, nodes);
    }

    public static TestingServer createZkServerWithNodes(int zkPort, List<String[]> nodes) throws Exception {
        return createZkServerWithNodes(zkPort, null, nodes);
    }

    public static TestingServer createZkServerWithNodes(int zkPort, File tempDirectory, List<String[]> nodes) throws Exception {
        TestingServer testingServer = new TestingServer(zkPort, tempDirectory);

        CuratorFramework client = CuratorFrameworkFactory.newClient(testingServer.getConnectString(),
                new RetryUntilElapsed(1000 * 3, 1000));
        client.start();

        if(null != nodes) for (String[] n : nodes) {
            assertEquals(2, n.length);

            if (null != n[1]) {
                byte[] data = n[1].getBytes(Charset.forName("UTF-8"));
                client.create().withMode(CreateMode.PERSISTENT).forPath(n[0], data);

                assertNotNull(client.checkExists().forPath(n[0]));
                assertArrayEquals(data, client.getData().forPath(n[0]));
            } else {
                client.create().withMode(CreateMode.PERSISTENT).forPath(n[0], new byte[0]);

                assertNotNull(client.checkExists().forPath(n[0]));
                assertEquals(0, client.getData().forPath(n[0]).length);
            }
        }

        client.close();

        return testingServer;
    }

    public static void addNodes(String zkConnectString, List<String[]> nodes) throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(zkConnectString,
                new ExponentialBackoffRetry(1000, Integer.MAX_VALUE, 2 * 60 * 1000));
        client.start();

        for (String[] n : nodes) {
            assertEquals(2, n.length);

            if (null != n[1]) {
                byte[] data = n[1].getBytes(Charset.forName("UTF-8"));
                client.create().withMode(CreateMode.PERSISTENT).forPath(n[0], data);

                assertNotNull(client.checkExists().forPath(n[0]));
                assertArrayEquals(data, client.getData().forPath(n[0]));
            } else {
                client.create().withMode(CreateMode.PERSISTENT).forPath(n[0], new byte[0]);

                assertNotNull(client.checkExists().forPath(n[0]));
                assertEquals(0, client.getData().forPath(n[0]).length);
            }
        }

        client.close();
    }

    public static void updateNodes(String zkConnectString, List<String[]> nodes) throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(zkConnectString,
                new ExponentialBackoffRetry(1000, Integer.MAX_VALUE, 2 * 60 * 1000));
        client.start();

        for (String[] n : nodes) {
            assertEquals(2, n.length);

            if (null != n[1]) {
                byte[] data = n[1].getBytes(Charset.forName("UTF-8"));
                client.setData().forPath(n[0], data);

                assertNotNull(client.checkExists().forPath(n[0]));
                assertArrayEquals(data, client.getData().forPath(n[0]));
            } else {
                client.setData().forPath(n[0], new byte[0]);

                assertNotNull(client.checkExists().forPath(n[0]));
                assertEquals(0, client.getData().forPath(n[0]).length);
            }
        }

        client.close();
    }
}
