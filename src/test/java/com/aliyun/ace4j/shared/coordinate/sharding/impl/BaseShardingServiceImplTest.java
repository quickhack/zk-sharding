package com.aliyun.ace4j.shared.coordinate.sharding.impl;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.curator.test.TestingServer;

/**
 * @author ding.lid
 */
public abstract class BaseShardingServiceImplTest {
    private static final Logger logger = LoggerFactory.getLogger(BaseShardingServiceImplTest.class);

    volatile static TestingServer testingServer;

    @AfterClass
    public static void afterClass() throws Exception {
        if (null != testingServer) {
            testingServer.close();
        }
    }

    List<ShardingServiceImpl> shardingServiceList = new ArrayList<ShardingServiceImpl>();

    @After
    public void after() throws Exception {
        for (ShardingServiceImpl shardingService : shardingServiceList) {
            try {
                shardingService.destroy();
            } catch (Exception e) {
                logger.error("error when close ShardingService", e);
            }
        }
    }
}
