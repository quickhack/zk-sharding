package com.aliyun.ace4j.shared.coordinate.sharding.impl;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.ace4j.shared.coordinate.sharding.ShardingInfo;
import com.aliyun.ace4j.shared.coordinate.sharding.ShardingListener;
import com.aliyun.ace4j.shared.coordinate.sharding.support.TrueShardingRule;

import static com.aliyun.ace4j.shared.coordinate.sharding.impl.CreatorHelper.SHARDING_BASE_DIR;
import static com.aliyun.ace4j.shared.coordinate.sharding.impl.CreatorHelper.createShardingService;
import static com.aliyun.ace4j.shared.coordinate.sharding.impl.CreatorHelper.createZkServerWithNodes;
import static java.lang.Thread.sleep;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * @author ding.lid
 */
public class ShardingServiceImpl_Abnormal_ZkCrash_Test extends BaseShardingServiceImplTest {
    private static final Logger logger = LoggerFactory.getLogger(ShardingServiceImpl_Abnormal_ZkCrash_Test.class);

    static volatile Integer port = null;


    static void startZk() throws Exception {
        List<String[]> nodes = asList(
                new String[]{SHARDING_BASE_DIR, null},
                new String[]{"/sharding/key1", null},
                new String[]{"/sharding/key1/grabs", null},
                new String[]{"/sharding/key1/grabs/n1", "true"}
        );

        if (testingServer != null) {
            testingServer.close();
            testingServer = null;
        }

        if (port == null) {
            testingServer = CreatorHelper.createZkServerWithNodes(nodes);
            port = testingServer.getPort();
        } else {
            testingServer = createZkServerWithNodes(port, nodes);
        }
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        startZk();
    }

    @Test
    public void test_ZkCrash_Fail() throws Exception {
        ShardingServiceImpl shardingService = createShardingService(testingServer.getConnectString(), SHARDING_BASE_DIR, "key1");
        shardingServiceList.add(shardingService);
        sleep(2000);

        final AtomicReference<ShardingInfo> holder = new AtomicReference<ShardingInfo>();
        final AtomicInteger counter = new AtomicInteger();
        ShardingListener listener = new ShardingListener() {
            @Override
            public void shardingChanged(ShardingInfo shardingInfo) {
                logger.info("receive sharing info: " + shardingInfo);
                counter.incrementAndGet();
                holder.set(shardingInfo);
            }
        };
        shardingService.addShardingListener(listener);

        sleep(2000);
        assertEquals(1, counter.get());
        assertNotNull(holder.get());

        testingServer.close();
        logger.info("=== stop zk server! ===");

        for (int i = 0; i < 8; ++i) {
            sleep(1000);
            logger.info("=== Wait Notify: " + i);
            if (counter.get() >= 2) {
                logger.info("=== Get Notify: " + i);
                break;
            }
        }

        assertEquals(2, counter.get());
        ShardingInfo info = holder.get();
        assertNull(info);

        startZk();
        logger.info("=== restart zk server! ===");

        for (int i = 0; i < 8; ++i) {
            sleep(1000);
            logger.info("=== Wait Notify: " + i);
            if (counter.get() >= 4) {
                logger.info("=== Get Notify: " + i);
                break;
            }
        }

        info = holder.get();
        assertNotNull(info);
        assertTrue(System.currentTimeMillis() - info.getCreateTime().getTime() < 1000 * 4);
        assertEquals("true", info.getRule());
        assertSame(TrueShardingRule.TRUE_SHARDING_RULE, info.getShardingRule());
    }
}
