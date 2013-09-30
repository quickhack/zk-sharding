package com.aliyun.ace4j.shared.coordinate.sharding.impl;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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
public class ShardingServiceImpl_Abnormal_DownZk_Test extends BaseShardingServiceImplTest {
    private static final Logger logger = LoggerFactory.getLogger(ShardingServiceImpl_Abnormal_DownZk_Test.class);

    public static void startZk(int zkPort) throws Exception {
        List<String[]> nodes = asList(
                new String[]{SHARDING_BASE_DIR, null},
                new String[]{"/sharding/key1", null},
                new String[]{"/sharding/key1/grabs", null},
                new String[]{"/sharding/key1/grabs/n1", "true"}
        );
        testingServer = createZkServerWithNodes(zkPort, nodes);
    }

    @Test
    public void test_ZkDown_recoverFromDownZk() throws Exception {
        ShardingServiceImpl shardingService = createShardingService("127.0.0.1:49876", SHARDING_BASE_DIR, "key1");
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
        assertNull(holder.get());

        startZk(49876);
        logger.info("=== start zk server! ===");

        for (int i = 0; i < 8; ++i) {
            sleep(1000);
            logger.info("=== Wait Notify: " + i);
            if (counter.get() >= 3) {
                logger.info("=== Get Notify: " + i);
                break;
            }
        }

        ShardingInfo info = holder.get();
        assertNotNull(info);
        assertTrue(System.currentTimeMillis() - info.getCreateTime().getTime() < 1000 * 4);
        assertEquals("true", info.getRule());
        assertSame(TrueShardingRule.TRUE_SHARDING_RULE, info.getShardingRule());
    }
}
