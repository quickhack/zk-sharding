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
import static com.aliyun.ace4j.shared.coordinate.sharding.impl.CreatorHelper.addNodes;
import static com.aliyun.ace4j.shared.coordinate.sharding.impl.CreatorHelper.createShardingService;
import static com.aliyun.ace4j.shared.coordinate.sharding.impl.CreatorHelper.createZkServerWithNodes;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * @author ding.lid
 */
public class ShardingServiceImpl_Abnormal_NoRangeNode_Test extends BaseShardingServiceImplTest {
    private static final Logger logger = LoggerFactory.getLogger(ShardingServiceImpl_Abnormal_NoRangeNode_Test.class);

    @BeforeClass
    public static void beforeClass() throws Exception {
        List<String[]> nodes = asList(
                new String[]{"/sharding", null},
                new String[]{"/sharding/key1", null},
                new String[]{"/sharding/key1/grabs", null}
        );
        testingServer = createZkServerWithNodes(nodes);
    }

    @Test
    public void test_NoRangeNode() throws Exception {
        ShardingServiceImpl shardingService = createShardingService(testingServer.getConnectString(), SHARDING_BASE_DIR, "key1");
        shardingServiceList.add(shardingService);

        final AtomicReference<ShardingInfo> holder = new AtomicReference<ShardingInfo>();
        final AtomicInteger counter = new AtomicInteger();
        ShardingListener listener = new ShardingListener() {
            @Override
            public void shardingChanged(ShardingInfo shardingInfo) {
                counter.incrementAndGet();
                holder.set(shardingInfo);
            }
        };
        shardingService.addShardingListener(listener);

        Thread.sleep(2000);

        assertEquals(1, counter.get());
        ShardingInfo info = holder.get();
        assertNotNull(info);
        assertNull(info.getRule());
        assertNull(info.getShardingRule());

        // 添加RangeNode
        logger.warn("add range node!");
        List<String[]> nodes = asList(
                new String[][]{
                        new String[]{"/sharding/key1/grabs/n1", "true"}
                }
        );
        addNodes(testingServer.getConnectString(), nodes);

        Thread.sleep(2000);

        assertEquals(2, counter.get());
        info = holder.get();
        assertNotNull(info);
        assertTrue(System.currentTimeMillis() - info.getCreateTime().getTime() < 1000 * 4);
        assertEquals("true", info.getRule());
        assertSame(TrueShardingRule.TRUE_SHARDING_RULE, info.getShardingRule());
    }
}
