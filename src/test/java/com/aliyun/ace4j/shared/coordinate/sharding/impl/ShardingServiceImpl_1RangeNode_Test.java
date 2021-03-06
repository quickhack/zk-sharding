package com.aliyun.ace4j.shared.coordinate.sharding.impl;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.ace4j.shared.coordinate.sharding.ShardingInfo;
import com.aliyun.ace4j.shared.coordinate.sharding.ShardingListener;
import com.aliyun.ace4j.shared.coordinate.sharding.support.TrueShardingRule;

import static com.aliyun.ace4j.shared.coordinate.sharding.impl.CreatorHelper.SHARDING_BASE_DIR;
import static com.aliyun.ace4j.shared.coordinate.sharding.impl.CreatorHelper.createShardingService;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * @author ding.lid
 */
public class ShardingServiceImpl_1RangeNode_Test extends BaseShardingServiceImplTest {
    @BeforeClass
    public static void beforeClass() throws Exception {
        List<String[]> nodes = asList(
                new String[]{SHARDING_BASE_DIR, null},
                new String[]{"/sharding/key1", null},
                new String[]{"/sharding/key1/grabs", null},
                new String[]{"/sharding/key1/grabs/n1", "true"}
        );
        testingServer = CreatorHelper.createZkServerWithNodes(nodes);
    }

    @Test
    public void test_1Client_success() throws Exception {
        ShardingServiceImpl shardingService = createShardingService(testingServer.getConnectString(), SHARDING_BASE_DIR, "key1");
        shardingServiceList.add(shardingService);

        final AtomicReference<ShardingInfo> holder = new AtomicReference<ShardingInfo>();
        ShardingListener listener = new ShardingListener() {
            @Override
            public void shardingChanged(ShardingInfo shardingInfo) {
                holder.set(shardingInfo);
            }
        };
        shardingService.addShardingListener(listener);

        Thread.sleep(2000);

        ShardingInfo info = holder.get();
        assertNotNull(info);
        assertTrue(System.currentTimeMillis() - info.getCreateTime().getTime() < 1000 * 4);
        assertEquals("true", info.getRule());
        assertSame(TrueShardingRule.TRUE_SHARDING_RULE, info.getShardingRule());
    }

    @Test
    public void test_2client_1success() throws Exception {
        ShardingServiceImpl shardingService1;
        {
            shardingService1 = createShardingService(testingServer.getConnectString(), SHARDING_BASE_DIR, "key1");
            shardingServiceList.add(shardingService1);

            final AtomicReference<ShardingInfo> holder = new AtomicReference<ShardingInfo>();
            ShardingListener listener = new ShardingListener() {
                @Override
                public void shardingChanged(ShardingInfo shardingInfo) {
                    holder.set(shardingInfo);
                }
            };
            shardingService1.addShardingListener(listener);

            Thread.sleep(2000);

            ShardingInfo info = holder.get();
            assertNotNull(info);
            assertTrue(System.currentTimeMillis() - info.getCreateTime().getTime() < 1000 * 4);
            assertEquals("true", info.getRule());
            assertSame(TrueShardingRule.TRUE_SHARDING_RULE, info.getShardingRule());
        }

        ShardingServiceImpl shardingService2;
        {
            shardingService2 = createShardingService(testingServer.getConnectString(), SHARDING_BASE_DIR, "key1");
            shardingServiceList.add(shardingService2);

            final AtomicReference<ShardingInfo> holder = new AtomicReference<ShardingInfo>();
            ShardingListener listener = new ShardingListener() {
                @Override
                public void shardingChanged(ShardingInfo shardingInfo) {
                    holder.set(shardingInfo);
                }
            };
            shardingService2.addShardingListener(listener);

            Thread.sleep(2000);

            ShardingInfo info = holder.get();
            assertNotNull(info);
            assertTrue(System.currentTimeMillis() - info.getCreateTime().getTime() < 1000 * 4);
            assertNull(info.getRule());
            assertNull(info.getShardingRule());
        }
    }
}
