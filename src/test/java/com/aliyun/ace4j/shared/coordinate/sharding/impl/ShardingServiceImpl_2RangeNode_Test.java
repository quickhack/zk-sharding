package com.aliyun.ace4j.shared.coordinate.sharding.impl;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.ace4j.shared.coordinate.sharding.ShardingInfo;
import com.aliyun.ace4j.shared.coordinate.sharding.ShardingListener;
import com.aliyun.ace4j.shared.coordinate.sharding.support.NumberInputModShardingRule;

import static com.aliyun.ace4j.shared.coordinate.sharding.impl.CreatorHelper.SHARDING_BASE_DIR;
import static com.aliyun.ace4j.shared.coordinate.sharding.impl.CreatorHelper.createShardingService;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * @author ding.lid
 */
public class ShardingServiceImpl_2RangeNode_Test extends BaseShardingServiceImplTest {
    private static final Logger logger = LoggerFactory.getLogger(ShardingServiceImpl_2RangeNode_Test.class);


    static final String RULE0 = "strategy:class=com.aliyun.ace4j.shared.coordinate.sharding.support.NumberInputModShardingRule&modSize=2&modResult=0";
    static final String RULE1 = "strategy:class=com.aliyun.ace4j.shared.coordinate.sharding.support.NumberInputModShardingRule&modSize=2&modResult=1";

    @BeforeClass
    public static void beforeClass() throws Exception {
        List<String[]> nodes = asList(
                new String[]{"/sharding", null},
                new String[]{"/sharding/key1", null},
                new String[]{"/sharding/key1/grabs", null},
                new String[]{"/sharding/key1/grabs/n0", RULE0},
                new String[]{"/sharding/key1/grabs/n1", RULE1}
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
        assertThat(info.getRule(), anyOf(equalTo(RULE0), equalTo(RULE1)));

        NumberInputModShardingRule shardingRule = (NumberInputModShardingRule) info.getShardingRule();
        assertEquals(2, shardingRule.getModSize());
        assertThat(((NumberInputModShardingRule) info.getShardingRule()).getModResult(), anyOf(equalTo(0), equalTo(1)));
    }

    @Test
    public void test_2client_2success() throws Exception {
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
            assertThat(info.getRule(), anyOf(equalTo(RULE0), equalTo(RULE1)));

            NumberInputModShardingRule shardingRule = (NumberInputModShardingRule) info.getShardingRule();
            assertEquals(2, shardingRule.getModSize());
            assertThat(((NumberInputModShardingRule) info.getShardingRule()).getModResult(), anyOf(equalTo(0), equalTo(1)));
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
            assertThat(info.getRule(), anyOf(equalTo(RULE0), equalTo(RULE1)));

            NumberInputModShardingRule shardingRule = (NumberInputModShardingRule) info.getShardingRule();
            assertEquals(2, shardingRule.getModSize());
            assertThat(((NumberInputModShardingRule) info.getShardingRule()).getModResult(), anyOf(equalTo(0), equalTo(1)));
        }
    }

    @Test
    public void test_3client_2success1Fail() throws Exception {
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
            assertThat(info.getRule(), anyOf(equalTo(RULE0), equalTo(RULE1)));

            NumberInputModShardingRule shardingRule = (NumberInputModShardingRule) info.getShardingRule();
            assertEquals(2, shardingRule.getModSize());
            assertThat(((NumberInputModShardingRule) info.getShardingRule()).getModResult(), anyOf(equalTo(0), equalTo(1)));
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
            assertThat(info.getRule(), anyOf(equalTo(RULE0), equalTo(RULE1)));

            NumberInputModShardingRule shardingRule = (NumberInputModShardingRule) info.getShardingRule();
            assertEquals(2, shardingRule.getModSize());
            assertThat(((NumberInputModShardingRule) info.getShardingRule()).getModResult(), anyOf(equalTo(0), equalTo(1)));
        }

        ShardingServiceImpl shardingService3;
        {
            shardingService3 = createShardingService(testingServer.getConnectString(), SHARDING_BASE_DIR, "key1");
            shardingServiceList.add(shardingService3);

            final AtomicReference<ShardingInfo> holder = new AtomicReference<ShardingInfo>();
            ShardingListener listener = new ShardingListener() {
                @Override
                public void shardingChanged(ShardingInfo shardingInfo) {
                    holder.set(shardingInfo);
                }
            };
            shardingService3.addShardingListener(listener);

            Thread.sleep(2000);

            ShardingInfo info = holder.get();
            assertNotNull(info);
            assertTrue(System.currentTimeMillis() - info.getCreateTime().getTime() < 1000 * 4);
            assertNull(info.getRule());
            assertNull(info.getShardingRule());
        }
    }


    @Test
    public void test_3client_1leave1in() throws Exception {
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
        }

        Thread.sleep(2000);

        ShardingServiceImpl shardingService3;
        {
            shardingService3 = createShardingService(testingServer.getConnectString(), SHARDING_BASE_DIR, "key1");
            shardingServiceList.add(shardingService3);

            final AtomicReference<ShardingInfo> holder = new AtomicReference<ShardingInfo>();
            ShardingListener listener = new ShardingListener() {
                @Override
                public void shardingChanged(ShardingInfo shardingInfo) {
                    holder.set(shardingInfo);
                }
            };
            shardingService3.addShardingListener(listener);

            Thread.sleep(2000);

            shardingService2.destroy();

            Thread.sleep(2000);

            ShardingInfo info = holder.get();
            assertNotNull(info);
            assertTrue(System.currentTimeMillis() - info.getCreateTime().getTime() < 1000 * 4);
            assertThat(info.getRule(), anyOf(equalTo(RULE0), equalTo(RULE1)));

            NumberInputModShardingRule shardingRule = (NumberInputModShardingRule) info.getShardingRule();
            assertEquals(2, shardingRule.getModSize());
            assertThat(((NumberInputModShardingRule) info.getShardingRule()).getModResult(), anyOf(equalTo(0), equalTo(1)));
        }
    }
}
