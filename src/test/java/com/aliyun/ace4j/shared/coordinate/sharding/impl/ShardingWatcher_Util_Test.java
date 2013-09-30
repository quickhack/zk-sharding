package com.aliyun.ace4j.shared.coordinate.sharding.impl;

import java.util.Date;

import org.junit.Test;

import com.aliyun.ace4j.shared.coordinate.sharding.ShardingInfo;
import com.aliyun.ace4j.shared.coordinate.sharding.support.FalseShardingRule;
import com.aliyun.ace4j.shared.coordinate.sharding.support.TrueShardingRule;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author ding.lid
 */
public class ShardingWatcher_Util_Test {
    @Test
    public void test_shardingInfoEquals() throws Exception {
        assertTrue(ShardingWatcher.shardingInfoEquals(null, null));
        assertFalse(ShardingWatcher.shardingInfoEquals(new ShardingInfo(null, null), null));
        assertFalse(ShardingWatcher.shardingInfoEquals(null, new ShardingInfo(null, null)));

        assertFalse(ShardingWatcher.shardingInfoEquals(new ShardingInfo(TrueShardingRule.TRUE_SHARDING_RULE, TrueShardingRule.TRUE_RULE),
                new ShardingInfo(FalseShardingRule.FALSE_SHARDING_RULE, FalseShardingRule.FALSE_RULE)));

        assertTrue(ShardingWatcher.shardingInfoEquals(new ShardingInfo(TrueShardingRule.TRUE_SHARDING_RULE, TrueShardingRule.TRUE_RULE),
                new ShardingInfo(new Date(System.currentTimeMillis() - 1000000), TrueShardingRule.TRUE_SHARDING_RULE, TrueShardingRule.TRUE_RULE, false)));
    }
}
