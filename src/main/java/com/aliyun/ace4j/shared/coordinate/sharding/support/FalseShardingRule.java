package com.aliyun.ace4j.shared.coordinate.sharding.support;

import com.aliyun.ace4j.shared.coordinate.sharding.ShardingRule;

/**
 * @author ding.lid
 */
public class FalseShardingRule implements ShardingRule {
    public static final FalseShardingRule FALSE_SHARDING_RULE = new FalseShardingRule();
    public static final String FALSE_RULE = "false";

    private FalseShardingRule() {
    }

    @Override
    public boolean inRange(Object input) {
        return false;
    }
}
