package com.aliyun.ace4j.shared.coordinate.sharding.support;

import com.aliyun.ace4j.shared.coordinate.sharding.ShardingRule;

/**
 * @author ding.lid
 */
public class TrueShardingRule implements ShardingRule {
    public static final TrueShardingRule TRUE_SHARDING_RULE = new TrueShardingRule();
    public static final String TRUE_RULE = "true";

    @Override
    public boolean inRange(Object input) {
        return true;
    }

    private TrueShardingRule() {
    }
}
