package com.aliyun.ace4j.shared.coordinate.sharding.support;

import com.aliyun.ace4j.shared.coordinate.sharding.ShardingInfo;
import com.aliyun.ace4j.shared.coordinate.sharding.ShardingRule;

/**
 * @author ding.lid
 */
public class ShardingUtils {
    /**
     * 根据ShardingInfo，确定是否要处理Sharding对象（Sharding Input）。
     *
     * @return 是否要处理Sharding对象（Sharding Input）。
     */
    public static boolean isShardToThis(ShardingInfo shardingInfo, Object shardingInput) {
        if (null == shardingInfo) return false; // Sharding服务失效，则暂停操作。

        if (shardingInfo.isDisabled()) return false;

        ShardingRule shardingRule = shardingInfo.getShardingRule();
        if (null == shardingRule) {
            return false; // 没有抢到RangeNode，则不Sharding（Standby状态）
        }

        return shardingRule.inRange(shardingInput);
    }

    private ShardingUtils() {
    }
}
