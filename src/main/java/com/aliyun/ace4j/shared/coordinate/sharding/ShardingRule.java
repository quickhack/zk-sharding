package com.aliyun.ace4j.shared.coordinate.sharding;

/**
 * Sharding规则。
 *
 * @author ding.lid
 * @see ShardingInfo
 */
public interface ShardingRule {
    /**
     * 被Shard的对象是否属于此Rule。
     *
     * @param input 用于Sharding依据的输入对象。
     */
    boolean inRange(Object input);
}
