package com.aliyun.ace4j.shared.coordinate.sharding;

import java.util.Date;

/**
 * Sharding信息，值对象（不修改，线程安全）。
 * 
 * @author ding.lid
 * @see ShardingRule
 */
public class ShardingInfo {
    private final Date createTime;
    private final ShardingRule shardingRule;
    private final String rule;
    private final boolean disabled;

    /**
     * {@link ShardingInfo}的更新时间。
     */
    public Date getCreateTime() {
        return createTime;
    }

    public ShardingRule getShardingRule() {
        return shardingRule;
    }

    public String getRule() {
        return rule;
    }

    public boolean isDisabled() {
        return disabled;
    }

    public ShardingInfo(Date createTime, ShardingRule shardingRule, String rule, boolean disabled) {
        this.createTime = createTime;
        this.shardingRule = shardingRule;
        this.rule = rule;
        this.disabled = disabled;
    }


    public ShardingInfo(ShardingRule shardingRule, String rule) {
        this.createTime = new Date();
        this.shardingRule = shardingRule;
        this.rule = rule;
        this.disabled = false;
    }
}
