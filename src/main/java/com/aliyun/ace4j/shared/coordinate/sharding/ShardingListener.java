package com.aliyun.ace4j.shared.coordinate.sharding;

/**
 * ShardingListener
 *
 * @author ding.lid
 * @see ShardingInfo
 */
public interface ShardingListener {
    /**
     * 通知shardingInfo。
     * <p/>
     * <ul>
     * <li>{@link ShardingInfo}参数为<code>null</code>表示没有获得Sharding信息（比如与Coordinator连接断开了），或是没有Sharding结点！
     * <li>参数的{@link ShardingInfo#getShardingRule()}为<code>null</code>表示没有抢到Sharding（Standby状态）。
     * </ul>
     * <p/>
     * <font color="red">注意</font>：<B>不要</B>在方法执行任何长时间的操作。
     *
     * @param shardingInfo 通知的Sharding信息。
     */
    void shardingChanged(ShardingInfo shardingInfo);
}
