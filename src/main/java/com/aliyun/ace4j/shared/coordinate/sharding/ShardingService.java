package com.aliyun.ace4j.shared.coordinate.sharding;

/**
 * ShardingService
 *
 * @author ding.lid
 * @see ShardingInfo
 * @see ShardingListener
 */
public interface ShardingService {
    /**
     * 支持轮训方式。
     * <p/>
     * 返回值说明：
     * <ul>
     * <li>{@link ShardingInfo}参数为<code>null</code>表示没有获得Sharding信息（比如与Coordinator连接断开了），或是没有Sharding结点！
     * <li>参数的{@link ShardingInfo#getShardingRule()}为<code>null</code>表示没有抢到Sharding（Standby状态）。
     * </ul>
     * @throws IllegalStateException 没有配置过Sharding Key
     */
    ShardingInfo getShardingInfo();

    /**
     * 注册Sharding监听。
     * <p/>
     * 目前的实现，只允许注册一个Listener。
     *
     * @param listener ShardingListener
     * @throws IllegalStateException 这个Listener已经注册过了。
     */
    void addShardingListener(ShardingListener listener);

    /**
     * 手动动退出Sharding。
     *
     * @param listener 即{@link #addShardingListener(ShardingListener)}中给的Listener。
     */
    void removeShardingListener(ShardingListener listener);
}
