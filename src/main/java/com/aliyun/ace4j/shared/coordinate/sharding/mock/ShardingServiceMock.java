package com.aliyun.ace4j.shared.coordinate.sharding.mock;

import com.aliyun.ace4j.shared.coordinate.sharding.ShardingInfo;
import com.aliyun.ace4j.shared.coordinate.sharding.ShardingListener;
import com.aliyun.ace4j.shared.coordinate.sharding.ShardingService;
import com.aliyun.ace4j.shared.coordinate.sharding.support.TrueShardingRule;

/**
 * @author ding.lid
 */
public class ShardingServiceMock implements ShardingService {
    public void setKeys(String keys) {
    }

    public void setShardingBaseDir(String shardingBaseDir) {
    }

    public void setZkAddress(String zkAddress) {
    }

    final ShardingInfo shardingInfo;

    public ShardingServiceMock() {
        this.shardingInfo = new ShardingInfo(TrueShardingRule.TRUE_SHARDING_RULE, "true");
    }

    public void init() {
    }

    public void destroy() {
    }

    @Override
    public ShardingInfo getShardingInfo() {
        return shardingInfo;
    }

    @Override
    public void addShardingListener(ShardingListener listener) {
        listener.shardingChanged(shardingInfo);
    }

    @Override
    public void removeShardingListener(ShardingListener listener) {
    }
}
