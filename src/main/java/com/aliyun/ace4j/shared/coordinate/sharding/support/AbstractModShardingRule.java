package com.aliyun.ace4j.shared.coordinate.sharding.support;

import com.aliyun.ace4j.shared.coordinate.sharding.ShardingRule;

/**
 * @author ding.lid
 */
public abstract class AbstractModShardingRule implements ShardingRule {
    int modSize;

    int modResult;

    public int getModSize() {
        return modSize;
    }

    public void setModSize(int modSize) {
        this.modSize = modSize;
    }

    public int getModResult() {
        return modResult;
    }

    public void setModResult(int modResult) {
        this.modResult = modResult;
    }

    @Override
    public boolean inRange(Object input) {
        return Math.abs(getModValue(input)) % modSize == modResult;
    }

    protected abstract int getModValue(Object beSharded);
}
