package com.aliyun.ace4j.shared.coordinate.sharding.support;

/**
 * @author ding.lid
 */
public class NumberInputModShardingRule extends AbstractModShardingRule {
    @Override
    protected int getModValue(Object input) {
        if (!(input instanceof Number)) {
            throw new IllegalStateException("sharding input is not Number!");
        }
        Number num = (Number) input;
        int value = num.intValue();
        return value * value * 31;
    }
}
