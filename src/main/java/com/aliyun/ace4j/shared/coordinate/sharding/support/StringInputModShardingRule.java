package com.aliyun.ace4j.shared.coordinate.sharding.support;

/**
 * @author ding.lid
 */
public class StringInputModShardingRule extends AbstractModShardingRule {
    @Override
    protected int getModValue(Object input) {
        if (!(input instanceof String)) {
            throw new IllegalStateException("sharding input is not String!");
        }
        String bs = (String) input;

        return bs.hashCode();
    }
}
