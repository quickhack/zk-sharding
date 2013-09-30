package com.aliyun.ace4j.shared.coordinate.sharding.support;

/**
 * 目前支持{@link String}和{@link Number}类型的输入。
 * 
 * @author ding.lid
 */
public class GeneralInputModShardingRule extends AbstractModShardingRule {
    @Override
    protected int getModValue(Object input) {
        if (input == null) {
            throw new IllegalArgumentException("sharding input == null!");
        }
        int hash;
        if (input instanceof String) {
            hash = input.hashCode();
        } else if (input instanceof Number) {
            int value = ((Number) input).intValue();
            hash = value * value * 31;
        } else {
            throw new IllegalStateException(GeneralInputModShardingRule.class.getName() +
                    " not support sharding input type " + input.getClass().getName());
        }

        return hash;
    }
}
