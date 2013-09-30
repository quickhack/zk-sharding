package com.aliyun.ace4j.shared.coordinate.sharding.internal;

import com.aliyun.ace4j.shared.coordinate.sharding.ShardingRule;

/**
 * @author ding.lid
 */
public class ColorShardingRule implements ShardingRule {
    Color color;

    long count;

    String name;

    public Color getColor() {
        return color;
    }

    public void setColor(Color color) {
        this.color = color;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public boolean inRange(Object input) {
        return false;
    }
}
