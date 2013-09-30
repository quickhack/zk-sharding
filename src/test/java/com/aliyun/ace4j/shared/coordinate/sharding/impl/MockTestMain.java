package com.aliyun.ace4j.shared.coordinate.sharding.impl;

import com.aliyun.ace4j.shared.coordinate.sharding.ShardingInfo;

import static com.aliyun.ace4j.shared.coordinate.sharding.impl.CreatorHelper.createShardingService;
import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.sleep;

/**
 * @author ding.lid
 */
public class MockTestMain {
    public static void main(String[] args) throws Exception {
        ShardingServiceImpl scheduler = null;
        try {
            scheduler = createShardingService("10.12.49.17:2181", "/sharding", "scheduler");

            long tick = currentTimeMillis();
            while (scheduler.getShardingInfo() == null || scheduler.getShardingInfo().getRule() == null) {
                sleep(10);
            }

            System.err.println("Get ShardingInfo use " + (currentTimeMillis() - tick));
            ShardingInfo shardingInfo = scheduler.getShardingInfo();
            System.err.println(shardingInfo.getRule());
        } finally {
            if (scheduler != null)
                scheduler.destroy();
        }
    }
}
