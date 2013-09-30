package com.aliyun.ace4j.shared.coordinate.sharding.impl;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author ding.lid
 */
public class ShardingServiceImpl_Util_Test {
    @Test
    public void test_splitToKeyList() throws Exception {
        assertEquals(Collections.<String>emptyList(), ShardingServiceImpl.splitToKeyList(","));
        assertEquals(Arrays.asList("a", "b"), ShardingServiceImpl.splitToKeyList("a,b"));
        assertEquals(Arrays.asList("a", "b"), ShardingServiceImpl.splitToKeyList(", a, b,"));
    }

    @Test
    public void test_splitToKeyList_error_containSlash() throws Exception {
        try {
            ShardingServiceImpl.splitToKeyList("a/b,key2");
            fail();
        } catch (IllegalStateException expected) {
            assertEquals("key a/b contains '/' in a/b,key2", expected.getMessage());
        }
    }

    @Test
    public void test_init_noKey() throws Exception {
        ShardingServiceImpl shardingService = new ShardingServiceImpl();

        shardingService.setKeys(null);
        assertTrue(shardingService.keyList.isEmpty());

        shardingService.setKeys(" , ");
        assertTrue(shardingService.keyList.isEmpty());
    }
}
