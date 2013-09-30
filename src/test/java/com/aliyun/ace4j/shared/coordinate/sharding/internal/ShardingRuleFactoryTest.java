package com.aliyun.ace4j.shared.coordinate.sharding.internal;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.aliyun.ace4j.shared.coordinate.sharding.ShardingRule;
import com.aliyun.ace4j.shared.coordinate.sharding.support.FalseShardingRule;
import com.aliyun.ace4j.shared.coordinate.sharding.support.TrueShardingRule;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * @author ding.lid
 */
public class ShardingRuleFactoryTest {
    static <T> Map<T, T> toMap(T... input) {
        if (input.length % 2 != 0)
            throw new IllegalStateException("odd count input!");
        Map<T, T> ret = new HashMap<T, T>();
        for (int i = 0; i < input.length; i += 2) {
            ret.put(input[i], input[i + 1]);
        }

        return ret;
    }

    @Test
    public void test_Kv2Map() throws Exception {
        Map<String, String> map = ShardingRuleFactory.kv2Map("a=1&b=2");
        assertEquals(toMap("a", "1", "b", "2"), map);
    }

    @Test
    public void test_Kv2Map_noValue() throws Exception {
        try {
            ShardingRuleFactory.kv2Map("a=1&b");
            fail();
        } catch (IllegalStateException expected) {
            assertThat(expected.getMessage(), startsWith("b is illegal"));
        }
    }

    @Test
    public void test_Kv2Map_noKeyOrValueContent() throws Exception {
        try {
            ShardingRuleFactory.kv2Map("a=1&b=");
            fail();
        } catch (IllegalStateException expected) {
            assertThat(expected.getMessage(), containsString("is illegal, value is blank"));
        }
        try {
            ShardingRuleFactory.kv2Map("a=1&=v2");
            fail();
        } catch (IllegalStateException expected) {
            assertThat(expected.getMessage(), containsString("is illegal, key or value is blank"));
        }
    }

    @Test
    public void test_ToShardingRule_true_blankOrRedundantKv() throws Exception {
        ShardingRule shardingRule = ShardingRuleFactory.toShardingRule("true");
        assertNotNull(shardingRule);
        assertSame(TrueShardingRule.TRUE_SHARDING_RULE, shardingRule);

        shardingRule = ShardingRuleFactory.toShardingRule(" TruE   ");
        assertNotNull(shardingRule);
        assertSame(TrueShardingRule.TRUE_SHARDING_RULE, shardingRule);

        shardingRule = ShardingRuleFactory.toShardingRule("true:k1=v1");
        assertNotNull(shardingRule);
        assertSame(TrueShardingRule.TRUE_SHARDING_RULE, shardingRule);
    }

    @Test
    public void test_ToShardingRule_false_blankOrRedundantKv() throws Exception {
        ShardingRule shardingRule = ShardingRuleFactory.toShardingRule(
                "false");
        assertNotNull(shardingRule);
        assertSame(FalseShardingRule.FALSE_SHARDING_RULE, shardingRule);

        shardingRule = ShardingRuleFactory.toShardingRule(" FaLse   ");
        assertNotNull(shardingRule);
        assertSame(FalseShardingRule.FALSE_SHARDING_RULE, shardingRule);

        shardingRule = ShardingRuleFactory.toShardingRule("false:k1=v1");
        assertNotNull(shardingRule);
        assertSame(FalseShardingRule.FALSE_SHARDING_RULE, shardingRule);
    }

    @Test
    public void test_ToShardingRule_noClassKey() throws Exception {
        final String rule = "strategy:modSize=3&modResult=0";
        try {
            ShardingRuleFactory.toShardingRule(rule);
            fail();
        } catch (Exception expected) {
            assertThat(expected.getMessage(), containsString("no class key in rule: " + rule));
        }
    }

    @Test
    public void test_ToShardingRule_UnsupportedProtocol() throws Exception {
        try {
            ShardingRuleFactory.toShardingRule("foo:a=b&modSize=3&modResult=0");
            fail();
        } catch (Exception expected) {
            assertThat(expected.getMessage(), containsString("Unsupported protocol(foo)"));
        }
    }
}
