package com.aliyun.ace4j.shared.coordinate.sharding.internal;

import java.util.Date;

import org.junit.Test;

import com.aliyun.ace4j.shared.coordinate.sharding.ShardingRule;
import com.aliyun.ace4j.shared.coordinate.sharding.support.GeneralInputModShardingRule;
import com.aliyun.ace4j.shared.coordinate.sharding.support.NumberInputModShardingRule;
import com.aliyun.ace4j.shared.coordinate.sharding.support.StringInputModShardingRule;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author ding.lid
 */
public class ShardingRuleFactory_RuleLogic_Test {
    @Test
    public void test_ToShardingRule_true() throws Exception {
        ShardingRule shardingRule = ShardingRuleFactory.toShardingRule("true");
        assertTrue(shardingRule.inRange("xxx"));
        assertTrue(shardingRule.inRange(new Date()));
    }

    @Test
    public void test_ToShardingRule_false() throws Exception {
        ShardingRule shardingRule = ShardingRuleFactory.toShardingRule("false");
        assertFalse(shardingRule.inRange("xxx"));
        assertFalse(shardingRule.inRange(new Date()));
    }

    @Test
    public void test_ToShardingRule_StringInputModShardingRule() throws Exception {
        ShardingRule shardingRule = ShardingRuleFactory.toShardingRule(
                "strategy:class=com.aliyun.ace4j.shared.coordinate.sharding.support.StringInputModShardingRule" +
                        "&modSize=3&modResult=0");

        assertThat(shardingRule, instanceOf(StringInputModShardingRule.class));
        StringInputModShardingRule stringInputModShardingRule = (StringInputModShardingRule) shardingRule;
        assertEquals(3, stringInputModShardingRule.getModSize());
        assertEquals(0, stringInputModShardingRule.getModResult());

        shardingRule.inRange("foo1");
        shardingRule.inRange("foo2");
        shardingRule.inRange("foo3");

        try {
            shardingRule.inRange(new Date());
            fail();
        } catch (Exception expected) {
            assertThat(expected.getMessage(), containsString("sharding input is not String"));
        }
    }

    @Test
    public void test_ToShardingRule_NumberInputModShardingRule() throws Exception {
        ShardingRule shardingRule = ShardingRuleFactory.toShardingRule(
                "strategy:class=com.aliyun.ace4j.shared.coordinate.sharding.support.NumberInputModShardingRule" +
                        "&modSize=3&modResult=0");

        assertThat(shardingRule, instanceOf(NumberInputModShardingRule.class));
        NumberInputModShardingRule rule = (NumberInputModShardingRule) shardingRule;
        assertEquals(3, rule.getModSize());
        assertEquals(0, rule.getModResult());

        shardingRule.inRange(1);
        shardingRule.inRange(1024L);

        try {
            shardingRule.inRange(new Date());
            fail();
        } catch (Exception expected) {
            assertThat(expected.getMessage(), containsString("sharding input is not Number"));
        }
    }

    @Test
    public void test_ToShardingRule_GeneralInputModShardingRule() throws Exception {
        ShardingRule shardingRule = ShardingRuleFactory.toShardingRule(
                "strategy:class=com.aliyun.ace4j.shared.coordinate.sharding.support.GeneralInputModShardingRule" +
                        "&modSize=3&modResult=0");

        assertThat(shardingRule, instanceOf(GeneralInputModShardingRule.class));
        GeneralInputModShardingRule generalInputModShardingRule = (GeneralInputModShardingRule) shardingRule;
        assertEquals(3, generalInputModShardingRule.getModSize());
        assertEquals(0, generalInputModShardingRule.getModResult());

        shardingRule.inRange("foo1");
        shardingRule.inRange("foo2");
        shardingRule.inRange("foo3");

        shardingRule.inRange(1);
        shardingRule.inRange(1024L);

        try {
            shardingRule.inRange(new Date());
            fail();
        } catch (Exception expected) {
            assertThat(expected.getMessage(), containsString("not support sharding input type java.util.Date"));
        }
    }

    @Test
    public void test_ToShardingRule_color() throws Exception {
        ShardingRule shardingRule = ShardingRuleFactory.toShardingRule(
                "strategy:class=com.aliyun.ace4j.shared.coordinate.sharding.internal.ColorShardingRule" +
                        "&color=Blue&count=1122&name=foo34");

        ColorShardingRule colorShardingRule = (ColorShardingRule) shardingRule;
        assertEquals(Color.Blue, colorShardingRule.getColor());
        assertEquals(1122, colorShardingRule.getCount());
        assertEquals("foo34", colorShardingRule.getName());
    }
}
