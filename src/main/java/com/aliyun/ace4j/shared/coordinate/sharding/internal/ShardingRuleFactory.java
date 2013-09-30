package com.aliyun.ace4j.shared.coordinate.sharding.internal;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.ace4j.shared.coordinate.sharding.ShardingRule;
import com.aliyun.ace4j.shared.coordinate.sharding.support.FalseShardingRule;
import com.aliyun.ace4j.shared.coordinate.sharding.support.TrueShardingRule;

/**
 * @author ding.lid
 */
public class ShardingRuleFactory {
    private static final Logger logger = LoggerFactory.getLogger(ShardingRuleFactory.class);

    public static final String STRATEGY = "strategy";
    public static final String TRUE = "true";
    public static final String FALSE = "false";

    private static final Pattern PARAM_SEP = Pattern.compile("\\s*&\\s*");
    private static final Pattern KV_SEP = Pattern.compile("\\s*=\\s*");

    static Map<String, String> kv2Map(String parameters) {
        parameters = parameters.trim();

        Map<String, String> ret = new HashMap<String, String>();
        String[] kvs = PARAM_SEP.split(parameters);
        for (String kv : kvs) {
            if (kv == null || kv.length() == 0) continue;
            String[] split = KV_SEP.split(kv);
            if (split.length != 2) {
                throw new IllegalStateException(kv + " is illegal, value is blank");
            }
            if (split[0].length() == 0 || split[1].length() == 0) {
                throw new IllegalStateException(kv + " is illegal, key or value is blank.");
            }
            ret.put(split[0], split[1]);
        }

        return ret;
    }

    /**
     * 把rule字符串转成{@link ShardingRule}对象。
     * 目前Sharding类型：
     * <ul>
     *     <li>strategy</li>
     * </ul>
     *
     * @param rule rule字符串说明。形如：<code>strategy:class=com.aliyun.ModShardingRule&modeSize=3&modResult=0</code>
     * @return {@link ShardingRule}对象
     */
    public static ShardingRule toShardingRule(String rule) {
        try {
            rule = rule.trim();
            final int idx = rule.indexOf(':');
            final String protocol;
            final String body;
            if (idx > 0) {
                protocol = rule.substring(0, idx).trim();
                body = rule.substring(idx + 1).trim();
            } else {
                protocol = rule;
                body = null;
            }

            if (STRATEGY.equalsIgnoreCase(protocol)) {
                Map<String, String> kv = kv2Map(body);
                if (!kv.containsKey("class")) {
                    throw new IllegalStateException("no class key in rule: " + rule);
                }
                String clazz = kv.remove("class");
                Class<? extends ShardingRule> ruleClass = Class.forName(clazz).asSubclass(ShardingRule.class);
                ShardingRule shardingRule = ruleClass.newInstance();
                MyBeanUtil.setProperty(shardingRule, kv);
                return shardingRule;
            } else if (TRUE.equalsIgnoreCase(protocol)) {
                if (StringUtils.isNotBlank(body)) {
                    logger.warn("body of rule(" + rule +
                            ") is ignored, TrueShardingRule do not need rule body!");
                }
                return TrueShardingRule.TRUE_SHARDING_RULE;
            } else if (FALSE.equalsIgnoreCase(protocol)) {
                if (StringUtils.isNotBlank(body)) {
                    logger.warn("body of rule(" + rule +
                            ") is ignored, FalseShardingRule do not need rule body!");
                }
                return FalseShardingRule.FALSE_SHARDING_RULE;
            } else {
                throw new IllegalStateException("Unsupported protocol(" + protocol +
                        ") in rule(" + rule + ")");
            }
        } catch (Exception e) {
            throw new IllegalStateException("Fail to create sharding rule(" + rule +
                    "), cause: " + e.getMessage(), e);
        }
    }
}
