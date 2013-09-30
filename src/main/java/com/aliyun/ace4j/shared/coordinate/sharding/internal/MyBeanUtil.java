package com.aliyun.ace4j.shared.coordinate.sharding.internal;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.beanutils.PropertyUtils;

/**
 * @author ding.lid
 */
class MyBeanUtil {

    private MyBeanUtil() {
    }

    static EnumConvert enumConvert = new EnumConvert();

    /**
     * 通过String来设置Bean的属性。目前的支持的属性的类型有：
     * <ol>
     * <li>基本类型，如int、boolean、 {@link Integer} 、{@link Long}等
     * <li>Java5的枚举类型
     * </ol>
     *
     * @throws NullPointerException 参数bean, name为 <code>null</code>。
     * @throws RuntimeException     设置属性失败！
     */
    public static void setProperty(Object bean, String name, String value) {
        if (null == bean) {
            throw new IllegalArgumentException("Argument bean is null!");
        }
        if (null == name) {
            throw new IllegalArgumentException("Argument name is null!");
        }

        try {
            Class<?> propertyType = PropertyUtils.getPropertyType(bean, name);

            Object convert = null;
            if (value != null) {
                if (Enum.class.isAssignableFrom(propertyType)) {
                    convert = enumConvert.convert(propertyType, value);
                } else {
                    convert = ConvertUtils.convert(value, propertyType);
                }
            }

            PropertyUtils.setSimpleProperty(bean, name, convert);
        } catch (Exception e) {
            throw new RuntimeException("Exception when set property(" + name + ") of bean(" + bean
                    + ")[bean class: " + bean.getClass().getName() + "] with value(" + value + "): " + e.toString(), e);
        }
    }

    /**
     * @throws NullPointerException 参数bean, name为 <code>null</code>。
     * @throws RuntimeException     设置属性失败！
     * @see #setProperty(Object, String, String)
     */
    public static void setProperty(Object bean, Map<String, String> properties) {
        Iterator<Entry<String, String>> iterator = properties.entrySet().iterator();
        for (; iterator.hasNext(); ) {
            Entry<String, String> next = iterator.next();
            setProperty(bean, next.getKey(), next.getValue());
        }
    }
}
