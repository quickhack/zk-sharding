package com.aliyun.ace4j.shared.coordinate.sharding.internal;

import org.apache.commons.beanutils.Converter;

/**
 * @author ding.lid
 */
class EnumConvert implements Converter {

    @SuppressWarnings({"unchecked", "rawtypes"})
    public Object convert(Class type, Object value) {
        if (null == value) {
            return null;
        }

        if (!(value instanceof String)) {
            throw new IllegalArgumentException("value must be a String!");
        }

        String enumName = (String) value;
        enumName = enumName.trim();

        return Enum.valueOf(type, enumName);
    }
}
