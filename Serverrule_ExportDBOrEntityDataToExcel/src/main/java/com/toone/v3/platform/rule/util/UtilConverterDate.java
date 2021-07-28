package com.toone.v3.platform.rule.util;

import org.apache.commons.beanutils.Converter;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * java.util.Date转换器 实现Converter 接口convert方法
 * */
public class UtilConverterDate implements Converter {

    @SuppressWarnings("rawtypes")
    public Object convert(Class type, Object value) {

        if (value == null)// 如果为空,返回
        {
            return value;
        }
        if (value instanceof Date)// 如果类型是java.uitl.Date类型,返回
        {
            return value;
        }
        if (value instanceof String)// 如果类型是String,开始转换成java.util.Date,再返回
        { // 也就是把一个字符串转换成一个日期
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd",
                    Locale.getDefault());// 格式化日期的类
            try {
                return simpleDateFormat.parse((String) value);// 将String转换为java.util.Date类型
            }
            catch (ParseException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

}
