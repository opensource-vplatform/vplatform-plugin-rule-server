package com.toone.v3.platform.rule.util;

import org.apache.commons.collections.map.CaseInsensitiveMap;

import java.lang.reflect.InvocationTargetException;
import java.util.*;

/**
 * 获取对象属性工具
 * @deprecated 性能极慢
 * @author shenxiangz
 */
class ObjPropertyUtil {

    /**
     * 获取map中指定key的值（忽略大小写）
     *
     * @param map
     * @param key
     * @return
     */
    private static Object getMapValueByIgnoreCase(Map<?, ?> map, Object key) {
        if (map == null) {
            return null;
        }
        Map<?, ?> copy = new CaseInsensitiveMap(map);
        Object val = copy.get(key);
        // copy.clear();不需要这样的吧，垃圾回收器会处理的
        return val;
    }

    /* 获取对象的指定属性的值 */
    private static Object getValueByKey(Object object, String key, boolean ignoreCase) {
        if (ignoreCase) {// 不区分大小写
            Map<?, ?> map = describe(object);
            return map != null ? getMapValueByIgnoreCase(map, key) : null;
        }
        else {// 区分大小写
            return getProperty(object, key);
        }
    }

    /**
     * 获取对象列表的属性值列表(去掉重复值),忽略空值，shenxiangz,2010-08-11
     *
     * @param list
     *        对象列表
     * @param propertyName
     *        对象的属性名
     * @return
     */
    private static <T> List<T> getPropertyList(Collection<?> list, String propertyName) {
        return getPropertyList(list, propertyName, true);
    }

    /**
     * 获取对象列表的属性值列表(去掉重复值)
     *
     * @param list
     *        对象列表
     * @param propertyName
     *        对象的属性名
     * @param ignoreCase
     *        是否忽略属性名的大小写
     * @return
     */
    private static <T> List<T> getPropertyList(Collection<?> list, String propertyName,
                                              boolean ignoreCase) {
        return getPropertyList(list, propertyName, ignoreCase, true);
    }

    /* 获取对象列表的属性值列表,忽略空值(isIgnoreRepeatValue:是否去掉重复值) */
    private static <T> List<T> getPropertyList(Collection<?> list, String propertyName,
                                               boolean ignoreCase, boolean isIgnoreRepeatValue) {
        if (isEmpty(list) || isEmpty(propertyName)) {
            return new ArrayList<T>();
        }
        List<T> retList = new ArrayList<T>(list.size());

        propertyName = propertyName.trim();
        for (Object object : list) {
            if (object == null) {
                continue;
            }

            Object val = getValueByKey(object, propertyName, ignoreCase);
            if ((!(val instanceof String) && val != null)
                    || (val instanceof String && !isEmpty((String) val))) {
                if (isIgnoreRepeatValue && !retList.contains(val)) {
                    retList.add((T) val);
                }
                else {
                    retList.add((T) val);
                }
            }
        }
        return retList;
    }

    /**
     * 获取对象属性值 处理三种类型，Map、Properties、和一般的JavaBean
     *
     * @param attriname
     * @param object
     * @return
     * @throws Exception
     */
    private static Object getProperty(Object object, String attriname) {
        try {
            // 参数异常判断 lujx 2008-1-22
            if (object == null || isEmpty(attriname)) {
                return null;
            }

            // 返回值
            if (object instanceof Map<?, ?>) {// MAP类型对象
                Map<?, ?> map = (Map<?, ?>) object;
                return map.get(attriname);
            }
            else if (object instanceof Properties) {// Properties类型对象
                Properties prop = (Properties) object;
                return prop.getProperty(attriname);
            }
            else {// 其他JavaBean对象
                return org.apache.commons.beanutils.BeanUtils.getProperty(object,attriname);
            }
        }
        catch (Exception ex) {
            ex.printStackTrace();
            throw new RuntimeException("获取源对象属性值失败", ex);
        }
    }

    /**
     * 将bean对象转化成map
     *
     * @param obj
     * @return
     */
    private static Map<?, ?> describe(Object obj) {
        if (obj == null) {
            return null;
        }
        else if (obj instanceof Map<?, ?>) {
            return (Map<?, ?>) obj;
        }
        else {
            Map map = null;
            try {
                map = org.apache.commons.beanutils.BeanUtils.describe(obj);
            }
            catch (IllegalAccessException e) {
                e.printStackTrace();
            }
            catch (InvocationTargetException e) {
                e.printStackTrace();
            }
            catch (NoSuchMethodException e) {
                e.printStackTrace();
            }
            return map;
        }
    }

    private static boolean isEmpty(String str) {
        if(str == null || str.equals("")) {
            return true;
        }

        return false;
    }

    private static boolean isEmpty(Collection<?> coll) {
        if(coll == null || coll.isEmpty()) {
            return true;
        }

        return false;
    }
}
