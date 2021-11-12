package com.toone.v3.platform.rule.util;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.yindangu.v3.business.plugin.execptions.ConfigException;
import com.yindangu.v3.platform.plugin.util.VdsUtils;

public class ObjPropertyUtil2 {
	/**
	 * 取属性值
	 * @param list
	 * @param propertyName 属性名（忽略大小写）
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <T> List<T> getPropertyList(Collection<?> list, String propertyName) {
        Object obj = null;
        Method method = null;
        try {
            Object[] args = {};
            int size = (list == null ? 0 : list.size());
            if(size ==0) {
            	return Collections.emptyList();
            }
	        List<T> rs = new ArrayList<>(size);
	        
	        for(Object o : list) {
	        	if(o!=null) {
	        		obj =o;
		        	if(method == null) {
		        		method = findMethod(o, propertyName);
		        	} 
					T t =(T) method.invoke(o, args);
					if(t!=null) {
						rs.add(t);
					}
	        	}
	        }
        	return rs;
        }
        catch (Throwable e) {
			throw new ConfigException("读取属性[" + obj + "](class=" +
					(obj == null ? "null" : obj.getClass()) + ",method=" + 
					(method == null ? "null" : method.getName()) + ")出错");
		}
    }
	private static Method findMethod(Object o,String propertyName) {
		if(VdsUtils.string.isEmpty(propertyName)) {
			throw new ConfigException("属性名不能为空");
		}		
		Method[] mds = o.getClass().getMethods();
		Method rs = null;
		for(int i =0 ,size = mds.length ;i < size;i++) {
			String menthName = mds[i].getName();
			//判断get，或者is
			int startIdx = (menthName.startsWith("get") ?3 : (menthName.startsWith("is") ? 2 :-1));
			if(startIdx >0 && mds[i].getParameterCount() ==0 && 
					propertyName.equalsIgnoreCase(menthName.substring(startIdx))) {
				rs = mds[i];
				break;
			}
		}
		return rs;
	}
}
