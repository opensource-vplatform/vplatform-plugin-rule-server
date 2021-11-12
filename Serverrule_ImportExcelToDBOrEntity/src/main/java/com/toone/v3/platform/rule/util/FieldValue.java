package com.toone.v3.platform.rule.util;

import java.util.Map;
/**
 * 字段名和字段值
 * @author jiqj
 *
 */
class FieldValue implements Map.Entry<String, Object>{
	private final String key;
	private Object value;
	private final int column ,row;
	public FieldValue(String key,Object value,int row,int column) {
		this.key = key;
		this.value = value;
		this.row = row;
		this.column =column;
	}
	@Override
	public String getKey() { 
		return key;
	}

	@Override
	public Object getValue() { 
		return value;
	}

	@Override
	public Object setValue(Object val) {
		Object o = this.value;
		this.value = val;
		return o;
	}
	public int getColumn() {
		return column;
	}
	public int getRow() {
		return row;
	}
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("key:").append(key).append(",value:").append(value)
			.append(",row:").append(row).append(",column:").append(column);
		return sb.toString();
	}
}