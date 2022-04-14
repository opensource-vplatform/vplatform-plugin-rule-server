package com.toone.v3.platform.rule.model;

import java.util.Map;

public class RequestModel {

	private boolean isSuccess;
	private int statusCode;
	private String msg;
	private Map<Object, Object> data;
	
	public RequestModel(boolean isSuccess, int statusCode, String msg) {
		this.isSuccess = isSuccess;
		this.statusCode = statusCode;
		this.msg = msg;
	}
	
	public boolean isSuccess() {
		return isSuccess;
	}
	public void setSuccess(boolean isSuccess) {
		this.isSuccess = isSuccess;
	}
	public int getStatusCode() {
		return statusCode;
	}
	public void setStatusCode(int statusCode) {
		this.statusCode = statusCode;
	}
	public String getMsg() {
		return msg;
	}
	public void setMsg(String msg) {
		this.msg = msg;
	}
	public Map<Object, Object> getData() {
		return data;
	}
	public void setData(Map<Object, Object> data) {
		this.data = data;
	}
}
