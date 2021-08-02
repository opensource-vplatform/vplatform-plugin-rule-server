package com.toone.itop.formula.rule;

class InvokeTargetVo {
	//private String executeType;
	private String remoteUrl;
	private String ruleSetCode;
	private String componentCode;
	private boolean parallelism;
	private String instanceCode;
	
	/*
	public String getExecuteType() {
		return executeType;
	}
	public InvokeTargetVo setExecuteType(String executeType) {
		this.executeType = executeType;
		return this;
	}*/
	public String getRemoteUrl() {
		return remoteUrl;
	}
	public InvokeTargetVo setRemoteUrl(String remoteUrl) {
		this.remoteUrl = remoteUrl;
		return this;
	}
	public String getRuleSetCode() {
		return ruleSetCode;
	}
	public InvokeTargetVo setRuleSetCodes(Object ruleSetCode) {
		this.ruleSetCode = (String)ruleSetCode;
		return this;
	}
	public String getComponentCode() {
		return componentCode;
	}
	public InvokeTargetVo setComponentCodes(Object componentCode) {
		this.componentCode = (String)componentCode;
		return this;
	}
	public boolean isParallelism() {
		return parallelism;
	}
	public void setParallelisms(boolean parallelism) {
		this.parallelism = parallelism;
	}
	public String getInstanceCode() {
		return instanceCode;
	}
	public void setInstanceCode(String instanceCode) {
		this.instanceCode = instanceCode;
	}
	
}
