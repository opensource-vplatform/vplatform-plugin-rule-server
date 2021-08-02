package com.toone.itop.formula.rule;
class EpImplVo{
	private String componentCode;
	private String metaCode;
	private String epImplCode;
	public String getComponentCode() {
		return componentCode;
	}
	public void setComponentCode(String componentCode) {
		this.componentCode = componentCode;
	}
	public String getMetaCode() {
		return metaCode;
	}
	public void setMetaCode(String metaCode) {
		this.metaCode = metaCode;
	}
	public String getEpImplCode() {
		return epImplCode;
	}
	public void setEpImplCode(String epImplCode) {
		this.epImplCode = epImplCode;
	}
	public String getKey(String key) {
		if("componentCode".equalsIgnoreCase(key)) {
			return getComponentCode();
		}
		else if("metaCode".equalsIgnoreCase(key)) {
			return getMetaCode();
		}
		else if("epImplCode".equalsIgnoreCase(key)) {
			return getEpImplCode();
		}
		else {
			return null;
		}
	}
}