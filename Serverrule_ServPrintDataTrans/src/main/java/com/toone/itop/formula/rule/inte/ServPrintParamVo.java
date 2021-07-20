package com.toone.itop.formula.rule.inte;

import java.util.List;
import java.util.Map;

import com.yindangu.v3.business.plugin.business.api.rule.ContextVariableType;
import com.yindangu.v3.business.plugin.execptions.EnviException;

class ServPrintParamVo {
	
	private String resultEntity;

	private String resultCodeField;
	
	private ContextVariableType resultEntityType;

	private String resultContentField;
	
	private boolean continuousPrint;
	
	private String printType;
	
	private List<String>  selectTemplates;
	private List<Map<String, Object>> templetes;
	
	/**报表打印数据转换结果实体, 用于输出*/
	public String getResultEntity() {
		return resultEntity;
	}

	public ServPrintParamVo setResultEntity(String resultEntity) {
		this.resultEntity = resultEntity;
		return this;
	}
	/**报表打印数据转换结果实体模板编码字段，用于输出*/
	public String getResultCodeField() {
		return resultCodeField;
	}

	public ServPrintParamVo setResultCodeField(String resultCodeField) {
		this.resultCodeField = resultCodeField;
		return this;
	}
	/**用于输出到对应实体类型*/
	public ContextVariableType getResultEntityType() {
		return resultEntityType;
	}
	
	public ServPrintParamVo setResultEntityType(ContextVariableType resultEntityType) { 
		this.resultEntityType =resultEntityType;// ContextVariableType.getInstanceType(resultEntityType);
		if(this.resultEntityType == null) {
			throw new EnviException("不存在的变量类型:" + resultEntityType);
		}
		return this;
	}
	/**报表打印数据转换结果实体数据字段，用于输出*/
	public String getResultContentField() {
		return resultContentField;
	}
	public ServPrintParamVo setResultContentField(String resultContentField) {
		this.resultContentField = resultContentField;
		return this;
	}
	/**是否连打*/
	public boolean isContinuousPrint() {
		return continuousPrint;
	}

	public ServPrintParamVo setContinuousPrints(Object pb) {
		Boolean b = ((pb != null && pb instanceof Boolean) ? (Boolean)pb : false);
		this.continuousPrint = b;
		return this;
	}
	/**操作类型*/
	public String getPrintType() {
		return printType;
	}

	public ServPrintParamVo setPrintType(String printType) {
		this.printType = printType;
		return this;
	}
	/**选择打印的模板，来源类型为表达式*/
	public 		List<String>  getSelectTemplates() {
		return selectTemplates;
	}

	public ServPrintParamVo setSelectTemplates(	List<String>  selectTemplates) {
		this.selectTemplates = selectTemplates;
		return this;
	}

	/**模板及模板数据源集合*/
	public List<Map<String, Object>> getTempletes() {
		return templetes;
	}
	public ServPrintParamVo setTempletes(List<Map<String, Object>> templetes) {
		this.templetes = templetes;
		return this;
	}
}
