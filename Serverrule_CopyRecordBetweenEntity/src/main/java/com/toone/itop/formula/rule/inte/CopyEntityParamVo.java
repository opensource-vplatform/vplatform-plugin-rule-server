package com.toone.itop.formula.rule.inte;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.yindangu.v3.business.plugin.business.api.rule.ContextVariableType;
import com.yindangu.v3.business.plugin.execptions.ConfigException;

/**执行的操作类型：1 ：追加；2：忽略；3：替换；4：合并*/
enum OperType{
	Append(1),Ignore(2),Replace(3),Merge(4);
	private final int value;
	private OperType(int type) {
		value = type;
	}
	public int getValue() {
		return value;
	}
	
	static OperType getOperType(String type) {
		if(type == null || (type = type.trim()).length()==0) {
			return null;
		}
		OperType rs = null;
		int tp = Integer.parseInt(type);
		for(OperType t : OperType.values()) {
			if(tp == t.getValue()) {
				rs = t;
				break;
			}
		}
		return rs;
	}
}

	
/**复制的实体类型*/
class CopyEntityParamVo{
	private OperType type;
	private String sourceName;
	private ContextVariableType sourceType;
	private String destName;
	private ContextVariableType destType;
	private List<String> checkitems;
	/**合并字段*/
	private List<String> mageitems ;
	
	private List<Map> condition;
	private List<Map> queryParam;
	
	/**替换/合并的方式下，是否新增记录(默认true)*/
	private boolean  addRecord = true;
	/**执行的操作类型：1 ：追加；2：忽略；3：替换；4：合并*/
	public OperType getType() {
		return type;
	}
	public CopyEntityParamVo setTypes(String type) {
		this.type = OperType.getOperType(type);
		return this;
	}
	/**数据来源的实体名称*/
	public String getSourceName() {
		return sourceName;
	}
	public CopyEntityParamVo setSourceName(String sourceName) {
		this.sourceName = sourceName;
		return this;
	}
	public ContextVariableType getSourceType() {
		return sourceType;
	}
	public CopyEntityParamVo setSourceType(String types) {
		ContextVariableType type=  ContextVariableType.getInstanceType(types);
		if(type == null) {
			throw new ConfigException("不支持取值上下文类型:" + types);
		}
		this.sourceType = type;
		return this;
	}
	/**数据目标的实体名称*/
	public String getDestName() {
		return destName;
	}
	/**数据目标的实体名称*/
	public CopyEntityParamVo setDestName(String destName) {
		this.destName = destName;
		return this;
	}
	public ContextVariableType getDestType() {
		return destType;
	}
	public CopyEntityParamVo setDestType(String types) {
		ContextVariableType type=  ContextVariableType.getInstanceType(types);
		if(type == null) {
			throw new ConfigException("不支持取值上下文类型:" + types);
		}
		this.destType = type;
		return this;
	}
	@SuppressWarnings("unchecked")
	public List<String> getCheckitems() {
		return (checkitems == null ? Collections.EMPTY_LIST : checkitems);
	}
	public CopyEntityParamVo setCheckitems(List<String> checkitems) {
		this.checkitems = checkitems;
		return this;
	}
	/**合并字段*/
	public List<String> getMageitems() {
		return mageitems;
	}
	
	@SuppressWarnings("unchecked")
	public CopyEntityParamVo setMageitemObject(Object items) {
		if(items != null && items instanceof List) {
			this.mageitems =(List<String>) items;	
		}
		else {
			this.mageitems = Collections.emptyList();
		}
		return this;
	}
	public List<Map> getCondition() {
		return condition;
	}
	public CopyEntityParamVo setCondition(List<Map> condition) {
		this.condition = condition;
		return this;
	}
	public List<Map> getQueryParam() {
		return queryParam;
	}
	public CopyEntityParamVo setQueryParam(List<Map> queryParamMap) {
		this.queryParam = queryParamMap;
		return this;
	}
	public boolean isAddRecord() {
		return addRecord;
	}
	/**替换/合并的方式下，是否新增记录*/
	public CopyEntityParamVo setAddRecordObject(Object addRecord) { 
		if(addRecord !=null && addRecord instanceof Boolean) {
			this.addRecord =((Boolean)addRecord).booleanValue();
		}		
		return this;
	}
}
/***复制映射信息*/
class MappingVo{
	private List<Map> mapping;
	private Map<String, Object> sqlExper;
	private Map<String, Object> lastExper;
	/***操作方式的映射字段 */
	@SuppressWarnings("rawtypes")
	public List<Map> getMapping() {
		return mapping;
	}
	public MappingVo setMapping(List<Map> mapping) {
		this.mapping = mapping;
		return this;
	}
	/**sql类型的函数表达式*/
	public Map<String, Object> getSqlExper() {
		return sqlExper;
	}
	public MappingVo setSqlExper(Map<String, Object> sqlExper) {
		this.sqlExper = sqlExper;
		return this;
	}
	/**在得到最后结果才进行处理的函数表达式*/
	public Map<String, Object> getLastExper() {
		return lastExper;
	}
	public MappingVo setLastExper(Map<String, Object> lastExper) {
		this.lastExper = lastExper;
		return this;
	}
}