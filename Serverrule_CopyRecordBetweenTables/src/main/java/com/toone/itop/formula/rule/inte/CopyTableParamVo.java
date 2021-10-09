package com.toone.itop.formula.rule.inte;
 
import java.util.List;
import java.util.Map;
 
/**
 * 	
	private static final String REPEAT_TYPE_IGNORE = "1";

	
	private static final String REPEAT_TYPE_APPEND = "2";

	
	private static final String REPEAT_TYPE_UPDATE = "3";
 * @author jiqj
 *
 */
enum RepeatType{
	
	/** 1-行重复处理方式：忽略：对于重复行，什么都不做 */
	Ignore(1),
	/** 2-行重复处理方式：追加：对于重复行，执行insert */
	Append(2),
	/** 3-行重复处理方式：更新：对于重复行，执行update，但是具体字段究竟是累加还是覆盖，由treatRepeattype参数决定 */
	Update(3);
	private final int value;
	private RepeatType(int type) {
		value = type;
	}
	public int getValue() {
		return value;
	}
	
	static RepeatType getRepeatType(String type) {
		if(type == null || (type = type.trim()).length()==0) {
			return null;
		}
		RepeatType rs = null;
		int tp = Integer.parseInt(type);
		for(RepeatType t : RepeatType.values()) {
			if(tp == t.getValue()) {
				rs = t;
				break;
			}
		}
		return rs;
	}
}
enum FieldUpdateType{
	/**VSQLConst.OpDescend*/
	Descend("descend"),
	/**VSQLConst.OpLeave*/
	Leave( "leave"),
	/**VSQLConst.OpReplace*/
	Replace("replace"),
	/**VSQLConst.OpAccumulate*/
	Accumulate("accumulate");
	private final String value;
	private FieldUpdateType(String v) {
		value = v;
	}
	public String getValue() {
		return value;
	}
}
	
/**复制的实体类型*/
class CopyTableParamVo{
	private RepeatType repeatType;
	private List<Map> equalFields;
	private String sourceTable;
	
	private String destTable;
	
	private List<Map> queryParam;
	private List<Map> condition;
	
	/**重复处理方式*/
	public RepeatType getRepeatType() {
		return repeatType;
	}
	public CopyTableParamVo setRepeatTypes(String types) { 
		this.repeatType = RepeatType.getRepeatType(types);
		return this;
	}
	/**源表名*/
	public String getSourceTable() {
		return sourceTable;
	}
	public CopyTableParamVo setSourceTable(String sourceName) {
		this.sourceTable = sourceName;
		return this;
	}
 
 
	/**目标表名*/
	public String getDestTable() {
		return destTable;
	}
	/**目标表名*/
	public CopyTableParamVo setDestTable(String destName) {
		this.destTable = destName;
		return this;
	}
 
 
 
	public List<Map> getEqualFields() {
		return equalFields;
	}
	public CopyTableParamVo setEqualFields(List<Map> condition) {
		this.equalFields = condition;
		return this;
	}
	public List<Map> getQueryParam() {
		return queryParam;
	}
	public CopyTableParamVo setQueryParam(List<Map> queryParamMap) {
		this.queryParam = queryParamMap;
		return this;
	}
	/**源表的数据过滤条件*/
	public List<Map> getCondition() {
		return condition;
	}
	public CopyTableParamVo setCondition(List<Map> condition) {
		this.condition = condition;
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