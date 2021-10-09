package com.toone.itop.formula.rule.inte;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.toone.itop.rule.apiserver.util.AbstractRule4Tree;
import com.yindangu.v3.business.VDS;
import com.yindangu.v3.business.plugin.business.api.rule.IRule;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleContext;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleOutputVo;
import com.yindangu.v3.business.plugin.execptions.ConfigException;
import com.yindangu.v3.business.plugin.execptions.EnviException;
import com.yindangu.v3.business.rule.api.parse.IConditionParse;
import com.yindangu.v3.business.rule.api.parse.ISQLBuf;
import com.yindangu.v3.business.vsql.apiserver.IFieldMapping;
import com.yindangu.v3.business.vsql.apiserver.IParamFieldVo;
import com.yindangu.v3.business.vsql.apiserver.IVSQLConditions;
import com.yindangu.v3.business.vsql.apiserver.IVSQLQuery;
import com.yindangu.v3.business.vsql.apiserver.IVSQLQueryUpdate;
import com.yindangu.v3.business.vsql.apiserver.IVSQLQueryUpdate.CheckType;
import com.yindangu.v3.business.vsql.apiserver.IVSQLQueryUpdate.OptionType;
import com.yindangu.v3.business.vsql.apiserver.IVSQLQueryUpdate.UpdateType;
import com.yindangu.v3.platform.plugin.util.VdsUtils;


public class CopyRecordBetweenTables  extends AbstractRule4Tree implements IRule{
	public static final String D_RULE_NAME = "表间数据复制";
	public static final String D_RULE_CODE = "CopyRecordBetweenTables";
	public static final String D_RULE_DESC = "";
	


	/** 字段更新处理方式：累加：如果行重复处理方式为更新的话，这个参数有效 */
	private static final String FIELD_UPDATE_TYPE_ADD = "1";

	/** 字段更新处理方式：覆盖：如果行重复处理方式为更新的话，这个参数有效 */
	private static final String FIELD_UPDATE_TYPE_REPLACE = "2";

	/** 字段更新处理方式：忽略：如果行重复处理方式为更新的话，这个参数有效 */
	private static final String FIELD_UPDATE_TYPE_IGNORE = "3";// 或者为空

	/** 字段更新处理方式：累减：如果行重复处理方式为更新的话，这个参数有效 */
	private static final String FIELD_UPDATE_TYPE_REDUCE = "4";// 或者为空

	private static final String SUM = "sum";

	private static final String MAX = "max";
	

	/*
	ParamField paramField = new ParamFieldUtil(equalFields, new FieldMapping() {

		@Override
		public SourceType getSourceType(Map row) {
			if (CollectionUtils.isEmpty(row)) {
				throw new ExpectedException("参数传入错误！row is null");
			}
			String sourceType = (String) row.get("sourcetype");
			if (StringUtils.isEmpty(sourceType)) {
				throw new ExpectedException("参数传入错误！参数[row]中没有包含对应的[sourceType]");
			}
			SourceType type = null;
			if (sourceType.equalsIgnoreCase("expression")) {
				type = SourceType.Expression;
			} else if (sourceType.equalsIgnoreCase("tableField")) {
				type = SourceType.TableField;
			}
			return type;
		}

		@Override
		public String getSourceFieldName() {
			return "sourceField";
		}

		@Override
		public String getDestFieldName() {
			return "destField";
		}
	}).parse();
	equalFields = paramField.getItemsConverted();
	*/
	private static class FileMapVo  implements IFieldMapping{
		@Override
		public String getSourceFieldName() {
			return "sourceField";
		}
		@Override
		public String getDestFieldName() {
			return "destField";
		}

		@Override
		public SourceMappingType getSourceType(Map<String, Object> typeMap) {
			if (VdsUtils.collection.isEmpty(typeMap)) {
				throw new ConfigException("参数传入错误！row is null");
			}
			String sourceType = (String) typeMap.get("sourcetype");
			if (VdsUtils.string.isEmpty(sourceType)) {
				throw new ConfigException("参数传入错误！参数[row]中没有包含对应的[sourceType]");
			}
			SourceMappingType type = null;
			if (sourceType.equalsIgnoreCase("expression")) {
				type = SourceMappingType.Expression;
			} else if (sourceType.equalsIgnoreCase("tableField")) {
				type = SourceMappingType.TableField;
			}
			return type;
		}
	}
	@Override
	@SuppressWarnings({ "unchecked", "rawtypes", "deprecation"  })
	public IRuleOutputVo evaluate(IRuleContext context) {
		/*
		//Map<String, Object> inParams =  context.getRuleConfig().getConfigParams();
		// 字段对应关系数组
		List<Map> equalFields = (List<Map>) context.getPlatformInput("equalFields");

		// 源表名
		String sourceTable = (String) context.getPlatformInput("sourceTableName");

		// 重复处理方式
		String repeatType = (String) context.getPlatformInput("repeatType");

		// 目标表名
		String destTable = (String) context.getPlatformInput("destTableName");

		List<Map> condParams = (List<Map>) context.getPlatformInput("queryParam");
		
		// 源表的数据过滤条件
		List<Map> conditionMap = (List<Map>) context.getPlatformInput("condition");
		*/
		CopyTableParamVo cpv = new CopyTableParamVo();
		cpv.setEqualFields((List<Map>) context.getPlatformInput("equalFields"))
			.setSourceTable( (String) context.getPlatformInput("sourceTableName"))
			.setRepeatTypes((String) context.getPlatformInput("repeatType"))
			.setDestTable((String) context.getPlatformInput("destTableName"))
			.setCondition((List<Map>) context.getPlatformInput("condition"))
			.setQueryParam((List<Map>) context.getPlatformInput("queryParam"));
			
		IVSQLQuery vquery = VDS.getIntance().getVSQLQuery() ;
		IConditionParse parse =VDS.getIntance().getVSqlParse(); 
		ISQLBuf sql = parse.parseConditionsJson(null, cpv.getCondition(), cpv.getQueryParam());
		//SQLBuf sql = QueryConditionUtil.parseNotSupportRuleTemplate(cpv.getCondition(), cpv.getQueryParam());
		String condSql = sql.getSQL();
		Map<String, Object> params = sql.getParams();
		IParamFieldVo paramField= vquery.parseParam(cpv.getEqualFields(), new FileMapVo());
		params.putAll(paramField.getParamMap()); 

		IVSQLQueryUpdate vSQLQueryUpdate = buildSql(cpv,condSql);
		// System.out.println(JsonUtils.toJson(sql));
		//IVSQLQueryFactory.getService().excuteSql(vSQLQueryUpdate, params);
		vquery.excuteSql(vSQLQueryUpdate, params);
		return context.newOutputVo();
	}

	/**
	 * 构造高级查询对象
	 * 
	 * @param sourceTable
	 *        源表名
	 * @param condSql
	 *        源表的数据过滤条件
	 * @param destTable
	 *        目标表名
	 * @param repeatType
	 *        行重复处理方式
	 * @param fields
	 *        记录匹配条件、以及要复制的字段数据
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private IVSQLQueryUpdate buildSql(CopyTableParamVo cpv,String condSql) {
		//VSQLQueryUpdate sql = new VSQLQueryUpdate();
		IVSQLQuery vquery =VDS.getIntance().getVSQLQuery();
		IVSQLQueryUpdate queryUpdate = vquery.newQueryUpdate();
		
		queryUpdate.setQueryName(cpv.getSourceTable());// 源表
		if (!VdsUtils.string.isEmpty(condSql)) {
			IVSQLConditions cdt = vquery.getVSQLConditions(condSql);
			queryUpdate.setVSqlConditions(cdt);// 源表where条件
		}
		
		queryUpdate.setTableName(cpv.getDestTable());// 目标表

		OptionType operType = translateRepeatType(cpv.getRepeatType());
		queryUpdate.setOpType(operType); // 行重复处理方式：忽略=1，追加=2，更新=3
		Map<String, Map> opFields = buildOpFields(operType, cpv.getEqualFields());
		queryUpdate.setSetMap(opFields.get("opFields"));// 要操作的字段
		queryUpdate.setSetOpMap(opFields.get("opTypeFields"));// 要操作的字段的更新类型
		queryUpdate.setUniqueFields(opFields.get("eqFields"));// 相同记录判定的字段
		queryUpdate.setUpdateType(UpdateType.Insert); // 固定为insert
		// weicd
		queryUpdate.setCheckType(CheckType.ManyToOne);
		queryUpdate.setFieldFuncs(opFields.get("fieldFuncs"));
		// queryUpdate.setCheckType(VSqlConst.CheckManyToMany);//
		// 目标表和源表如果出现一对多、或者多对一都抛出异常
		return queryUpdate;
	}

	/**
	 * 转换开发系统参数到高级查询参数--行重复处理方式
	 * 
	 * @param repeatType
	 * @return
	 */
	private OptionType translateRepeatType(RepeatType repeatType) {
		/*if (REPEAT_TYPE_IGNORE.equals(repeatType))
			return VSQLConst.OpLeave;
		if (REPEAT_TYPE_APPEND.equals(repeatType))
			return VSQLConst.OpAdd;
		if (REPEAT_TYPE_UPDATE.equals(repeatType))
			return VSQLConst.OpReplace;
		return null;*/
		
		if( repeatType==null){
			throw new EnviException("行重复处理方式不正确:" + repeatType);
		}
		OptionType rs = null;
		switch (repeatType) {
		case Ignore:
			rs =OptionType.Leave;
			break;
		case Append:
			rs =OptionType.Add;
			break;
		case Update:
			rs =OptionType.Replace;
			break;
		default:
			throw new EnviException("行重复处理方式不正确:" + repeatType);
		}
		return rs;
	}

	/**
	 * 字段更新类型(行重复处理方式为更新时有效)
	 * 
	 * @param fieldUpdateType
	 * @return
	 */
	private FieldUpdateType translateFieldUpdateType(String fieldUpdateType) {
		/*if (StringUtils.isEmpty(fieldUpdateType)//
				|| FIELD_UPDATE_TYPE_IGNORE.equals(fieldUpdateType))
			return VSQLConst.OpLeave;
		if (FIELD_UPDATE_TYPE_REPLACE.equals(fieldUpdateType))
			return VSQLConst.OpReplace;
		if (FIELD_UPDATE_TYPE_ADD.equals(fieldUpdateType))
			return VSQLConst.OpAccumulate;
		if (FIELD_UPDATE_TYPE_REDUCE.equals(fieldUpdateType))
			return VSQLConst.OpDescend;
		return null;
		*/
		FieldUpdateType rs = null;
		if (VdsUtils.string.isEmpty(fieldUpdateType)//
				|| FIELD_UPDATE_TYPE_IGNORE.equals(fieldUpdateType)) {
			 rs = FieldUpdateType.Leave;
		}
		else if (FIELD_UPDATE_TYPE_REPLACE.equals(fieldUpdateType)) {
			 rs = FieldUpdateType.Replace;
		}
		else if (FIELD_UPDATE_TYPE_ADD.equals(fieldUpdateType)) {
			 rs = FieldUpdateType.Accumulate;
		}
		else if (FIELD_UPDATE_TYPE_REDUCE.equals(fieldUpdateType)) {
			 rs = FieldUpdateType.Descend;
		}
		return rs;
	}

	/**
	 * 要操作的字段 key=目标表字段，value=源表字段 <br>
	 * 要操作字段的更新方式：key=目标表字段，value=字段更新处理方式：（累加/覆盖）<br>
	 * 相同记录判定的字段 key=目标表字段，value=源表字段
	 * 
	 * @param fields
	 * @return
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected Map buildOpFields(OptionType statementType, List<Map> fields) {
		Map result = new HashMap();
		Map opFields = new HashMap();
		Map opTypeFields = new HashMap();
		Map eqFields = new HashMap();
		Map fieldFuncs = new HashMap();
		result.put("opFields", opFields);
		result.put("opTypeFields", opTypeFields);
		result.put("eqFields", eqFields);
		result.put("fieldFuncs", fieldFuncs);

		for (Map field : fields) {
			// 标识为检查的字段
			boolean isCheckField = Boolean.parseBoolean(field.get("checkRepeat").toString());
			Object destField = field.get("destField");
			Object sourceField = field.get("sourceField");

			if (isCheckField) {
				eqFields.put(destField, sourceField);
			}
			else {
				opFields.put(destField, sourceField);
				String fieldUpdateType = (String) field.get("treatRepeattype");
				FieldUpdateType opType = translateFieldUpdateType(fieldUpdateType);
				opTypeFields.put(destField, (opType == null ? null: opType.getValue()));
				
				if (statementType == OptionType.Replace) {
					if (opType == FieldUpdateType.Accumulate  || opType == FieldUpdateType.Descend) {
						fieldFuncs.put(destField, SUM);
					}
					else if (opType == FieldUpdateType.Replace) {
						fieldFuncs.put(destField, MAX);
					}
				}
			}
		}
		return result;
	}

	/**
	 * 构造高级查询where条件
	 * 
	 * @param condSql
	 * @return 
	private IVSQLConditions buildSqlConds(String condSql) {
		//IVSQLQueryUpdate queryUpdate = VDS.getIntance().getVSqlParse().getVSQLConditions(condSql);
		//IVSQLConditions cond =VDS.getIntance().getVSqlParse().getVSQLConditions(condSql);
		//cond.setSqlConStr(condSql);
		//cond.setLogic(VSQLConst.LogicAnd);
		return cond;
	}*/
}
