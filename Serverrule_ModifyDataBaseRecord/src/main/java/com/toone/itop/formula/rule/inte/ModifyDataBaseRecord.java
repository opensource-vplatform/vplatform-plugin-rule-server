package com.toone.itop.formula.rule.inte;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.toone.itop.rule.apiserver.util.AbstractRule4Tree;
import com.yindangu.v3.business.VDS;
import com.yindangu.v3.business.formula.api.IFormulaEngine;
import com.yindangu.v3.business.plugin.business.api.rule.IRule;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleContext;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleOutputVo;
import com.yindangu.v3.business.plugin.execptions.ConfigException;
import com.yindangu.v3.business.rule.api.parse.IQueryParse;
import com.yindangu.v3.business.rule.api.parse.ISQLBuf;
import com.yindangu.v3.business.vsql.apiserver.IFieldMapping;
import com.yindangu.v3.business.vsql.apiserver.IParamFieldVo;
import com.yindangu.v3.business.vsql.apiserver.IVSQLConditions;
import com.yindangu.v3.business.vsql.apiserver.IVSQLQuery;
import com.yindangu.v3.business.vsql.apiserver.IVSQLQueryUpdate;
import com.yindangu.v3.business.vsql.apiserver.VSQLConditionLogic;
import com.yindangu.v3.business.vsql.apiserver.IVSQLQueryUpdate.OptionType;
import com.yindangu.v3.business.vsql.apiserver.IVSQLQueryUpdate.UpdateType;
import com.yindangu.v3.platform.plugin.util.VdsUtils;

/**
 * 修改数据库中的记录
 * @author jiqj
 *
 */
public class ModifyDataBaseRecord extends AbstractRule4Tree implements IRule {
	private static final Logger log = LoggerFactory.getLogger(ModifyDataBaseRecord.class);

	public static final String D_RULE_NAME = "修改数据库中的记录";
	public static final String D_RULE_CODE = "ModifyDataBaseRecord";
	public static final String D_RULE_DESC = "对指定实体进行新增行操作。 \r\n" + "方法名：" + D_RULE_CODE;
	private static final String Key_DataSourcesMapping="dataSourcesMapping";
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public IRuleOutputVo evaluate(IRuleContext context) {
		//Map<String, Object> inParams = (Map<String, Object>) context.getRuleConfig().getConfigParams();
		List<Map> condParams = (List<Map>) context.getPlatformInput(Key_DataSourcesMapping);
		if(VdsUtils.collection.isEmpty(condParams)) {
			return context.newOutputVo();//.setMessage("没有映射数据").setSuccess(false);
		}
		IFormulaEngine engine = VDS.getIntance().getFormulaEngine();
		IQueryParse vparse= VDS.getIntance().getVSqlParse();
		
		for (int i = 0; i < condParams.size(); i++) {
			Map config = condParams.get(i);
			evaluateSub(vparse, engine, config);
		}

		return context.newOutputVo();
	}
	@SuppressWarnings({ "unchecked", "rawtypes", "deprecation" })
	private void evaluateSub(IQueryParse vparse,IFormulaEngine engine,Map config ) {
		
		// 目标表名
		String destTable = (String) config.get("dataSource");
		// 目标表名的类型，规则链中使用
		String dataSourceType = (String) config.get("dataSourceType");
		// 目标表名为表达式时需要解析处理
		if (dataSourceType != null && dataSourceType.trim().equals("1")) {
			destTable = engine.eval(destTable);
		}
		// 字段值
		List<Map> fields = (List<Map>) config.get("dataMap");

		// TODO: 替换掉前缀表名
		for (Map temp : fields) {
			String destField = (String) temp.get("colName");
			if (destField.indexOf(".") != -1) {
				destField = destTable + destField.substring(destField.lastIndexOf("."));
			}
			temp.put("colName", destField);
		}

		List<Map> conditionMap = (List<Map>) config.get("condition");

		//SQLBuf sqlBuffer = QueryConditionUtil.parseReplaceTableName(destTable, conditionMap, null);
		ISQLBuf bf = vparse.parseConditionsJson(Collections.singletonList(destTable), conditionMap, null);
		String queryCondition = bf.getSQL();
		Map<String, Object> parameters = bf.getParams();
		
		IVSQLQuery query = VDS.getIntance().getVSQLQuery();
		/*
		ParamField paramField = new ParamFieldUtil(fields, new FieldMapping() {

			@Override
			public String getDestFieldName() {
				return "colName";
			}

			@Override
			public String getSourceFieldName() {
				return "colValue";
			}

			@Override
			public SourceType getSourceType(Map row) {
				if (CollectionUtils.isEmpty(row)) {
					throw new ExpectedException("参数传入错误！row is null");
				}
				String sourceType = (String) row.get("valueType");
				if (StringUtils.isEmpty(sourceType)) {
					throw new ExpectedException("参数传入错误！参数[row]中没有包含对应的[sourceType]");
				}
				// 数据来源："expression : 表达式； sqlExpression ： sql表达式"
				SourceType type = null;
				if (sourceType.toLowerCase().equals("expression")) {
					type = SourceType.Expression;
				} else if (sourceType.toLowerCase().equals("sqlexpression")) {
					type = SourceType.SQLExpression;
				} else {
					throw new ExpectedException("参数传入错误！不支持[sourceType]类型.");
				}
				return type;
			}

		}).parse();
		*/
		IFieldMapping fm = new IFieldMapping() {

			@Override
			public String getDestFieldName() {
				return "colName";
			}

			@Override
			public String getSourceFieldName() {
				return "colValue";
			}

			@Override
			public SourceMappingType getSourceType(Map row) {
				if (VdsUtils.collection.isEmpty(row)) {
					throw new ConfigException("参数传入错误！row is null");
				}
				String sourceType = (String) row.get("valueType");
				if (VdsUtils.string.isEmpty(sourceType)) {
					throw new ConfigException("参数传入错误！参数[row]中没有包含对应的[sourceType]");
				}
				// 数据来源："expression : 表达式； sqlExpression ： sql表达式"
				SourceMappingType type = null;
				if (sourceType.toLowerCase().equals("expression")) {
					type = SourceMappingType.Expression;
				} else if (sourceType.toLowerCase().equals("sqlexpression")) {
					type = SourceMappingType.SQLExpression;
				} else {
					throw new ConfigException("参数传入错误！不支持[sourceType]类型.");
				}
				return type;
			}				
		};
		IParamFieldVo paramField = query.parseParam(fields,fm);
		List<Map> mappingItems = paramField.getItemsConverted();

		parameters.putAll(paramField.getParamMap());

		IVSQLQueryUpdate updateVo = query.newQueryUpdate();
		updateVo.setQueryName(null);// 源表
		updateVo.setTableName(destTable);// 目标表
		updateVo.setVSqlConditions(buildSqlConds(vparse,queryCondition));// 源表where条件
		
		buildSql(updateVo,   mappingItems, parameters, true);
		//IVSQLQueryFactory.getService().excuteSql(sql, parameters);
		query.excuteSql(updateVo, parameters);
	}
	/**
	 * 构造高级查询对象
	 * 
	 * @param sourceTable
	 *            源表名
	 * @param condSql
	 *            源表的数据过滤条件
	 * @param destTable
	 *            目标表名
	 * @param fields
	 *            记录匹配条件、以及要复制的字段数据
	 * @return
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected IVSQLQueryUpdate buildSql(IVSQLQueryUpdate updateVo,   List<Map> fields,
			Map<String, Object> parameters, boolean isBackGround) {
		//IVSQLQueryUpdate sql = new VSQLQueryUpdate();
		//sql.setQueryName(sourceTable);// 源表
		//updateVo.setTableName(destTable);// 目标表
		
		
		updateVo.setOpType(OptionType.getTypeByValue("replace")); // 操作类型：替换
		if (isBackGround) {
			IFormulaEngine engine = VDS.getIntance().getFormulaEngine();
			Map opFields = new HashMap();
			for (Map field : fields) {
				Object colName = field.get("colName");
				String colValue = (String) field.get("colValue");
				String valueType = (String) field.get("valueType");
				if (valueType.toLowerCase().equals("sqlexpression")) {
					String ev = engine.eval(colValue);
					opFields.put(field.get("colName"), "(" +ev+ ")");
				} else {
					opFields.put(colName, colValue);
				}

			}

			updateVo.setSetMap(opFields);// 要复制的字段
		}
		updateVo.setUniqueFields(Collections.EMPTY_MAP);// 相同记录判定的字段
		updateVo.setUpdateType(UpdateType.getTypeByValue("update")); // 固定为update
		return updateVo;
	}

	/**
	 * 构造高级查询where条件
	 * 
	 * @param condSql
	 * @return
	 */
	protected IVSQLConditions buildSqlConds(IQueryParse vparse,String condSql) {
		if (VdsUtils.string.isEmpty(condSql)) {
			return null;
		}
		IVSQLConditions c = vparse.getVSQLConditions(condSql);
		c.setLogic(VSQLConditionLogic.AND);
		/*
		IVSQLConditions cond = IVSQLConditionsFactory.getService().init();
		cond.setSqlConStr(condSql);
		cond.setLogic(VSQLConst.LogicAnd);*/
		return c;
	}
	
}
