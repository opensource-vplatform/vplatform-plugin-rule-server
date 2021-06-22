package com.toone.itop.formula.rule.inte;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.toone.itop.rule.apiserver.util.AbstractRule4Tree;
import com.yindangu.v3.business.VDS;
import com.yindangu.v3.business.formula.api.IFormulaEngine;
import com.yindangu.v3.business.metadata.api.IDataObject;
import com.yindangu.v3.business.metadata.api.IDataView;
import com.yindangu.v3.business.plugin.business.api.rule.ContextVariableType;
import com.yindangu.v3.business.plugin.business.api.rule.IRule;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleContext;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleOutputVo;
import com.yindangu.v3.business.plugin.execptions.ConfigException;
import com.yindangu.v3.platform.plugin.util.VdsUtils;

public class ModifyDataBaseRecord extends AbstractRule4Tree implements IRule {
	private static final Logger log = LoggerFactory.getLogger(ModifyDataBaseRecord.class);

	public static final String D_RULE_NAME = "修改数据库中的记录";
	public static final String D_RULE_CODE = "AddTableRecord";
	public static final String D_RULE_DESC = "对指定实体进行新增行操作。 \r\n" + "方法名：" + D_RULE_CODE;
	private static final String Key_DataSourcesMapping="dataSourcesMapping";
	@Override
	public IRuleOutputVo evaluate(IRuleContext context) {
		//Map<String, Object> inParams = (Map<String, Object>) context.getRuleConfig().getConfigParams();
		List<Map> condParams = (List<Map>) context.getPlatformInput(Key_DataSourcesMapping);
		if(VdsUtils.collection.isEmpty(condParams)) {
			return context.newOutputVo();//.setMessage("没有映射数据").setSuccess(false);
		}
		IFormulaEngine formulaEngine = VDS.getIntance().getFormulaEngine();
		for (int i = 0; i < condParams.size(); i++) {
			Map config = condParams.get(i);
			// 目标表名
			String destTable = (String) config.get("dataSource");
			// 目标表名的类型，规则链中使用
			String dataSourceType = (String) config.get("dataSourceType");
			// 目标表名为表达式时需要解析处理
			if (dataSourceType != null && dataSourceType.trim().equals("1")) {
				destTable = formulaEngine.eval(destTable);
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

			SQLBuf sqlBuffer = QueryConditionUtil.parseReplaceTableName(destTable, conditionMap, null);
			String queryCondition = sqlBuffer.getSQL();
			Map<String, Object> parameters = sqlBuffer.getParams();

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

			List<Map> mappingItems = paramField.getItemsConverted();

			parameters.putAll(paramField.getParamMap());

			String sourceTable = null;
			VSQLQueryUpdate sql = buildSql(sourceTable, queryCondition, destTable, mappingItems, parameters, true);
			IVSQLQueryFactory.getService().excuteSql(sql, parameters);
		}

		return context.newOutputVo().setSuccess(true);
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
	protected VSQLQueryUpdate buildSql(String sourceTable, String condSql, String destTable, List<Map> fields,
			Map<String, Object> parameters, boolean isBackGround) {
		VSQLQueryUpdate sql = new VSQLQueryUpdate();

		sql.setQueryName(sourceTable);// 源表
		sql.setvSqlConditions(buildSqlConds(condSql));// 源表where条件
		sql.setTableName(destTable);// 目标表
		sql.setOpType("replace"); // 操作类型：替换
		if (isBackGround) {
			Map opFields = new HashMap();
			for (Map field : fields) {
				Object colName = field.get("colName");
				String colValue = (String) field.get("colValue");
				String valueType = (String) field.get("valueType");
				if (valueType.toLowerCase().equals("sqlexpression")) {
					opFields.put(field.get("colName"), "(" + FormulaEngineFactory.getFormulaEngine().eval(colValue)
							+ ")");
				} else {
					opFields.put(colName, colValue);
				}

			}

			sql.setSetMap(opFields);// 要复制的字段
		}
		sql.setUniqueFields(Collections.EMPTY_MAP);// 相同记录判定的字段
		sql.setUpdateType("update"); // 固定为update
		return sql;
	}

	/**
	 * 构造高级查询where条件
	 * 
	 * @param condSql
	 * @return
	 */
	protected IVSQLConditions buildSqlConds(String condSql) {
		if (StringUtils.isBlank(condSql)) {
			return null;
		}

		IVSQLConditions cond = IVSQLConditionsFactory.getService().init();
		cond.setSqlConStr(condSql);
		cond.setLogic(VSQLConst.LogicAnd);
		return cond;
	}
	
}
