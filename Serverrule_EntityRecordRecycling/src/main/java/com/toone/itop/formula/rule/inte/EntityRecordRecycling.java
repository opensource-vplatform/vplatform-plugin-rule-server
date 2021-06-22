package com.toone.itop.formula.rule.inte;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yindangu.v3.business.VDS;
import com.yindangu.v3.business.formula.api.IFormulaEngine;
import com.yindangu.v3.business.metadata.api.IDataObject;
import com.yindangu.v3.business.metadata.api.IDataView;
import com.yindangu.v3.business.plugin.business.api.rule.ContextVariableType;
import com.yindangu.v3.business.plugin.business.api.rule.IRule;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleContext;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleOutputVo;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleVObject;
import com.yindangu.v3.business.plugin.execptions.ConfigException;
import com.yindangu.v3.business.rule.api.parse.IQueryParse;
import com.yindangu.v3.business.rule.api.parse.ISQLBuf;
/**
 * 实体记录循环处理
 * @author jiqj
 *
 */
public class EntityRecordRecycling  implements IRule  {
	private static final Logger log = LoggerFactory.getLogger(EntityRecordRecycling.class); 
	public static final String D_RULE_NAME = "实体记录循环处理";
	public static final String D_RULE_CODE = "EntityRecordRecycling";
	public static final String D_RULE_DESC = "在循环体内部使用，用于中断最近的封闭循环体（break）或中断最近的封闭循环体的一次迭代，开始新的迭代（continue）。\r\n" + 
			"方法名：" + D_RULE_CODE;
	
	private static final String D_TargetEntity="TargetEntity";
	private static final String D_TargetFields="TargetFields";
	private static final String D_Condition="Condition";
	private static final String D_TargetEntityType="TargetEntityType";
	
	private static final String D_SourceType="SourceType";
	private static final String D_FieldValue="FieldValue";
	private static final String D_TargetField="TargetField";
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public IRuleOutputVo evaluate(IRuleContext context) {
		//Map<String, Object> inParams = (Map<String, Object>) context.getRuleConfig().getConfigParams();
		String entityName = (String) context.getPlatformInput(D_TargetEntity);
		List<Map> mappingItems = (List<Map>) context.getPlatformInput(D_TargetFields);
		List<Map> conditions = (List<Map>) context.getPlatformInput(D_Condition);
		
		ContextVariableType targetEntityType ;{
			String t = (String) context.getPlatformInput(D_TargetEntityType);	
			targetEntityType = ContextVariableType.getInstanceType(t);
		}
		
		//SQLBuf sql = QueryConditionUtil.parseConditionsNotSupportRuleTemplate(condition);
		IQueryParse vparse= VDS.getIntance().getVSqlParse(); 
		@SuppressWarnings("deprecation")
		ISQLBuf sql = vparse.parseConditionsJson(conditions);
		String condSql = sql.getSQL();
		Map<String, Object> queryParams = sql.getParams();
		IRuleVObject ruleVObject = context.getVObject();
		
		@SuppressWarnings("deprecation")
		IDataView dataView = (IDataView)ruleVObject.getContextObject(entityName, targetEntityType) ;//(IDataView) getDataViewWithType(context, entityName, TargetEntityType);
		if (dataView == null) {
			return context.newOutputVo();
		}
		
		List<IDataObject> list = dataView.select(condSql, queryParams);
		for (IDataObject dataObject : list) {
			for (int i = 0; i < mappingItems.size(); i++) {
				Map<String, String> item = mappingItems.get(i);
				String type = item.get(D_SourceType);
				String formula = item.get(D_FieldValue);
				String destField = item.get(D_TargetField);
				// 获取需要设置的值
				Object value = getTargetValue(type, formula);
				dataObject.set(destField, value);
			}
		}
		return context.newOutputVo().put(Boolean.TRUE);
	}
	/**
	 * 获取来源值
	 * 
	 * @param type
	 * @param formula
	 * @return
	 */
	private Object getTargetValue(String type, String formula) {
		Object expressValue = null;
		if ("expression".equalsIgnoreCase(type)) {
			IFormulaEngine en = VDS.getIntance().getFormulaEngine();
			expressValue = en.eval(formula);
			return expressValue;
		}
		else {
			throw new ConfigException( "不支持类型[" + type + "]的值来源.");
		}
	}
	
/*	enum VariableType {
		// 活动集输出变量
		RuleSetOutput("ruleSetOutput"),
		// 活动集输入变量
		RuleSetInput("ruleSetInput"),
		// 活动集上下文变量
		RuleSetVar("ruleSetVar"),
		// 窗体实体
		Window("window");

		private String type;

		private VariableType(String type) {
			this.type = type;
		}

		public static VariableType getInstanceType(String key) {
			VariableType ret = null;
			for (VariableType type : VariableType.values()) {
				if (key.equals(type.type)) {
					ret = type;
				}
			}
			return ret;
		}
	}*/	
}
