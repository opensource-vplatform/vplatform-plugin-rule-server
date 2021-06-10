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
import com.yindangu.v3.business.plugin.execptions.ConfigException;
import com.yindangu.v3.platform.plugin.util.VdsUtils;

public class AddTableRecord implements IRule {
	private static final Logger log = LoggerFactory.getLogger(AddTableRecord.class);

	public static final String D_RULE_NAME = "新增实体记录";
	public static final String D_RULE_CODE = "AddTableRecord";
	public static final String D_RULE_DESC = "对指定实体进行新增行操作。 \r\n" + "方法名：" + D_RULE_CODE;
	private static final String Param_TableScope = "TableType";
	private static final String Param_TableName = "TableName";
	private static final String Param_Mappings = "Mappings";
	private static final String Param_NumCount = "NumCount";

	private static final String Key_ruleSetVar = "ruleSetVar", Key_ruleSetOutput = "ruleSetOutput",
			Key_ruleSetInput = "ruleSetInput";

	@Override
	public IRuleOutputVo evaluate(IRuleContext context) {
		log.info("使用新插件接口..");
		String numCountFormula = (String) context.getPlatformInput(Param_NumCount);
		List<Map<String, Object>> mappings = (List<Map<String, Object>>) context.getPlatformInput(Param_Mappings);
		if (VdsUtils.collection.isEmpty(mappings)) { // 没有配置映射
			IRuleOutputVo vo = context.newOutputVo();//.setMessage("缺少字段映射").setSuccess(false);
			return vo;
		}
		
		IFormulaEngine formulaEngine = VDS.getIntance().getFormulaEngine();
		int count;{
			Object numCount = formulaEngine.eval(numCountFormula);
			String num = String.valueOf(numCount);
			if (!isNum(num)) {
				throw new ConfigException("表达式执行结果不合法，请检查表达式");
			}
			float floatNumber = Float.parseFloat(num);
			int number = (int) floatNumber;
			count = VdsUtils.string.isEmpty(num) ? 1 : number;
		}
		
		IDataView dataView;
		{
			String tableName = (String) context.getPlatformInput(Param_TableName);
			String tableScope = (String) context.getPlatformInput(Param_TableScope);
			ContextVariableType scope = getSourceEntityType(tableScope);
			Object dv =  context.getVObject().getContextObject(tableName, scope);
			/*
			 * if ("ruleSetInput".equals(tableScope)) { dataView = (DataView)
			 * RuleSetVariableUtil.getInputVariable(context, tableName); } else if
			 * ("ruleSetVar".equals(tableScope)) { // 上下文变量获取 dataView = (DataView)
			 * RuleSetVariableUtil.getContextVariable(context, tableName); } else if
			 * ("ruleSetOutput".equals(tableScope)) { // 输出变量 dataView = (DataView)
			 * RuleSetVariableUtil.getOutputVariable(context, tableName); }
			 */

			if (null == dv || !(dv instanceof IDataView)) {
				throw new ConfigException("找不到需要新增记录的后台实体变量" + tableName);
	//			throw new BusinessException("找不到需要新增记录的后台实体变量" + tableName);
			}
			dataView =(IDataView)dv;
		}

		for (int i = 0; i < count; i++) {
			IDataObject dataObject = dataView.insertDataObject();
			// if(mappings!=null){ //前面已经检查了
			for (Map<String, Object> mapping : mappings) {
				String field = (String) mapping.get("destField");
				String source = (String) mapping.get("srcField");
				// 目前只有表达式
				Object fieldValue = formulaEngine.eval(source);
				//
				String[] _fieldItems = field.split("\\.");
				if (_fieldItems.length > 1) {
					field = _fieldItems[_fieldItems.length - 1];
				}

				dataObject.set(field, fieldValue);
			}
			// }

		} 
		return context.newOutputVo();
	}

	private ContextVariableType getSourceEntityType(String sourceEntityType) {
		/*
		 * if (Key_ruleSetVar.equals(sourceEntityType)) { sourceDataView = (IDataView)
		 * RuleSetVariableUtil.getContextVariable(context, sourceDataViewName); } else
		 * if (Key_ruleSetOutput.equals(sourceEntityType)) { sourceDataView =
		 * (IDataView) RuleSetVariableUtil.getOutputVariable(context,
		 * sourceDataViewName); } else if (Key_ruleSetInput.equals(sourceEntityType)) {
		 * sourceDataView = (IDataView) RuleSetVariableUtil.getInputVariable(context,
		 * sourceDataViewName); }
		 */
		ContextVariableType rs = null;
		if (Key_ruleSetVar.equals(sourceEntityType)) {
			// sourceDataView = (IDataView) RuleSetVariableUtil.getContextVariable(context,
			// sourceDataViewName);
			rs = ContextVariableType.RuleSetVar;
		} else if (Key_ruleSetOutput.equals(sourceEntityType)) {
			// sourceDataView = (IDataView) RuleSetVariableUtil.getOutputVariable(context,
			// sourceDataViewName);
			rs = ContextVariableType.RuleSetOutput;
		} else if (Key_ruleSetInput.equals(sourceEntityType)) {
			// sourceDataView = (IDataView) RuleSetVariableUtil.getInputVariable(context,
			// sourceDataViewName);
			rs = ContextVariableType.RuleSetInput;
		} else {
			throw new ConfigException(D_RULE_DESC + "不支持的赋值类型:" + rs);
		}
		return rs;
	}

	public boolean isNum(String str) {
		return str.matches("^[+]?(([0-9]+)([.]([0-9]+))?|([.]([0-9]+))?)$");
	}
}