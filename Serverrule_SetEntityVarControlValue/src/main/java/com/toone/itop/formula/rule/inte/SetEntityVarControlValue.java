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
import com.yindangu.v3.platform.plugin.util.VdsUtils;
/**
 * 给界面实体/控件/变量赋值
 * @author jiqj
 *
 */
public class SetEntityVarControlValue implements IRule{
    private static final Logger log = LoggerFactory.getLogger(SetEntityVarControlValue.class);
    
    public static final String D_RULE_NAME="给界面实体/控件/变量赋值";
    public static final String D_RULE_CODE="SetEntityVarControlValue";
    
    public static final String D_RULE_DESC="给目标赋值，当目标为实体类型时，需设置字段映射；\r\n" + 
    		"支持的目标类型：构件变量、控件、窗体输入/输出、实体字段、方法变量/输出。\r\n" + 
    		"方法名："+D_RULE_CODE;

	private static final String Param_InParams_FieldMap = "FieldMap";

	private static final String Key_Target = "Target";
	private static final String Key_TargetType = "TargetType";
	private static final String Key_SourceType = "SourceType";
	private static final String Key_Source = "Source",Key_SourceEntityType="SourceEntityType";
	private static final String Key_EntityFieldMapping = "entityFieldMapping";
	private static final String Key_ruleSetVar = "ruleSetVar",Key_ruleSetOutput = "ruleSetOutput",Key_ruleSetInput = "ruleSetInput";
 
	@Override
	public IRuleOutputVo evaluate(IRuleContext context) {
		//IRuleConfigVo ruleConfig = context.getConfigVo();//(开发系统配置的信息)
		List<Map<String, Object>> variables =(List<Map<String, Object>>)context.getPlatformInput(Param_InParams_FieldMap);


		if(VdsUtils.collection.isEmpty(variables)) {
			IRuleOutputVo vo = context.newOutputVo();//.setMessage("缺少配置信息").setSuccess(false);
			return vo;//没有配置
		}
		IRuleVObject vobjectContext = context.getVObject();
		// 给变量循环赋值 
		for (Map<String, Object> variable : variables) { 
			List<Map<String, Object>> entityFieldMapping = getEntityFieldMapping(variable);
			if (VdsUtils.collection.isEmpty(entityFieldMapping)) {
				setSingleVariable(vobjectContext, variable);
			}
			else {
				// 存在字段映射信息，获取来源的DataView及目标的DataView，再进行记录复制
				String sourceDataViewName = (String) variable.get(Key_Source);
				ContextVariableType sourceType = getSourceEntityType((String) variable.get(Key_SourceEntityType));
				Object sourceDataView = vobjectContext.getContextObject(sourceDataViewName, sourceType);
				
				if (null == sourceDataView || !(sourceDataView instanceof IDataView)){
					//throw new ConfigException(ErrorCodeServerRules.notSupportVariableTypeException.code(), "给变量赋值规则运行失败，无法获取来源实体" + sourceDataViewName);
					throw new ConfigException(D_RULE_DESC +  "无法获取来源实体" + sourceDataViewName);
				}
				
				String targetDataViewName = (String) variable.get(Key_Target);
				ContextVariableType targetType = getOutputType((String) variable.get(Key_TargetType));
				Object targetDataView = vobjectContext.getContextObject(targetDataViewName, targetType);
				if (null == targetDataView || !(targetDataView instanceof IDataView)){
					//throw new ConfigException(ErrorCodeServerRules.notSupportVariableTypeException.code(), "给变量赋值规则运行失败，无法获取来源实体" + sourceDataViewName);
					throw new ConfigException(D_RULE_DESC +  "无法获取目标实体" + targetDataView);
				}
				
				setEntityVariable((IDataView)sourceDataView,(IDataView)targetDataView,entityFieldMapping);
			} 
		}
		IRuleOutputVo vo = context.newOutputVo();//.setMessage("设置成功").setSuccess(true);
		return vo;
	} 
	
	@SuppressWarnings("unchecked")
	private List<Map<String, Object>> getEntityFieldMapping(Map<String,Object> configMap){
		Object fdm = configMap.get(Key_EntityFieldMapping);
		boolean hasFieldMapping = true;
		if(null != fdm) {
			if((fdm instanceof String) && VdsUtils.string.isEmpty((String) fdm)) {
				hasFieldMapping = false;
			}
		} else {
			hasFieldMapping = false;
		}
	
		if(hasFieldMapping) {
			return (List<Map<String, Object>>) fdm;
		}
		else {
			return null;
		}
	}
	/**
	 * 单值赋值
	 * @param context
	 * @param config
	 */
	private void setSingleVariable(IRuleVObject context,Map<String,Object> configMap) {
		// 变量信息
		String variableName = (String) configMap.get(Key_Target);
		ContextVariableType variableType = getOutputType((String) configMap.get(Key_TargetType));
		// 变量值来源
		String valueType = (String) configMap.get(Key_SourceType) ;
		String sourceValue = (String) configMap.get(Key_Source);
		
		// 如果不存在字段映射信息，按照补充赋值的方式进行
		Object variableValue = getVariableValue(valueType,sourceValue);
		context.setContextObject(variableType, variableName, variableValue);
	}
	
	private void setEntityVariable(IDataView sourceDataView,IDataView targetDataView,List<Map<String, Object>> entityFieldMapping) {
		IFormulaEngine engine = VDS.getIntance().getFormulaEngine();
		List<IDataObject> sourceRecords = sourceDataView.select();
		for (IDataObject sourceRecord : sourceRecords) {
			IDataObject targetRecord = targetDataView.insertDataObject();
			for (Map<String, Object> mapping : entityFieldMapping) {
				Object targetValue = null;
				String targetField = (String) mapping.get("destFieldName");
				String source = (String) mapping.get("srcValue");
				String sourceType = (String) mapping.get("srcValueType");
				if (D_Expression.equals(sourceType)) {
					targetValue = engine.eval(source);
				} else {
					targetValue = sourceRecord.get(source);
				}
				targetRecord.set(targetField, targetValue);
			}
		}
	}
	
	private static final String D_Expression="expression";
	/**
	 * 获取来源值
	 * 
	 * @param valueType
	 * @param sourceValue
	 * @return
	 */
	private Object getVariableValue(String valueType, String sourceValue) {
		
		Object expressValue = null;
		if (D_Expression.equalsIgnoreCase(valueType)) {
			IFormulaEngine engine = VDS.getIntance().getFormulaEngine();
			expressValue = engine.eval(sourceValue);
			return expressValue;
		}
		throw new ConfigException(D_RULE_DESC + "-不支持类型[" + valueType + "]的值来源.");
//		throw new ExpectedException("不支持类型[" + valueType + "]的值来源.");
	}



	/**
	 * 给界面实体/控件/变量赋值 的类型
	 * 对应开发系统的“目标类型”<br/>
	 * 只支持4种类型:(构件变量:0,活动集上下文变量:4,活动集输出变量:5,输入变量:7)
	 * @param key
	 * @return
	 */
	private ContextVariableType getOutputType(String key) {
		ContextVariableType rs = null;
		if("0".equals(key)) {
		/** 构件变量:0*/
			rs = ContextVariableType.SystemVariant; 
		}
		else if("5".equals(key)) {
			/**  活动集输出变量:5*/
			rs = ContextVariableType.RuleSetOutput ;
		}
		else if("4".equals(key)) {
			/** 活动集上下文变量:4*/
			rs = ContextVariableType.RuleSetVar ;
		}
		else if("7".equals(key)) {
			/**输入变量:7*/
			rs = ContextVariableType.RuleSetInput ;
		}
		else {
			throw new ConfigException(D_RULE_DESC +  "不支持的赋值类型:" + rs);
		}
		
		return rs; 
	}
	
	private ContextVariableType getSourceEntityType(String sourceEntityType) {
		/*if (Key_ruleSetVar.equals(sourceEntityType)) {
					sourceDataView = (IDataView) RuleSetVariableUtil.getContextVariable(context, sourceDataViewName);
				} else if (Key_ruleSetOutput.equals(sourceEntityType)) {
					sourceDataView = (IDataView) RuleSetVariableUtil.getOutputVariable(context, sourceDataViewName);
				} else if (Key_ruleSetInput.equals(sourceEntityType)) {
					sourceDataView = (IDataView) RuleSetVariableUtil.getInputVariable(context, sourceDataViewName);
				}*/
		ContextVariableType rs = null;
		if (Key_ruleSetVar.equals(sourceEntityType)) {
			//sourceDataView = (IDataView) RuleSetVariableUtil.getContextVariable(context, sourceDataViewName);
			rs = ContextVariableType.RuleSetVar;	
		} else if (Key_ruleSetOutput.equals(sourceEntityType)) {
			//sourceDataView = (IDataView) RuleSetVariableUtil.getOutputVariable(context, sourceDataViewName);
			rs = ContextVariableType.RuleSetOutput;
		} else if (Key_ruleSetInput.equals(sourceEntityType)) {
			//sourceDataView = (IDataView) RuleSetVariableUtil.getInputVariable(context, sourceDataViewName);
			rs = ContextVariableType.RuleSetInput;
		}
		else {
			throw new ConfigException(D_RULE_DESC +  "不支持的赋值类型:" + rs);
		}
		return rs;
	}

}
