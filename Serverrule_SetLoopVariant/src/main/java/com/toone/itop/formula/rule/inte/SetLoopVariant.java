package com.toone.itop.formula.rule.inte;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yindangu.v3.business.VDS;
import com.yindangu.v3.business.formula.api.IEvalFormulaInterceptor;
import com.yindangu.v3.business.formula.api.IFormulaEngine;
import com.yindangu.v3.business.formula.spi.IEvalFormulaCallback;
import com.yindangu.v3.business.metadata.api.IDataObject;
import com.yindangu.v3.business.plugin.business.api.rule.IRule;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleContext;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleOutputVo;
import com.yindangu.v3.business.plugin.execptions.ConfigException;
/**
 * 后台规则：给循环变量赋值
 * 
 * @author dengb
 * 
 */
public class SetLoopVariant implements IRule  {
	private static final Logger log = LoggerFactory.getLogger(SetLoopVariant.class); 
	public static final String D_RULE_NAME = "给循环变量赋值";
	public static final String D_RULE_CODE = "SetLoopVariant";
	public static final String D_RULE_DESC = "给循环变量字段赋值，允许选择循环体内所有循环变量字段；\r\n" + 
			"支持的实体类型：方法输入实体、方法输出实体、方法变量实体。\r\n" + 
			"方法名：" + D_RULE_CODE;
	private static final String D_LoopVar = "LoopVar";
	private static final String D_Source = "Source",D_LoopVarField="LoopVarField";
	
	@Override
	public IRuleOutputVo evaluate(IRuleContext context) {
		//RuleConfig ruleConfig = context.getRuleConfig();
		//Map<String, Object> ruleCfgParams = (Map<String, Object>)ruleConfig.getConfigParams();
		@SuppressWarnings({ "unchecked" })
		final List<Map<String, Object>> fields=(List<Map<String, Object>>)context.getPlatformInput("Fields");
		final String varName = (String)context.getPlatformInput(D_LoopVar);
		IEvalFormulaInterceptor interceptor = VDS.getIntance().getFormulaInterceptor();
		IEvalFormulaCallback fn = new FormulaCallbackImpl(varName, fields);
		interceptor.execute(fn,null);

		return context.newOutputVo();
	}

	private class FormulaCallbackImpl implements IEvalFormulaCallback{
		private String variableName;
		private List<Map<String, Object>> fields;
		private FormulaCallbackImpl(String varName,List<Map<String, Object>> fds) {
			variableName = varName;
			fields = fds;
		}
		@Override
		public void doInEvalFormula() {
			IFormulaEngine engine = VDS.getIntance().getFormulaEngine();
			Object obj =engine.eval("LV." + variableName);
			if (obj != null && obj instanceof IDataObject) {
				IDataObject dataObject = (IDataObject) obj;
				//	这里操作dataObject
				for(int i=0;i<fields.size();i++){
					Map<String, Object> field = fields.get(i);
					Object srcValue = engine.eval((String) field.get(D_Source));
					String fieldCode = (String) field.get(D_LoopVarField);
					dataObject.set(fieldCode,srcValue);
				}
			}else{
				String msg =(obj==null ?"值为空" : "类型不匹配,要求 IDataObject(yindangu),实际返回(" + obj.getClass().getName() + ")");
				throw new ConfigException(variableName+"-循环变量:" + msg);
			}
		}
	
	}
}
