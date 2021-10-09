package com.toone.itop.formula.rule.inte;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yindangu.v3.business.VDS;
import com.yindangu.v3.business.formula.api.IFormulaEngine;
import com.yindangu.v3.business.plugin.business.api.rule.IRule;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleContext;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleOutputVo;
import com.yindangu.v3.business.plugin.execptions.ConfigException;
import com.yindangu.v3.platform.plugin.util.VdsUtils;

/**
 * 执行函数/表达式
 * @deprecated 建议使用变量赋值代替
 * @author weicd
 */ 
public class ExecExpression  implements IRule{
    private static final Logger log = LoggerFactory.getLogger(ExecExpression.class);
    
    public static final String D_RULE_NAME="执行函数/表达式";
    public static final String D_RULE_CODE="ExecExpression";
    public static final String D_RULE_DESC="执行表达式，并返回值表达式的执行结果。\r\n" + 
    		"方法名："+D_RULE_CODE;
    @Override
	public IRuleOutputVo evaluate(IRuleContext context) {
		//Map<String, Object> ruleCfgParams = (Map<String, Object>) context.getRuleConfig().getConfigParams();// context.getRuleCfg().get("inParams");
		//Map<String, Object> runtimeParams = context.getInputParams();
		// 表达式
		//String expression = (String) ruleCfgParams.get("expression");
		String exp = (String)context.getPlatformInput("expression");

		if (VdsUtils.string.isEmpty(exp)) {
			throw new ConfigException("表达式不能为空!");
		}
		try {
			context.setResultToJson(false);
			// 执行表达式
			IFormulaEngine e = VDS.getIntance().getFormulaEngine();
			Object val =  e.eval(exp);
			IRuleOutputVo rs = context.newOutputVo().put(val);
			return rs;
		} catch (Exception e) {
			log.error(exp + "表达式不正确!请检查!");
			throw new ConfigException(exp + "表达式不正确!请检查!", e);
		}
		 

	}

}
