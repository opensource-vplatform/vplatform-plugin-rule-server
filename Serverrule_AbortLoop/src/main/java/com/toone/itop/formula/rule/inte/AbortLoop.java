package com.toone.itop.formula.rule.inte;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yindangu.v3.business.VDS;
import com.yindangu.v3.business.formula.api.IFormulaEngine;
import com.yindangu.v3.business.plugin.business.api.rule.IRule;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleContext;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleOutputVo;
import com.yindangu.v3.business.plugin.execptions.BusinessException;
/***
 * 中断循环
 * @author jiqj
 *
 */
public class AbortLoop implements IRule  {
	private static final Logger log = LoggerFactory.getLogger(AbortLoop.class); 
	public static final String D_RULE_NAME = "中断循环";
	public static final String D_RULE_CODE = "AbortLoop";
	public static final String D_RULE_DESC = "在循环体内部使用，用于中断最近的封闭循环体（break）或中断最近的封闭循环体的一次迭代，开始新的迭代（continue）。\r\n" + 
			"方法名：" + D_RULE_CODE;
	private static enum AbortLoopType{
		CONTINUE,BREAK;
	}
	private AbortLoopType getAbortLoopType(String key) {
		if("continue".equalsIgnoreCase(key) ){
			return AbortLoopType.CONTINUE;
		}
		else if("break".equalsIgnoreCase(key) ){
			return AbortLoopType.BREAK;
		}
		else {
			return null;
		}
	}
	@Override
	public IRuleOutputVo evaluate(IRuleContext context) {
		//Map<String, Object> inParams = (Map<String, Object>) context.getRuleConfig().getConfigParams();
		String key = (String)context.getPlatformInput("abortType");
		AbortLoopType type = getAbortLoopType(key);
		
		if(AbortLoopType.CONTINUE.equals(type)){
			context.setForEachContinue(true); 
		}
		else if(type.equals(AbortLoopType.BREAK)){
			context.setForEachBreak(true); 
		}
		else {
			throw new BusinessException("循环中断的类型不正确:" + key + "，请检查！");
		}
		return context.newOutputVo();
	}
}
