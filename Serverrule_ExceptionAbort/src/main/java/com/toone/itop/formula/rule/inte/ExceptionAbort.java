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
 * 异常中断
 * @author jiqj
 *
 */
public class ExceptionAbort implements IRule  {
	private static final Logger log = LoggerFactory.getLogger(ExceptionAbort.class); 
	public static final String D_RULE_NAME = "异常中断";
	public static final String D_RULE_CODE = "ExceptionAbort";
	public static final String D_RULE_DESC = "中断后续规则链执行，并返回指定业务异常。\r\n" + 
			"方法名：" + D_RULE_CODE;
	
	private static final String D_ErrorCode="errorCode",D_ErrorMsg="errorMsg";
	@Override
	public IRuleOutputVo evaluate(IRuleContext context) {
		//Map<String, Object> inParams = (Map<String, Object>) context.getRuleConfig().getConfigParams();
		//String errorCodeExp= (String)inParams.get("errorCode");
		//String errorMsgExp = (String) inParams.get("errorMsg");
		String codeExp = (String)context.getPlatformInput(D_ErrorCode);
		String msgExp = (String)context.getPlatformInput(D_ErrorMsg);
		IFormulaEngine en =  VDS.getIntance().getFormulaEngine();
		String errorCode = en.eval(codeExp);
		String errorMsg = en.eval(msgExp);
		BusinessException e = new BusinessException(errorMsg);
		e.setCode(errorCode);
		throw e;
	}
}
