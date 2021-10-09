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
import com.yindangu.v3.business.plugin.execptions.BusinessException;
import com.yindangu.v3.business.plugin.execptions.ConfigException;
import com.yindangu.v3.business.plugin.execptions.EnviException;
import com.yindangu.v3.platform.plugin.util.VdsUtils;
 
/**
 * 中断规则
 * @author jiqj
 *
 */
public class AbortRule implements IRule{
    private static final Logger log = LoggerFactory.getLogger(AbortRule.class);
    
    public static final String D_RULE_NAME="中断规则";
    public static final String D_RULE_CODE="AbortRule";
    public static final String D_RULE_DESC="退出当前规则链：仅中断当前规则链的执行（若后续存在其他规则链，将继续执行）；\r\n" + 
    		"中断所有规则链：中断所有规则链的执行（若后续存在其他规则链，也不再执行）；客户端：不回滚事务，服务端：回滚事务。\r\n" + 
    		"方法名："+D_RULE_CODE;
     

    protected static enum AbortType{
    	/**退出当前规则链*/
    	CURRENT,
    	/**中断使用规则链*/
    	GLOBAL
    }
	private static final String Key_AbortType= "abortType";
	private static final String Key_Msgnote= "msgnote";
	/**是否显示提示值*/
	private static final String Key_AlertEnabled = "alertEnabled";
	/**
	 * 默认是当前
	 * @param type
	 * @return
	 */
	private AbortType getAbortType(String type) {
		if(VdsUtils.string.isEmpty(type) || AbortType.CURRENT.name().equalsIgnoreCase(type)) {
			return AbortType.CURRENT;
		}
		else if(AbortType.GLOBAL.name().equalsIgnoreCase(type)) {
			return AbortType.GLOBAL; 
		}
		else {
			throw new EnviException("不能识别的中断类型:" + type);
		}
	}
	@Override
	public IRuleOutputVo evaluate(IRuleContext context) {
		//Map<String, Object> inParams = (Map<String, Object>) context.getRuleConfig().getConfigParams();
		String abortTypes = (String)context.getPlatformInput(Key_AbortType);
		AbortType abortType = getAbortType(abortTypes);
		if(abortType.equals(AbortType.CURRENT)){
			context.setBreak(true);
			return context.newOutputVo();
		}
		
		String msgnoteExp = (String) context.getPlatformInput(Key_Msgnote);
		
		boolean alertEnabled = true;
		if ("false".equalsIgnoreCase(String.valueOf(context.getPlatformInput(Key_AlertEnabled)))) {
			alertEnabled = false;
		}
		String message;
		if(VdsUtils.string.isEmpty(msgnoteExp)) {
			message="中断所有规则链";
		}
		else {
			message = VDS.getIntance().getFormulaEngine().eval(msgnoteExp);
		}
		
		BusinessException exp = new BusinessException(message);
		exp.setAlertEnabled(alertEnabled);
		throw exp;
	}

}
