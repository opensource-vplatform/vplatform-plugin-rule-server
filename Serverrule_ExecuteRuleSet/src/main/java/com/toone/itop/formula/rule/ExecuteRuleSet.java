package com.toone.itop.formula.rule;

import java.util.List;
import java.util.Map;

import com.yindangu.v3.business.VDS;
import com.yindangu.v3.business.formula.api.IFormulaEngine;
import com.yindangu.v3.business.plugin.business.api.rule.IRule;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleContext;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleOutputVo;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleVObject;
import com.yindangu.v3.business.plugin.execptions.EnviException;
import com.yindangu.v3.platform.plugin.util.VdsUtils;

/**
 * 执行活动集
 * 
 * @author Bee
 * 
 */
public class ExecuteRuleSet  implements IRule{ 

	public static final String D_RULE_NAME = "执行活动集";
	public static final String D_RULE_CODE = "ExecuteRuleSet";
	public static final String D_RULE_DESC = "";
	
	protected static final String Param_InvokeTarget = "invokeTarget";
	private static final String Param_InvokeRuleSetCode = "ruleSetCode";
	private static final String Param_InvokeComponentCode = "componentCode";
	private static final String Param_InvokeParams = "invokeParams";
	private static final String Param_ReturnMapping = "returnMapping";
	private static final String Param_RemoteUrl = "remoteUrl";
	protected static final String Param_EventContext = "EventContext";
	private static final String Param_IsParallelism = "isParallelism";
	private static final String Param_ExecuteType = "executeType";
	
	protected static final String Param_SourceType = "sourceType";
	protected static final String Param_WindowCode = "windowCode";
	 

	// 是否统计输入输出变量处理消耗时间
	protected static boolean isCalInOutWasteTime = false;

	private InvokeTargetVo getInvokeTargetVo(IRuleContext context,Map<String, Object> invokeTarget) {
		InvokeTargetVo vo = new InvokeTargetVo();// 目标构件活动集信息
		IFormulaEngine formulaEngine = VDS.getIntance().getFormulaEngine(); 
		//Map<String, Object> invokeTarget = (Map<String, Object>) context.getPlatformInput(Param_InvokeTarget);
		IRuleVObject vObject = context.getVObject();
		String instanceCode = (String) context.getPlatformInput("instanceCode");
		vo.setInstanceCode(instanceCode);
		
		String remoteUrlExpr =(String)invokeTarget.get(Param_RemoteUrl); 
		String executeType = (String)invokeTarget.get(Param_ExecuteType);
		if (executeType != null && "unconfirm".equals(executeType)) {
			String ruleSetCode = formulaEngine.eval((String) invokeTarget.get(Param_InvokeRuleSetCode));
			String componentCode =formulaEngine.eval((String) invokeTarget.get(Param_InvokeComponentCode));
			vo.setRuleSetCodes(ruleSetCode).setComponentCodes(componentCode);
		}
		else {
			vo.setRuleSetCodes(invokeTarget.get(Param_InvokeRuleSetCode))
				.setComponentCodes(invokeTarget.get(Param_InvokeComponentCode));
		}
		
		//IRuleVObject vObject = context.getVObject();
		if (VdsUtils.string.isEmpty(vo.getComponentCode())) {
			/* 复杂的逻辑 放到 getContextComponentCode里面
			 if (VdsUtils.string.isEmpty(componentCode)) {
			VDS.getIntance().getSessionManager();
			componentCode = ISessionManagerFactory.getService().getCurrentComponentCode();
		}*/
			String componentCode = vObject.getContextComponentCode();//CompContext.getCompCode();
			if (VdsUtils.string.isEmpty(componentCode)) {
				throw new EnviException("没有找到构件编码");
			}
			vo.setComponentCodes(componentCode);
		}
 
		if (!VdsUtils.string.isEmpty(remoteUrlExpr)) {
			String remoteUrl = formulaEngine.eval(remoteUrlExpr);
			vo.setRemoteUrl(remoteUrl);
		}
		
		String isParallelismStr = (String) invokeTarget.get(Param_IsParallelism);
		vo.setParallelisms(!VdsUtils.string.isEmpty(isParallelismStr) && Boolean.parseBoolean(isParallelismStr)) ;
		return vo;
	}
	@Override
	@SuppressWarnings({ "unchecked" })
	public IRuleOutputVo evaluate(IRuleContext context) {

		// 当前规则的实例编号，用于设置兄弟规则链输出信息
		
		InvokeTargetVo invokeVo = getInvokeTargetVo(context,(Map<String, Object>) context.getPlatformInput(Param_InvokeTarget)); 
		
		// 活动集调用入参信息
		List<Map<String, Object>> invokeParams = (List<Map<String, Object>>) context.getPlatformInput(Param_InvokeParams);

		// 活动集返回信息设置
		List<Map<String, Object>> returnMappings = (List<Map<String, Object>>) context.getPlatformInput(Param_ReturnMapping);

		return execute(context,invokeVo,invokeParams,returnMappings);
		
	}
	
	private IRuleOutputVo execute(IRuleContext context, InvokeTargetVo invokeVo, List<Map<String, Object>> invokeParams, List<Map<String, Object>> returnMappings) {
		// 是否注解触发器
		boolean isAnnotationTrigger = isAnnotationTrigger(context, invokeVo);
		if(isAnnotationTrigger) {
			ExecuteAnnotaionTrigger  trigger = new ExecuteAnnotaionTrigger();
			return trigger.execute(context, invokeVo, invokeParams, returnMappings);
		}else {
			Execute ins = new Execute();
			return ins.execute(context,invokeVo,invokeParams,returnMappings);
		}
	}
	
	// 是否注解触发器
	@SuppressWarnings("unchecked")
	private boolean isAnnotationTrigger(IRuleContext context, InvokeTargetVo invokeVo) {
		
		Map<String, Object> invokeTarget = (Map<String, Object>) context.getPlatformInput(Param_InvokeTarget);
		String compCode = invokeVo.getComponentCode();
		String metaCode = invokeVo.getRuleSetCode();
		String windowCode = (String) invokeTarget.get(Param_WindowCode);
		String metaType = (String) invokeTarget.get(Param_SourceType);
		
		Boolean isTrigger = VDS.getIntance().getAnnotationEngine().isTrigger(compCode, metaCode, windowCode, metaType);
		return isTrigger;
	}
	

	public static void main(String[] args) {
		if (args == null || args.length <= 0) {
			return;
		}
		String cmd = args.length > 0 ? args[0] : null;
		String param1 = args.length > 1 ? args[1] : null;
		// String param2 = args.length > 2 ? args[2] : null;

		if ("isCalInOutWasteTime".equals(cmd)) {
			if (Boolean.TRUE.toString().equalsIgnoreCase(param1)) {
				ExecuteRuleSet.isCalInOutWasteTime = true;
			} else if (Boolean.FALSE.toString().equalsIgnoreCase(param1)) {
				ExecuteRuleSet.isCalInOutWasteTime = false;
			}
		}
	}
}
