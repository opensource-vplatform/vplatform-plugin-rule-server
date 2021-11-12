package com.toone.itop.formula.rule;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yindangu.v3.business.VDS;
import com.yindangu.v3.business.annotation.api.model.ITriggerResult;
import com.yindangu.v3.business.plugin.business.api.rule.ContextVariableType;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleContext;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleOutputVo;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleVObject;
import com.yindangu.v3.business.plugin.execptions.ConfigException;
import com.yindangu.v3.business.ruleset.api.model.IRuleSet;
import com.yindangu.v3.platform.plugin.util.VdsUtils;

/**
 * 执行注解触发器
 * @author tangjy
 *
 */
class ExecuteAnnotaionTrigger {
	
	private static final Logger logger = LoggerFactory.getLogger(ExecuteAnnotaionTrigger.class);
	
	@SuppressWarnings("unchecked")
	protected IRuleOutputVo execute(
			IRuleContext context,
			InvokeTargetVo invokeVo,
			List<Map<String, Object>> invokeParams,
			final List<Map<String, Object>> returnMappings) {

		ParamParser parser = new ParamParser();
		IRuleOutputVo newOutputVo = context.newOutputVo();
		
		// 查找RuleSet对象
		IRuleSet ruleSet = parser.getRuleSet(invokeVo);
		// 如果RuleSet对象为空则抛异常
		if (null == ruleSet) {
			String s = "调用构件" + invokeVo.getComponentCode() + "的方法标签触发器" + invokeVo.getRuleSetCode() + "失败，触发器不存在，请检查触发器是否已经部署";
			throw new ConfigException(s);
		}
		
		final Map<String, Object> triggerInputParams = parser.initRuleSetInputParams(context, ruleSet, invokeParams, invokeVo.getRemoteUrl());
		final String componentCode = invokeVo.getComponentCode();
		final String ruleSetCode = invokeVo.getRuleSetCode();
		Map<String, Object> invokeTarget = (Map<String, Object>) context.getPlatformInput(ExecuteRuleSet.Param_InvokeTarget);
		final String windowCode = (String) invokeTarget.get(ExecuteRuleSet.Param_WindowCode);
		final String metaType = (String) invokeTarget.get(ExecuteRuleSet.Param_SourceType);
		
		// 异步执行
		if (invokeVo.isParallelism()) {
			// 平台线程池尚未封装，这里直接new线程执行
			new Thread(new Runnable() {
				@Override
				public void run() {
					VDS.getIntance().getAnnotationEngine().exeAnnotationTrigger(componentCode, ruleSetCode, windowCode, metaType, triggerInputParams, returnMappings);
					logger.info(
							"【方法标签】【触发器】异步执行方法标签触发器：构件编码" + componentCode 
							+ ",构件方法编码" + ruleSetCode);
				}
			}, "AnnotationTrigger_Async_Execute_Thread_" + VdsUtils.uuid.generate()).start();
		}else {
			ITriggerResult exeAnnotationTrigger = VDS.getIntance().getAnnotationEngine().exeAnnotationTrigger(componentCode, ruleSetCode, windowCode, metaType, triggerInputParams, returnMappings);
			logger.info(
					"【方法标签】【触发器】同步执行方法标签触发器：构件编码" + componentCode 
					+ ",构件方法编码" + ruleSetCode);
			Map<String, Object> userResult = (Map<String, Object>) exeAnnotationTrigger.get("result");
			if(userResult != null) {
				for(String outPutKey : userResult.keySet()) {
//					newOutputVo.put(outPutKey, userResult.get(outPutKey));
					setRuleOutput(context, outPutKey, userResult.get(outPutKey));
				}
			}
		}
		
		return newOutputVo;
	}
	
    @SuppressWarnings("deprecation")
	private void setRuleOutput(IRuleContext context, String destName, Object generateStr) {
        if (!(destName == null || destName.equals(""))) {
            IRuleVObject ruleVObject = context.getVObject();
            ContextVariableType targetEntityType = ContextVariableType.RuleSetOutput;
            ruleVObject.setContextObject(targetEntityType, destName, generateStr);
        }
    }
	
}
