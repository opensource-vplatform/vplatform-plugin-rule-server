package com.toone.itop.formula.rule;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yindangu.v3.business.VDS;
import com.yindangu.v3.business.formula.api.IFormulaEngine;
import com.yindangu.v3.business.jdbc.api.model.ColumnType;
import com.yindangu.v3.business.jdbc.api.model.IColumn;
import com.yindangu.v3.business.jdbc.api.model.IDataSetMetaData;
import com.yindangu.v3.business.metadata.api.IDAS;
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
import com.yindangu.v3.business.rule.api.parse.IConditionParse;
import com.yindangu.v3.business.rule.api.parse.ISQLBuf;
import com.yindangu.v3.business.ruleset.api.IRuleSetExecutor;
import com.yindangu.v3.business.ruleset.api.IRuleSetExecutorWithEPImplCode;
import com.yindangu.v3.business.ruleset.api.IRuleSetQuery;
import com.yindangu.v3.business.ruleset.api.factory.IRemoteRuleSetService;
import com.yindangu.v3.business.ruleset.api.factory.IRuleSetService;
import com.yindangu.v3.business.ruleset.api.model.IRuleSet;
import com.yindangu.v3.business.ruleset.api.model.IRuleSetVariable;
import com.yindangu.v3.business.ruleset.api.model.IRuleSetVariableColumn;
import com.yindangu.v3.business.ruleset.api.model.constants.RuleSetInterfaceType;
import com.yindangu.v3.business.ruleset.api.model.constants.RuleSetVariableScopeType;
import com.yindangu.v3.business.ruleset.api.model.result.IRuleSetResult;
import com.yindangu.v3.business.ruleset.api.model.result.IRuleSetResultItem;
import com.yindangu.v3.business.ruleset.apiserver.async.IRuleSetAsyncExecuteParam;
import com.yindangu.v3.business.ruleset.apiserver.async.IRuleSetAsyncExecutedHandler;
import com.yindangu.v3.business.ruleset.apiserver.remote.IRemoteVServerAddress;
import com.yindangu.v3.business.vcomponent.manager.api.IComponentManager;
import com.yindangu.v3.business.vcomponent.manager.api.IMetaCodeVo;
import com.yindangu.v3.business.vcomponent.manager.api.component.ComponentMetaType;
import com.yindangu.v3.business.vcomponent.manager.api.component.IExtension;
import com.yindangu.v3.business.vcomponent.manager.api.product.ISpiConfigVo;
import com.yindangu.v3.business.vcomponent.manager.api.product.ISpiRuleSetVariable;
import com.yindangu.v3.platform.plugin.util.VdsUtils;

/**
 * 执行活动集
 * 
 * @author Bee
 * 
 */
public class ExecuteRuleSet  implements IRule{//extends AbstractRule {

	private static final Logger logger = LoggerFactory.getLogger(ExecuteRuleSet.class);
	public static final String D_RULE_NAME = "执行活动集";
	public static final String D_RULE_CODE = "ExecuteRuleSet";
	public static final String D_RULE_DESC = "";
	
	private static final String Param_InvokeTarget = "invokeTarget";
	private static final String Param_InvokeRuleSetCode = "ruleSetCode";
	private static final String Param_InvokeComponentCode = "componentCode";
	private static final String Param_EpConditionParams = "epConditionParam";
	private static final String Param_InvokeParams = "invokeParams";
	private static final String Param_ReturnMapping = "returnMapping";
	private static final String Param_RemoteUrl = "remoteUrl";
	protected static final String Param_EventContext = "EventContext";
	private static final String Param_IsParallelism = "isParallelism";
	private static final String Param_ExecuteType = "executeType";
	 

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
		//IRuleConfigVo ruleConfig = context.getConfigVo();//context.getRuleConfig();
		//Map<String, Object> inputParams = context.getInputParams();

		// 当前规则的实例编号，用于设置兄弟规则链输出信息
		
		
		InvokeTargetVo invokeVo = getInvokeTargetVo(context,(Map<String, Object>) context.getPlatformInput(Param_InvokeTarget)); 
		
		//boolean isParallelism = invokeVo.isParallelism();
		
		
		// 活动集调用入参信息
		//List<Map<String, Object>> invokeParams = (List<Map<String, Object>>) ruleConfig.getConfigParamValue(Param_InvokeParams);
		List<Map<String, Object>> invokeParams = (List<Map<String, Object>>) context.getPlatformInput(Param_InvokeParams);

		// 活动集返回信息设置
		//List<Map<String, Object>> returnMappings = (List<Map<String, Object>>) ruleConfig.getConfigParamValue(Param_ReturnMapping);
		List<Map<String, Object>> returnMappings = (List<Map<String, Object>>) context.getPlatformInput(Param_ReturnMapping);

		Execute ins = new Execute();
		return ins.execute(context,invokeVo,invokeParams,returnMappings);
		
	}
	
	private Map<String,Object> parseEpConditionParams(List<Map<String,Object>> epConditionParams){
		if(CollectionUtils.isEmpty(epConditionParams)){
			return null;
		}
		IFormulaEngine en = VDS.getIntance().getFormulaEngine();
		Map<String,Object> datas = new HashMap<String, Object>();
		for (int i = 0; i < epConditionParams.size(); i++) {
			Map<String,Object> params = epConditionParams.get(i);
			String code = (String)params.get("paramCode");
			String paramValue = (String)params.get("paramValue");
			Object tmpValue = null;
			if(!VdsUtils.string.isEmpty(paramValue)){
				tmpValue = en.eval(paramValue);
			}
			datas.put(code, tmpValue);
		}
		return datas;
		
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
