package com.toone.itop.formula.rule;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
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
	 
	private static final Logger log = LoggerFactory.getLogger(ExecuteRuleSet.class);
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
		String roleCode ="";
try {
		// 当前规则的实例编号，用于设置兄弟规则链输出信息
		
		InvokeTargetVo invokeVo = getInvokeTargetVo(context,(Map<String, Object>) context.getPlatformInput(Param_InvokeTarget)); 
		roleCode = invokeVo.getRuleSetCode();
		ExecuteRuleSetTimeVo.begin(roleCode);
		// 活动集调用入参信息
		List<Map<String, Object>> invokeParams = (List<Map<String, Object>>) context.getPlatformInput(Param_InvokeParams);

		// 活动集返回信息设置
		List<Map<String, Object>> returnMappings = (List<Map<String, Object>>) context.getPlatformInput(Param_ReturnMapping);

		return execute(context,invokeVo,invokeParams,returnMappings);
}
finally {
	
		ExecuteRuleSetTimeVo.end(roleCode);
	
}
		
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
	
 
	//main com.toone.itop.formula.rule.ExecuteRuleSet isCalInOutWasteTime true
	public static void main(String[] args) {
		if (args == null || args.length <= 0) {
			return;
		}
		String cmd = args.length > 0 ? args[0] : null;
		String param1 = args.length > 1 ? args[1] : null;
		//	String param2 = args.length > 2 ? args[2] : null;

		boolean cal = ExecuteRuleSet.isCalInOutWasteTime;
		if(param1==null && ("true".equalsIgnoreCase(cmd) || "false".equalsIgnoreCase(cmd)) ) {
			//省略第一个参数
			param1 = cmd;
			cmd ="isCalInOutWasteTime";
		}
		
		if ("isCalInOutWasteTime".equals(cmd)) {
			if (Boolean.TRUE.toString().equalsIgnoreCase(param1)) {
				ExecuteRuleSet.isCalInOutWasteTime = true;
			} else if (Boolean.FALSE.toString().equalsIgnoreCase(param1)) {
				ExecuteRuleSet.isCalInOutWasteTime = false;
			}
		}
		else if("show".equalsIgnoreCase(cmd)) {
			boolean clear = ("true".equalsIgnoreCase(param1));
			StringBuilder sb = ExecuteRuleSetTimeVo.showTimes(clear);
			log.info("函数访问时间统计:" + sb.toString());
			System.out.println("函数访问时间统计:" + sb.toString());
		}
		
		log.info("命令参数:{isCalInOutWasteTime|show} {true|false} ");
		System.out.println("isCalInOutWasteTime=" +cal + " to " + ExecuteRuleSet.isCalInOutWasteTime);
	}
}

/***方法耗时*/
class ExecuteRuleSetTimeVo implements Serializable{
	private static final long serialVersionUID = 7564048036617727215L;
	private static Map<String, ExecuteRuleSetTimeVo> cache ;
	private static int dept;
	private int count;
	private long wasteTime,minTime,maxTime;
	private long beginTime,endTime;
	static{
		cache = new ConcurrentHashMap<String, ExecuteRuleSetTimeVo>();//JCacheManagerFactory.getCacheManager().getCache("18formula_functime");
		dept=0;
	}
	protected ExecuteRuleSetTimeVo() {
		count =0;
		wasteTime = minTime = maxTime =beginTime = endTime = 0;
	}
	private static boolean checkStauts() {
		if(ExecuteRuleSet.isCalInOutWasteTime && cache.size() < 4 * 1024) {
			return true; //大于5000个元素就没有意义了，并且影响内存（需要手工clear）
		}
		else {
			return false;
		}
	}
	public static void begin(String funcName) {
		if(!checkStauts()) {
			return ;
		}
		dept ++;
		String key =  getKey(dept, funcName);
		Map<String, ExecuteRuleSetTimeVo> c = cache;
		ExecuteRuleSetTimeVo vo = c.get(key);
		if(vo == null) {
			vo = new ExecuteRuleSetTimeVo();
			c.put(key, vo);
		}
		vo.beginTime = System.currentTimeMillis();
		
	}
	public static void end(String funcName) {
		if(!checkStauts()) {
			return ;
		}
		String key = getKey(dept, funcName);
		dept --;
		
		Map<String, ExecuteRuleSetTimeVo> c = cache;
		ExecuteRuleSetTimeVo vo = c.get(key);
		if(vo == null) {
			vo = new ExecuteRuleSetTimeVo();
			c.put(key, vo);
		}
		vo.endTime = System.currentTimeMillis();
		vo.addCount(vo.endTime -vo.beginTime);
	}
	private static String getKey(int deptx,String funcName) {
		String ds = String.valueOf(deptx);
		StringBuilder sb = new StringBuilder(ds.length() + funcName.length() + 10);
		switch (ds.length()) {
		case 1:
			sb.append("00").append(ds);
			break;
		case 2:
			sb.append("0").append(ds);
			break;
		default:
			sb.append(ds);
			break;
		}
		sb.append('#').append(funcName);
		return sb.toString();
	}
	
	public static StringBuilder showTimes(boolean clear) {
		StringBuilder sb = new StringBuilder();
		if(cache == null) {
			return sb;
		}
		
		sb.append("\r\n count \t\t time \t\t avg(ms) \t\t max \t\t min \t\t funcName \r\n");
		List<String> keys = new ArrayList<String>(cache.keySet());
		Collections.sort(keys);
		
		for(String key  :keys) {
			ExecuteRuleSetTimeVo v = cache.get(key);
			sb.append(v.count).append("\t\t")
			 	.append(v.wasteTime).append("\t\t")
			 	.append(v.getAvgs()).append("\t\t")
			 	.append(v.maxTime).append("\t\t")
			 	.append(v.minTime).append("\t\t")
			 	.append(key).append("\r\n");
		}
		if(clear) {
			cache.clear();
			dept=0;
		}
		return sb;
	} 
	public void addCount(long times) {
		if(times>1000572023174l) {
			times =0;
		}
		wasteTime = wasteTime+ times;
		count ++;
		if(minTime==0 || (times>0 && times < minTime)){
			minTime =times;
		}
		if(times > maxTime) {
			maxTime =times;
		}
	}
	public int getCount() {
		return count;
	}
	public long getWasteTime() {
		return wasteTime;
	}
	public long getMinTime() {
		return minTime;
	}
	public long getMaxTime() {
		return maxTime;
	}
	private String getAvgs() {
		if(count ==0) {
			//sb.append("0.00");
			return "0.00";
		}
		StringBuilder sb = new StringBuilder(10);
		String avgs = String.valueOf(wasteTime * 100 / count);
		int len = avgs.length();
		if(len  == 1) {
			sb.append("0.0").append(avgs);
		}
		else if(len ==2 ) {
			sb.append("0.").append(avgs);
		}
		else {
			sb.append(avgs.substring(0, len-2)).append('.').append(avgs.substring(len-2));
		}
		return sb.toString();
	}
}
