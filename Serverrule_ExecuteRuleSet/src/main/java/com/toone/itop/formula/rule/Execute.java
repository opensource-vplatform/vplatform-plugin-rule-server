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
import com.yindangu.v3.business.plugin.business.api.rule.IRuleContext;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleOutputVo;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleVObject;
import com.yindangu.v3.business.plugin.execptions.BusinessException;
import com.yindangu.v3.business.plugin.execptions.ConfigException;
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

/** 执行实例 ExecuteRuleSet */ 
class Execute{
	private static final Logger logger = LoggerFactory.getLogger(Execute.class);
	/**这里只是记录引用的实体名字以便校验抛异常*/
	private List<IDataView> refdataViews ;
	
	private void addRefdataViews(IDataView dv) {
		refdataViews.add(dv);
	}
	
	private void clearRefdataView() {
		for (IDataView byRefDataView : refdataViews) {
			byRefDataView.appendByreferece(null, false);
		}
	}
	protected IRuleOutputVo execute(IRuleContext context,InvokeTargetVo invokeVo
			,List<Map<String, Object>> invokeParams,List<Map<String, Object>> returnMappings) {
		try {
			return execute0(context, invokeVo, invokeParams, returnMappings);
		}
		catch(RuntimeException e) {
			logger.error(invokeVo.getComponentCode() + "." + invokeVo.getRuleSetCode() + "发生错误:" + e.getMessage());
			throw e;
		}
	}
	/**
	 * 
	 * @param context
	 * @param invokeVo 上下文变量
	 * @param invokeParams 活动集调用入参信息
	 * @param returnMappings 活动集返回信息设置
	 * @return
	 */
	private IRuleOutputVo execute0(IRuleContext context,InvokeTargetVo invokeVo
			,List<Map<String, Object>> invokeParams,List<Map<String, Object>> returnMappings) {
		long start=System.currentTimeMillis();  
		
		// 查找RuleSet对象
		// 注释掉旧方法
		// RuleSet ruleSet = findRuleSetOld(remoteUrl, componentCode,ruleSetCode);
		IRuleSet ruleSet = getRuleSet(invokeVo);
		// 如果RuleSet对象为空则抛异常
		if (null == ruleSet) {
			String s ="调用构件" + invokeVo.getComponentCode() + "的后台活动集" + invokeVo.getRuleSetCode() + "失败，活动集不存在，请检查活动集是否已经部署";
			throw new ConfigException( s);
		}
		long start2=System.currentTimeMillis(); 
		
		// 按照以前的逻辑找到具体的扩展点实现进行执行
		
		
//		long start3=System.currentTimeMillis();
//		List<Map<String, Object>> epConditionParams =(List<Map<String, Object>>)ruleConfig.getConfigParamValue(Param_EpConditionParams);
//		long end3=System.currentTimeMillis()-start3;
//		long start4=System.currentTimeMillis();
//		Map<String,Object> epConditionDatas = parseEpConditionParams(epConditionParams);

//		long start5=System.currentTimeMillis();
//		long end4=start5-start4;
		
//		List<Map<String,Object>> codes = SpiManager.getServiceEpImpl(componentCode, ruleSetCode, epConditionDatas);
		this.refdataViews=new ArrayList<IDataView>();
		// 按照以前的逻辑找到具体的扩展点实现进行执行
        List<EpImplVo> codes = null;
        //这里liangzc说目前扩展点没有使用先注释掉提高性能
//        if(RuleSetInterfaceType.ExtensionPoint.equals(ruleSet.getInterfaceType())){
//                List<Map<String, Object>> epConditionParams =(List<Map<String, Object>>)ruleConfig.getConfigParamValue(Param_EpConditionParams);
//                Map<String,Object> epConditionDatas = parseEpConditionParams(epConditionParams);
//                codes = SpiManager.getServiceEpImpl(componentCode, ruleSetCode, epConditionDatas);
//        }
//        long start3=System.currentTimeMillis();
//		long end3=start3-start2;
        long start4=System.currentTimeMillis();
		if(null != codes){
			for (int i = 0; i < codes.size(); i++) {
				EpImplVo codeInfo = codes.get(i);
				//epImplCode = codeInfo.get("componentCode") + "." + codeInfo.get("metaCode");
				String epImplCode = codeInfo.getComponentCode() + "." + codeInfo.getMetaCode();
				codeInfo.setEpImplCode(epImplCode);
				IRuleSetResult ruleSetResult =  exeRuleSet(invokeVo,ruleSet,context,invokeParams , returnMappings,codeInfo);
				
				if (!invokeVo.isParallelism()) {
					// 处理返回信息
					handleRuleSetOutputParams(context, invokeVo.getInstanceCode(), returnMappings, ruleSetResult, codeInfo);
				}
			}
		}else{
			//Map<String,Object> epImpInfo = findEPImpl(remoteUrl, componentCode, ruleSetCode);
			EpImplVo epImpInfo = findEPImpl(invokeVo); 
			//epImplCode = (epImpInfo == null ? null : epImpInfo.getEpImplCode()) ;// epImpInfo.get("epImplCode") ;
			IRuleSetResult ruleSetResult =  
					exeRuleSet(invokeVo,ruleSet,context,invokeParams, returnMappings,epImpInfo);
			
			if (!invokeVo.isParallelism()) {
				// 处理返回信息
				handleRuleSetOutputParams(context, invokeVo.getInstanceCode(), returnMappings, ruleSetResult, epImpInfo);
			}
		}
		
		
		
		//这里只是记录引用的实体名字以便校验抛异常
		this.clearRefdataView();
		
		long endTime =System.currentTimeMillis() ;
		if(ExecuteRuleSet.isCalInOutWasteTime  ){
			logger.info(invokeVo.getComponentCode() + "." + invokeVo.getRuleSetCode() + "执行总时间="+ (endTime -start)
					+",start2-start="+(start2-start)+",start4-start2 = "+ (start4-start2)+",endTime-start4="+(endTime-start4));
		}
		return context.newOutputVo();
//		List<String> extensionImplCodes = null;
//		if (StringUtils.isNotEmpty(epImplCode)) {
//			extensionImplCodes = new ArrayList<String>(1);
//			extensionImplCodes.add(epImplCode);
//		}
//
//		String isTransactionStr = ruleSet.getTransactionType();
//		boolean isTransaction = false;
//		if (StringUtils.isNotEmpty(isTransactionStr) && Boolean.parseBoolean(isTransactionStr)) {
//			isTransaction = true;
//		}
//		// 真实的构件方法编号替换原本的编号
//		componentCode = ruleSet.getComponentCode();
//		ruleSetCode = ruleSet.getMetaCode();
//
//		// 初始化构件方法输入变量
//		Map<String, Object> ruleSetInputParams = initRuleSetInputParams(context, ruleSet, invokeParams, remoteUrl);
//
//		RuleSetResult ruleSetResult = null;
//		try {
//			// 设置执行上下文的构件编号
//			setCurrentComponentCode(componentCode);
//			// 执行活动集
//			if (isParallelism) {
//				RuleSetAsyncExecuteParam ruleSetAsyncExecuteParam = RuleSetServiceFactory
//						.getRuleSetAsyncExecuteParamGenerator().gen();
//				ruleSetAsyncExecuteParam.setAsynchronous(true);
//				ruleSetAsyncExecuteParam.setTransactional(isTransaction);
//				RuleSetAsyncExecutedHandler handler = new RuleSetAsyncExecutedHandler() {
//
//					@Override
//					public void success(RuleSetResult ruleSetResult) {
//						logger.info("执行构件方法成功,ruleSetResult=" + ruleSetResult.getRuleSetResultItemMap());
//					}
//
//					@Override
//					public void error(Throwable throwable) {
//						logger.info("执行构件方法异常，throwable=" + throwable);
//					}
//				};
//				// 注释以前调用方法
//				//RuleSetExecutor ruleSetExecutor = RuleSetServiceFactory.getRuleSetExecutor();
//				//ruleSetExecutor.asyncExecute(ruleSet, ruleSetInputParams, null, ruleSetAsyncExecuteParam, handler);
//				RuleSetServiceFactory.getRuleSetExecutorWithEPImplCode().asyncExecute(ruleSet, ruleSetInputParams,
//						null, extensionImplCodes, null, ruleSetAsyncExecuteParam, handler);
//			} else {
//				// 注释以前调用方法
//				//ruleSetResult = executeRuleSetOld(remoteUrl, componentCode, ruleSetCode, ruleSetInputParams);
//				ruleSetResult = executeRuleSet(remoteUrl, ruleSet, ruleSetInputParams, extensionImplCodes);
//			}
//
//		} finally {
//			// 清除执行上下文的构件编号
//			clearCurrentComponentCode();
//		}
//
//		if (!isParallelism) {
//			// 处理返回信息
//			handleRuleSetOutputParams(context, instanceCode, returnMappings, ruleSetResult);
//		}
		
	}
	/*
	private void exeRuleSet(String epImplCode,IRuleSet ruleSet,
			String componentCode,
			IRuleContext context,
			String ruleSetCode,
			List<Map<String, Object>> invokeParams,
			String remoteUrl,Boolean isParallelism,
			String instanceCode, 
			List<Map<String, Object>> returnMappings,
			Map<String,Object> epImpInfos,List<IDataView> byRefdataViews){
		*/
	private IRuleSetResult exeRuleSet(InvokeTargetVo invokeVo,IRuleSet ruleSet, 
			IRuleContext context, 
			List<Map<String, Object>> invokeParams,
			
			List<Map<String, Object>> returnMappings,
			EpImplVo epImpInfos ){
		long start=System.currentTimeMillis();
		List<String> extensionImplCodes = null;
		if (epImpInfos!=null && !VdsUtils.string.isEmpty(epImpInfos.getEpImplCode())) {
			extensionImplCodes = Collections.singletonList(epImpInfos.getEpImplCode()); 
		}
		boolean isTransaction = (!VdsUtils.string.isEmpty(ruleSet.getTransactionType()) && Boolean.parseBoolean(ruleSet.getTransactionType())) ;
		// 真实的构件方法编号替换原本的编号
		//String componentCode = ruleSet.getComponentCode();
		//String ruleSetCode = ruleSet.getMetaCode();

		// 初始化构件方法输入变量
		Map<String, Object> ruleSetInputParams = initRuleSetInputParams(context, ruleSet, invokeParams, invokeVo.getRemoteUrl());
 
		long  initRuleSetTime =System.currentTimeMillis();
		IRuleSetResult ruleSetResult = null;
		try {
			// 设置执行上下文的构件编号
			setCurrentComponentCode(ruleSet.getComponentCode()); 
			
			// 执行活动集
			if (invokeVo.isParallelism()) {
				IRuleSetService sf = VDS.getIntance().getRuleSetService();
				//RuleSetAsyncExecuteParam ruleSetAsyncExecuteParam = RuleSetServiceFactory.getRuleSetAsyncExecuteParamGenerator().gen();
				IRuleSetAsyncExecuteParam param = sf.getRuleSetAsyncExecuteParam();
				param.setAsynchronous(true);
				param.setTransactional(isTransaction);
				IRuleSetAsyncExecutedHandler handler = new IRuleSetAsyncExecutedHandler() {

					@Override
					public void success(IRuleSetResult ruleSetResult) {
						logger.info("执行构件方法成功,ruleSetResult=" + ruleSetResult.getRuleSetResultItemMap());
					}

					@Override
					public void error(Throwable throwable) {
						logger.error("执行构件方法异常，throwable=" + throwable,throwable);
					}
				};
				// 注释以前调用方法
				//RuleSetExecutor ruleSetExecutor = RuleSetServiceFactory.getRuleSetExecutor();
				//ruleSetExecutor.asyncExecute(ruleSet, ruleSetInputParams, null, ruleSetAsyncExecuteParam, handler);
				IRuleSetExecutorWithEPImplCode ep =	sf.getRuleSetExecutorWithEPImplCode(); 
				ep.asyncExecute(ruleSet, ruleSetInputParams,
						null, extensionImplCodes, null, param, handler);
			} else {
				// 注释以前调用方法
				//ruleSetResult = executeRuleSetOld(remoteUrl, componentCode, ruleSetCode, ruleSetInputParams);
				ruleSetResult = executeRuleSet(invokeVo.getRemoteUrl(), ruleSet, ruleSetInputParams, extensionImplCodes);
			}
			
			return ruleSetResult;

		} finally {
			// 清除执行上下文的构件编号
			clearCurrentComponentCode();
			long  endTime =System.currentTimeMillis();
			if(ExecuteRuleSet.isCalInOutWasteTime ){
				logger.info(invokeVo.getComponentCode() + "." + invokeVo.getRuleSetCode() + "执行exeRuleSet总时间="+ (endTime -start)
						+",初始化变量耗时=" +(initRuleSetTime-start)  + ",executeRuleSet耗时="+( endTime - initRuleSetTime));
			} 
		}
		/*
		if (!invokeVo.isParallelism()) {
			// 处理返回信息
			handleRuleSetOutputParams(context, instanceCode, returnMappings, ruleSetResult, epImpInfos);
		}*/

	}

	/**
	 * 初始化构件方法输入变量
	 * 
	 * @param context
	 * @param ruleSet
	 * @param invokeParams
	 * @param remoteUrl
	 * @return
	 */
	private Map<String, Object> initRuleSetInputParams(IRuleContext context, IRuleSet ruleSet,
			List<Map<String, Object>> invokeParams, String remoteUrl ) {

		long startTime = System.currentTimeMillis();

		String componentCode = ruleSet.getComponentCode();
		String ruleSetCode = ruleSet.getMetaCode();

		// 解析调用目标活动集的输入参数
		Map<String, Object> ruleSetInputParams = createRuleSetInputParams(context, ruleSet, invokeParams);
		// 如果是spi的情况下，需要查找configDatas中的配置，如果有则要替换原输入参数
		if (VdsUtils.string.isEmpty(remoteUrl)) {
			//RuleSetInterfaceType interfaceType = RuleSetInterfaceTypeService.getRuleSetInterfaceType(componentCode,ruleSetCode);
			RuleSetInterfaceType t = VDS.getIntance().getRuleSetService().getRuleSetInterfaceType(componentCode, ruleSetCode);
			if (t ==RuleSetInterfaceType.Spi ) {
				handleRuleSetInputParams(ruleSet, ruleSetInputParams);
			}
		}

		// TODO liangmf 2015-06-09 由于目前流程事件不能穿透执行活动集，暂时加一个使用
		//Object extraEvent = context.getInputParams().get(Param_EventContext);
		Object extraEvent = context.getInputParams(ExecuteRuleSet.Param_EventContext);
		if (null != extraEvent) {
			ruleSetInputParams.put(ExecuteRuleSet.Param_EventContext, extraEvent);
		}

		long endTime = System.currentTimeMillis();
		long wasteTime = endTime - startTime;
		if (ExecuteRuleSet.isCalInOutWasteTime) {
			logger.info("执行构件方法" + componentCode + "." + ruleSetCode + "的初始化RuleSet输入变量，消耗时间：【" + wasteTime + "】毫秒");
		}

		return ruleSetInputParams;
	}

	/**
	 * 兼容之前执行方法操作，找到要执行的扩展点实现
	 * 
	 * @param remoteUrl
	 * @param componentCode
	 * @param ruleSetCode
	 */
	private EpImplVo findEPImpl(InvokeTargetVo invokeVo){
	//private Map<String,Object> findEPImpl(String remoteUrl, String componentCode, String ruleSetCode) {
		if (!VdsUtils.string.isEmpty(invokeVo.getRemoteUrl())) {
			return null;
		}
		IRuleSetService sf = VDS.getIntance().getRuleSetService();
		//RuleSetInterfaceType interfaceType = RuleSetInterfaceTypeService.getRuleSetInterfaceType(componentCode,ruleSetCode);
		RuleSetInterfaceType interfaceType = sf.getRuleSetInterfaceType(invokeVo.getComponentCode(),invokeVo.getRuleSetCode());
		if (!RuleSetInterfaceType.ExtensionPoint.equals(interfaceType)) {
			return null;
		}
		 
		// 执行扩展点实现的情况下，找到扩展点的具体实现，并替换当前的componentCode和ruleSetCode信息
		// 注意：目前纯后台构件方法执行扩展点接口到扩展点实现，仅找到多个实现的第一个
		IComponentManager componentManager =  VDS.getIntance().getComponentManager();//ComponentManagerFactory.getComponentManager();
		List<IExtension> extensionLst = componentManager.findExtensionsByComponentCodeAndExtensionPointCode(invokeVo.getComponentCode(),invokeVo.getRuleSetCode());
		IExtension extension = CollectionUtils.isEmpty(extensionLst) ? null : extensionLst.get(0);
		// 如果不存在扩展点实现则暂不抛出异常
		if (null == extension) {
			return null;
		}
		
		EpImplVo epImpInfo = new EpImplVo();
		//epImpInfo = new HashMap<String, Object>();
		String epImpComponentCode = extension.getImplementation().getComponentCode();
		String epImpCode = extension.getImplementation().getMetaCode();
		epImpInfo.setComponentCode( epImpComponentCode);
		epImpInfo.setMetaCode( epImpCode);
		String epImplCode = extension.getImplementation().getComponentCode() + "."
				+ extension.getImplementation().getMetaCode();
		epImpInfo.setEpImplCode( epImplCode); 
		return epImpInfo;
	}

 
	/**
	 * 查找RuleSet
	 * 
	 * @param remoteUrl
	 * @param componentCode
	 * @param ruleSetCode
	 * @return
	private IRuleSet findRuleSet(String remoteUrl, String componentCode, String ruleSetCode) {

		long startTime = System.currentTimeMillis();

		// 查找对应的活动集对象
		IRuleSet ruleSet = getRuleSet(remoteUrl, componentCode, ruleSetCode);

		long endTime = System.currentTimeMillis();
		long wasteTime = endTime - startTime;
		if (isCalInOutWasteTime) {
			logger.info("执行构件方法" + componentCode + "." + ruleSetCode + "的查找构件方法RuleSet对象，消耗时间：【" + wasteTime + "】毫秒");
		}

		return ruleSet;
	}
	 */

	/**
	 * 查找RuleSet
	 * 
	 * @param remoteUrl
	 * @param componentCode
	 * @param ruleSetCode
	 * @return
	 */
	@SuppressWarnings("unused")
	//private IRuleSet findRuleSetOld(String remoteUrl, final String pcomponentCode,final  String pruleSetCode) {
	private IRuleSet findRuleSetOld(InvokeTargetVo invokeVo) {
		long startTime = System.currentTimeMillis();
		String components  = invokeVo.getComponentCode();
		String ruleSets = invokeVo.getRuleSetCode(); 
		
		// 获取活动集的接口类型，如果是扩展点实现的情况下还需要查找该接口的具体实现的构件编号及活动集编号
		if (VdsUtils.string.isEmpty(invokeVo.getRemoteUrl())) {
			//RuleSetInterfaceType interfaceType = RuleSetInterfaceTypeService.getRuleSetInterfaceType(componentCode,ruleSetCode);
			RuleSetInterfaceType t = VDS.getIntance().getRuleSetService().getRuleSetInterfaceType(components,ruleSets);
			if (t == RuleSetInterfaceType.ExtensionPoint) {
				// 执行扩展点实现的情况下，找到扩展点的具体实现，并替换当前的componentCode和ruleSetCode信息
				// 注意：目前纯后台构件方法执行扩展点接口到扩展点实现，仅找到多个实现的第一个
				IComponentManager m = VDS.getIntance().getComponentManager();
				List<IExtension> extensionLst = m.findExtensionsByComponentCodeAndExtensionPointCode(components, ruleSets);
				IExtension extension = CollectionUtils.isEmpty(extensionLst) ? null : extensionLst.get(0);

				if (null == extension) {
					throw new ConfigException("调用构件" + components + "的扩展点" + ruleSets + "失败，找不到对应的扩展点实现");
				}

				components = extension.getImplementation().getComponentCode();
				ruleSets = extension.getImplementation().getMetaCode();
				logger.info("新扩展点:({}.{})替换了旧实现({}.{})",components, ruleSets,invokeVo.getComponentCode() ,  invokeVo.getRuleSetCode());
			}
		}
 
		InvokeTargetVo invokeVo2 = new InvokeTargetVo();
		invokeVo2.setRemoteUrl(invokeVo.getRemoteUrl());
		invokeVo2.setComponentCodes(components);
		invokeVo2.setRuleSetCodes(ruleSets);
		
		// 查找对应的活动集对象
		IRuleSet ruleSet = getRuleSet(invokeVo2);

		long endTime = System.currentTimeMillis();
		long wasteTime = endTime - startTime;
		if (ExecuteRuleSet.isCalInOutWasteTime) {
			logger.info("执行构件方法({}.{})的查找构件方法RuleSet对象，消耗时间：【{}】毫秒",components, ruleSets,wasteTime);
		}

		return ruleSet;
	}

	/**
	 * 生成远程调用地址信息
	 * 
	 * @param remoteUrl
	 * @return
	 */
	private IRemoteVServerAddress getRemoteAddress(String remoteUrl) {
		IRemoteVServerAddress address = VDS.getIntance().getRemoteRuleSetService().getRemoteAddress(remoteUrl);
		return address;
	}

	/**
	 * 获取活动集对象，如果有远程调用地址，则查询远程活动集信息
	 * 
	 * @param remoteUrl
	 * @param componentCode
	 * @param ruleSetCode
	 * @return
	 */
	//private IRuleSet getRuleSet(String remoteUrl, String componentCode, String ruleSetCode) {
	private IRuleSet getRuleSet(InvokeTargetVo invokeVo) {
		IRuleSet ruleSet = null;
		if (VdsUtils.string.isEmpty(invokeVo.getRemoteUrl())) {
			IRuleSetQuery query = VDS.getIntance().getRuleSetService().getRuleSetQuery();
			ruleSet = query.getRuleSet(invokeVo.getComponentCode(), invokeVo.getRuleSetCode());
			//ruleSet = RuleSetServiceFactory.getRuleSetQuery().getRuleSet(componentCode, ruleSetCode);
		} else {

			IRemoteVServerAddress address = getRemoteAddress(invokeVo.getRemoteUrl());
			//IRuleSetQuery remoteQuery = RemoteRuleSetServiceFactory.getRemoteRuleSetServiceFactoryGenerator().gen(address).getRuleSetQuery();
			IRemoteRuleSetService rs = VDS.getIntance().getRemoteRuleSetService();
			IRuleSetQuery q = rs.getRuleSetQuery(address);
			ruleSet = q.getRuleSet(invokeVo.getComponentCode(), invokeVo.getRuleSetCode());
		}
		return ruleSet;
	}
	
	/**
	 * 执行活动集，如果有远程调用地址，则执行远程活动集
	 * 
	 * @param remoteUrl
	 * @param componentCode
	 * @param ruleSetCode
	 * @param ruleSetInputParams
	 * @return
	 */
	private IRuleSetResult executeRuleSet(String remoteUrl, IRuleSet ruleSet, Map<String, Object> ruleSetInputParams,
			List<String> extensionImplCodes) {
		IRuleSetResult result = null;
		if (VdsUtils.string.isEmpty(remoteUrl)) {
			//long start=System.currentTimeMillis();
			IRuleSetExecutorWithEPImplCode ep = VDS.getIntance().getRuleSetService().getRuleSetExecutorWithEPImplCode();
			List<IRuleSetResult> ruleSetResults = ep.execute(ruleSet,
					ruleSetInputParams, null, extensionImplCodes);
			/*long end=System.currentTimeMillis()-start;
			if(ExecuteRuleSet.isCalInOutWasteTime){
				logger.info("执行方法外部调用核心耗时：【"+end+"】");
			}*/
			if (CollectionUtils.isNotEmpty(ruleSetResults)) {
				result = ruleSetResults.get(0);
			}
		}
		else {
			IRemoteVServerAddress address = getRemoteAddress(remoteUrl);
			IRuleSetExecutorWithEPImplCode remoteExecutor =VDS.getIntance().getRemoteRuleSetService().getRuleSetExecutorWithEPImplCode(address);
			//IRuleSetExecutorWithEPImplCode remoteExecutor = RemoteRuleSetServiceFactory.getRemoteRuleSetServiceFactoryGenerator()
			//		.gen(address).getRuleSetExecutorWithEPImplCode();
			List<IRuleSetResult> ruleSetResults = remoteExecutor.execute(ruleSet, ruleSetInputParams, null, extensionImplCodes);
			if (CollectionUtils.isNotEmpty(ruleSetResults)) {
				result = ruleSetResults.get(0);
			}
		} 
		return result;
	}

	/**
	 * 执行活动集，如果有远程调用地址，则执行远程活动集
	 * 
	 * @param remoteUrl
	 * @param componentCode
	 * @param ruleSetCode
	 * @param ruleSetInputParams
	 * @return
	 */
	@SuppressWarnings("unused")
	//private IRuleSetResult executeRuleSetOld(String remoteUrl, String componentCode, String ruleSetCode,Map<String, Object> ruleSetInputParams) {
	private IRuleSetResult executeRuleSetOld(InvokeTargetVo invokeVo,Map<String, Object> ruleSetInputParams) {
		IRuleSetResult result = null;
		String cmp = invokeVo.getComponentCode(),rc = invokeVo.getRuleSetCode();
		
		if (VdsUtils.string.isEmpty(invokeVo.getRemoteUrl())) {
			IRuleSetExecutor exc = VDS.getIntance().getRuleSetService().getRuleSetExecutor();
			result = exc.execute(cmp, rc, ruleSetInputParams,null);
		}
		else {
			IRemoteVServerAddress address = getRemoteAddress(invokeVo.getRemoteUrl());
			//RuleSetExecutor remoteExecutor = RemoteRuleSetServiceFactory.getRemoteRuleSetServiceFactoryGenerator().gen(address).getRuleSetExecutor();
			IRuleSetExecutor exc = VDS.getIntance().getRemoteRuleSetService().getRuleSetExecutor(address);
			result = exc.execute(cmp, rc, ruleSetInputParams,null);
		} 
		return result;
	}

	/**
	 * 设置默认运行时构件编号
	 */
	private void setCurrentComponentCode(String componentCode) {
		
		if (!VdsUtils.string.isEmpty(componentCode)) {
			long start=System.currentTimeMillis();
			VDS.getIntance().getComponentContext().setComponentCode(componentCode);
			long end=System.currentTimeMillis()-start;
			if(ExecuteRuleSet.isCalInOutWasteTime&&end>0){
				logger.info("设置componentcode="+end);
			}
		}
		
	}

	/**
	 * 清除当前构件编号
	 */
	private void clearCurrentComponentCode() {
		try {
			VDS.getIntance().getComponentContext().clear(); 
		} catch (Exception e) {
			logger.warn("清除当前构件编号",e);
		}
	}	
	/**
	 * 创建活动集输入参数
	 * 
	 * @param invokeParams
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private Map<String, Object> createRuleSetInputParams(IRuleContext context, IRuleSet ruleSet,
			List<Map<String, Object>> invokeParams ) {
		Map<String, Object> inputParam = new HashMap<String, Object>();
		if (CollectionUtils.isEmpty(invokeParams)) {
			return inputParam;
		}
		IFormulaEngine engine = VDS.getIntance().getFormulaEngine();
		IRuleVObject vobject = context.getVObject();
		
		for (Map<String, Object> invokeParam : invokeParams) {
			String paramCode = (String) invokeParam.get("paramCode");
			String paramType = (String) invokeParam.get("paramType");
			String paramSource = (String) invokeParam.get("paramSource");
			Object paramValue = null;
			if ("expression".equalsIgnoreCase(paramType)) {
				String expression = (String) invokeParam.get("paramValue");
				if (!VdsUtils.string.isEmpty(expression)) {
					paramValue = engine.eval(expression);
				}
			}
			else if ("entity".equalsIgnoreCase(paramType)) { 
				boolean reference = isReferenceValue( (String) invokeParam.get("dataPassType"));
				IDataView dv = createRuleSetInputEntity(vobject,paramCode,ruleSet,invokeParam,paramSource,reference);
				paramValue = dv;
			} else {
				throw new ConfigException( "未能识别构件" + ruleSet.getComponentCode() + "的后台活动集" + ruleSet.getMetaCode()
						+ "的变量" + paramCode + "的参数类型:" + paramType + ",目前只允许expression或entity类型");
//				throw new BusinessException("未能识别构件" + ruleSet.getComponentCode() + "的后台活动集" + ruleSet.getMetaCode()
//						+ "的变量" + paramCode + "的参数类型:" + paramType + ",目前只允许expression或entity类型");
			}

			inputParam.put(paramCode, paramValue);
		}
		return inputParam;
	}
	private IDataView createRuleSetInputEntity(IRuleVObject vobject,String paramCode,IRuleSet ruleSet
			,Map<String, Object> invokeParam,String paramSource,boolean isReference) {
		//2020-09-22 taoyz 增加实体参数的传递方式dataPassType：按值传递(byValue)或按引用传递(byReference)
		//按值传递：逻辑跟以前一样复制出实体作为参数。
		//按引用传递：逻辑改为把源实体直接传递作为参数。
		IRuleSetVariable variable = ruleSet.getRuleSetVariableByName(RuleSetVariableScopeType.InputVariable,paramCode);
		if (null == variable) {
			throw new ConfigException( "未能对构件" + ruleSet.getComponentCode() + "的后台活动集" + ruleSet.getMetaCode()
					+ "的实体变量" + paramCode + "赋值，因为活动集信息中找不到该实体变量的定义信息，请检查活动集是否已经部署");
//			throw new BusinessException("未能对构件" + ruleSet.getComponentCode() + "的后台活动集" + ruleSet.getMetaCode()
//					+ "的实体变量" + paramCode + "赋值，因为活动集信息中找不到该实体变量的定义信息，请检查活动集是否已经部署");
		}

		List<IRuleSetVariableColumn> columns = variable.getColumns();
		if (CollectionUtils.isEmpty(columns)) {
			throw new ConfigException("未能对构件" + ruleSet.getComponentCode() + "的后台活动集" + ruleSet.getMetaCode()
					+ "的实体变量" + paramCode + "赋值，该实体变量未定义字段信息，请检查活动集是否已经部署");
//			throw new BusinessException("未能对构件" + ruleSet.getComponentCode() + "的后台活动集" + ruleSet.getMetaCode()
//					+ "的实体变量" + paramCode + "赋值，该实体变量未定义字段信息，请检查活动集是否已经部署");
		}
		
		String sourceName = (String) invokeParam.get("paramValue");
		
		// TODO:按照类型从对应的位置获取来源数据DataView对象
		ContextVariableType type = getContextVariableSource(paramSource);
		if(type == null){
			throw new ConfigException("未能对构件" + ruleSet.getComponentCode() + "的后台活动集" + ruleSet.getMetaCode()
				+ "的实体变量" + paramCode + "赋值，该实体变量来源类型" + paramSource
				+ "不正确，目前只支持ruleSetInput或ruleSetVar，请检查活动集是否已经部署");
			/**
			 * if ("ruleSetInput".equalsIgnoreCase(paramSource)) {
				sourceDataView = (IDataView) RuleSetVariableUtil.getInputVariable(context, sourceName);
			} else if ("ruleSetVar".equalsIgnoreCase(paramSource)) {
				sourceDataView = (IDataView) RuleSetVariableUtil.getContextVariable(context, sourceName);
			 */
		}
		IDataView sourceDataView =(IDataView)vobject.getContextObject(sourceName, type);
		if(sourceDataView == null){
			throw new ConfigException("未能对构件" + ruleSet.getComponentCode() + "的后台活动集" + ruleSet.getMetaCode()
					+ "的实体变量" + paramCode + "赋值，该实体变量来源类型(" + paramSource
					+ ")取dataView为null");
		}
		
		
		IDataView dataView;
		//if(dataPassType!=null && "byReference".equals(dataPassType)){ //按引用传递
		if(isReference) {//按引用传递
			String referName = ruleSet.getComponentCode()+"."+ruleSet.getMetaCode()+"."+sourceName;
			dataView = getParamValueReferenceDataView(referName,sourceDataView,columns);
			addRefdataViews(dataView); 
		}else{
			List<Map<String, Object>> fieldMappings = (List<Map<String, Object>>) invokeParam .get("paramFieldMapping");
			//按值传递
			dataView = getParamValueCopyDataView(paramCode,sourceDataView,columns,ruleSet,fieldMappings);
		}
		return dataView;
	}
	/**
	 * 按引用传递
	 * @param dataPassType -"byReference"
	 * @return true:是,false :否
	 */
	private boolean isReferenceValue(String dataPassType) {
		return (dataPassType!=null && "byReference".equals(dataPassType)); //
	}
	

	/**
	 * 按引用传递
	 * @param referName
	 * @param sourceDataView
	 * @param columns
	 * @return
	 */
	private IDataView  getParamValueReferenceDataView(String referName, IDataView sourceDataView,List<IRuleSetVariableColumn> columns) {
		sourceDataView.setEdit(true);
		//按引用传递
		//2020-10-26 taoyz 兼容以前老配置的处理：检查源数据源有没有包括需要输入的所有字段，没有的话，自动插入字段，但值不处理
		try{
			//这里先设置可修改保证执行方法可以做兼容处理，最后再设置不可修改
			
			IDataSetMetaData sourceMetaData = sourceDataView.getMetadata();
			for (IRuleSetVariableColumn column : columns) {
				String columnCode = column.getCode();
				if(sourceMetaData.getColumn(columnCode) == null){
					ColumnType columnType = column.getColumnType();
					int columnLength = column.getLength();
					int precision = -1;
					if (columnType == ColumnType.Number) {
						precision = column.getPrecision();
					}
					if ((columnLength >= 0)
							&& ((columnType == ColumnType.Text) || (columnType == ColumnType.LongText) || (columnType == ColumnType.Integer))) {
						String columnName = column.getChineseName();
						sourceDataView.addColumn(columnCode, columnName, columnType, precision, columnLength);
					} else if ((!(columnLength <= 0 && precision <= 0)) && (columnType == ColumnType.Number)) {
						String columnName = column.getChineseName();
						sourceDataView.addColumn(columnCode, columnName, columnType, precision, columnLength);
					} else {
						sourceDataView.addColumn(columnCode, columnType);
					}
				}
			}												
		}catch(Exception e){
			logger.warn("在源dataView自动增加字段失败。", e);
		}
		//这里只是记录引用的实体名字以便校验抛异常
		//sourceDataView.appendByreferece(ruleSet.getComponentCode()+"."+ruleSet.getMetaCode()+"."+sourceName,true);
		sourceDataView.appendByreferece(referName,true);
		sourceDataView.setEdit(false);
		addRefdataViews(sourceDataView);
		return sourceDataView;
	}
	/**
	 * 
	 * @param referName
	 * @param sourceDataView
	 * @param columns
	 * @return
	 */
	private IDataView  getParamValueCopyDataView(String parameterCode,IDataView sourceDataView,List<IRuleSetVariableColumn> columns,
			IRuleSet ruleSet,List<Map<String, Object>> fieldMappings) {
		IDataView dataView = getParamValueCopyCreateDataView(columns);
		// 初始化实体变量DataView的数据信息
		if (null == sourceDataView || sourceDataView.size() == 0 || CollectionUtils.isEmpty(fieldMappings)) {
			return dataView;
		}
		////////////////////////////////
		

		IFormulaEngine engine = VDS.getIntance().getFormulaEngine();
		//这里优化初始化参数逻辑提高性能
		Map<String, String> sourceFieldMap=new HashMap<String, String>();
		Map<String, Object> expressMap=new HashMap<String, Object>();
		
		//目标字段重复的字段
		//历史问题：由于sourceDataView.getDatas(）的sourceFieldMap使用了来源字段作为key，导致一个来源复制到多个目标字段就出问题了
		//(源a->b,源a->c,源a->d) 就变成( 源a先到 b ,然后 b再到c,b再到d)
		Map<String,List<String>> targetFieldRepeat = new HashMap<String, List<String>>();
		
		for (Map<String, Object> fieldMapping : fieldMappings) {
			String source = (String) fieldMapping.get("fieldValue");
			String sourceType = (String) fieldMapping.get("fieldValueType");
			String targetField = (String) fieldMapping.get("paramEntityField");
			Object targetValue = null;
			if ("expression".equals(sourceType)) {
				targetValue = engine.eval(source);
				expressMap.put(targetField, targetValue);
			}else if ("entityField".equals(sourceType) || "field".equals(sourceType)) {
				String firstTarget = sourceFieldMap.get(source);
				if(firstTarget!=null){//一个来源复制到多个目标字段
					List<String> tars = targetFieldRepeat.get(firstTarget);
					if(tars == null){
						tars = new ArrayList<String>();
						targetFieldRepeat.put(firstTarget, tars);
					}
					tars.add(targetField);//
				}
				else{
					sourceFieldMap.put(source, targetField);
				}
			}else {
				throw new ConfigException( "未能识别构件" + ruleSet.getComponentCode() + "的后台活动集"
						+ ruleSet.getMetaCode() + "的变量" + parameterCode + "的字段来源类型:" + sourceType
						+ ",目前只允许expression或entityField类型"); 
			}
			
		}
		long start=System.currentTimeMillis();
		List<Map<String, Object>> targetRecords = sourceDataView.getDatas(sourceFieldMap, expressMap);
		
		//目标字段重复的字段
		copyFieldRepeat(targetFieldRepeat, targetRecords);

		long end1=System.currentTimeMillis()-start;
		start=System.currentTimeMillis();
		try{
			dataView.insertDataObject(targetRecords);
		}catch (BusinessException e) {
			throw new ConfigException( "构件" + ruleSet.getComponentCode() + "的后台活动集" + ruleSet.getMetaCode()
					+ "的变量" + parameterCode+" "+ e.getMessage(),e);
//					throw new BusinessException("构件" + ruleSet.getComponentCode() + "的后台活动集" + ruleSet.getMetaCode()
//							+ "的变量" + paramCode+" "+ e.getMessage(),e);
			// TODO: handle exception
		}
		long end2=System.currentTimeMillis()-start;
		if (ExecuteRuleSet.isCalInOutWasteTime && (end1>0||end2>0)) {
			logger.info("输入变量映射="+end1+","+end2);
		}
			
		return dataView;
	}
	
	private IDataView  getParamValueCopyCreateDataView(List<IRuleSetVariableColumn> columns) {
		//IDataView dataView = IMetaDataFactory.getService().das().createDataViewByMetadata();
		IDAS das = VDS.getIntance().getDas();
		IDataView dataView = das.createDataViewByMetadata();
		// 初始化实体变量DataView的字段信息
		boolean hasID = false;
		for (IRuleSetVariableColumn column : columns) {
			String columnCode = column.getCode();
			ColumnType columnType = column.getColumnType();
			int columnLength = column.getLength();
			int precision = -1;
			if (columnType == ColumnType.Number) {
				precision = column.getPrecision();
			}
			if ((columnLength >= 0)
					&& ((columnType == ColumnType.Text) || (columnType == ColumnType.LongText) || (columnType == ColumnType.Integer))) {
				String columnName = column.getChineseName();
				dataView.addColumn(columnCode, columnName, columnType, precision, columnLength);
			} else if ((!(columnLength <= 0 && precision <= 0)) && (columnType == ColumnType.Number)) {
				String columnName = column.getChineseName();
				dataView.addColumn(columnCode, columnName, columnType, precision, columnLength);
			} else {
				dataView.addColumn(columnCode, columnType);
			}
			if (!hasID && "id".equalsIgnoreCase(column.getCode())) {
				hasID = true;
			}
		}
		if (!hasID) {
			// 补充ID字段信息
			dataView.addColumn("id", ColumnType.Text);
		}
		return dataView;
	}
		
	
	/**
	 * 目标字段重复的字段:例如(源a->b,源a->c,源a->d) 就变成( 源a先到 b ,然后 b再到c,b再到d)<br/>
	 * 历史问题：由于sourceDataView.getDatas(）的sourceFieldMap使用了来源字段作为key，导致一个来源复制到多个目标字段就出问题了
	 * @param targetFieldRepeat 复制多个
	 * @param targetDatas
	 */
	private void copyFieldRepeat(Map<String,List<String>> targetFieldRepeat,List<Map<String, Object>> targetDatas){
		for(Entry<String,List<String>> e : targetFieldRepeat.entrySet()){
			String fd = e.getKey();
			List<String> targets = e.getValue();
			copyFieldRepeat_sub(fd, targets, targetDatas);
		}
	}
	/**目标字段重复的字段 */
	private void copyFieldRepeat_sub(String source,List<String> targets,List<Map<String, Object>> targetDatas){
		if(targets == null || targets.isEmpty()){
			return ;
		}
		for(Map<String, Object> rd : targetDatas){
			Object sourceValue = rd.get(source);
			if(sourceValue != null){
				for(String tar : targets){
					rd.put(tar, sourceValue);
				}
			}
		}
	}
	
	/**
	 * 获取活动集的配置数据，并按照对应的code覆盖当前的输入参数
	 * 
	 * @param componentCode
	 * @param ruleSetCode
	 * @param inputParams
	 */
	private void handleRuleSetInputParams(IRuleSet ruleSet, Map<String, Object> inputParams) {
		//ComponentManager manager = ComponentManagerFactory.getComponentManager();
		IComponentManager manager = VDS.getIntance().getComponentManager();
		// 创建metaCode信息
		IMetaCodeVo metaCode = manager.createMetaCode();//new MetaCode();
		metaCode.setComponentCode(ruleSet.getComponentCode());
		metaCode.setMetaCode(ruleSet.getMetaCode());
		// 获取spi的配置数据
		/*IComponentSpiVo spi = manager.findComponentSpi(MetaType.ServerRuleSet, metaCode);
		if (null == spi) {
			return ;
		}
		*/
		// 获取配置数据
		ISpiConfigVo configData = manager.findSpiConfig(ComponentMetaType.ServerRuleSet, metaCode);
		// 获取配置数据的变量信息
		List<ISpiRuleSetVariable> variables =(configData == null ? null : configData.getDefaultInputValues());
		if (CollectionUtils.isEmpty(variables)) {
			return ;
		}
		for (ISpiRuleSetVariable v : variables) {
			String variableCode = v.getCode();
			Object variableInitValue = v.getInitValue();
			// 如果spi配置数据为null的情况下，则认为不用覆盖
			if (null != variableInitValue) {
				inputParams.put(variableCode, variableInitValue);
			}
		}
	}

	/**
	 * 从来源记录中获取字段值
	 * @param sourceRecord 实体记录
	 * @param fileName 字段编码
	 * @param propertys 字段属性列表
	 * @return
	 */
	private Object getTargetVale(IDataObject sourceRecord, String columnCode,Set<String> propertys){
		Object targetValue = null;
		//先判断来源记录里面有没有目标字段，如果没有，值为null。
		//处理扩展点添加实体字段，但是扩展点实现里面不添加字段会报错的问题
		if(null == propertys){
			propertys = sourceRecord.getPropertys();
		}
		if(propertys.contains(columnCode)){
			targetValue = sourceRecord.get(columnCode);
		}
		return targetValue;
	}
	/**
	 * 将对象转为布尔值
	 * 
	 * @param obj
	 * @return
	 */
	private static boolean toBooleanObj(Object obj) {
		if (obj == null) {
			return false;
		}
		else if (obj instanceof Boolean) {
			return (Boolean) obj;
		}
		else if (obj instanceof Number) {// 所有的数字型全部这样判断
			return ((Number) obj).intValue() > 0 ? true : false;
		}
		else if (obj instanceof String) {
			return Boolean.parseBoolean(obj.toString());
		}
		else {
			throw new RuntimeException("转换Boolean类型错误！目前只支持Boolean,Number,String类型");
		}
	}
	
	private IDataView getOutputDataViewByContext(String targetName,IRuleVObject vobject, ContextVariableType targetType){
		if(ContextVariableType.RuleSetVar.equals(targetType) ||  ContextVariableType.RuleSetOutput.equals(targetType)) {
			//vobject.getContextObject(key, type);
			IDataView dataView = (IDataView)vobject.getContextObject(targetName, targetType);
			if(dataView == null) {
				throw new ConfigException(targetName + "执行活动集目标实体变量赋值失败，返回目标类型" + targetType+ "不存在DataView");
			}
			
			return dataView;
		}
		else {
			throw new ConfigException(targetName + "执行活动集目标实体变量赋值失败，返回目标类型" + targetType
					+ "不正确，目前只支持类型ruleSetVariant及ruleSetOutput");
		}
		/*
		if ("ruleSetVariant".equals(targetType)) {
			//dataView = (DataView) RuleSetVariableUtil.getContextVariable(context, targetName);
		} else if ("ruleSetOutput".equals(targetType)) {
			//dataView = (DataView) RuleSetVariableUtil.getOutputVariable(context, targetName);
		}*/
		
	}
	
	private void setOutputDataView(IRuleContext context, IDataView dataView,boolean reference
			,Map<String, Object> returnMapping,IRuleSetResult ruleSetResult
			,List<Map<String, Object>> fieldMappings,EpImplVo epImpInfos
			,ContextVariableType targetType,String targetName ) {
		// 获取当前活动集的实体变量
		/*if ("ruleSetVariant".equals(targetType)) {
			dataView = (DataView) RuleSetVariableUtil.getContextVariable(context, targetName);
		} else if ("ruleSetOutput".equals(targetType)) {
			dataView = (DataView) RuleSetVariableUtil.getOutputVariable(context, targetName);
		} else {
			throw new ConfigException(ErrorCodeServerRules.notSupportVariableTypeException.code(), "执行活动集目标实体变量赋值失败，返回目标类型" + targetType
					+ "不正确，目前只支持类型ruleSetVariant及ruleSetOutput");
		}*/
		
		String sourceName = (String) returnMapping.get("src");
		
		if(!reference){
			boolean isCleanTarget = toBooleanObj(returnMapping.get("isCleanDestEntityData"));
			if (isCleanTarget) {
				// TODO 清空dataView数据
				List<IDataObject> dataObjects = dataView.select();
				for (IDataObject dataObject : dataObjects) {
					dataObject.remove();
				}
			}

			// 获取活动集输出的DataView信息
			if(sourceName.equals("#fieldEntity#")){//特殊实体名称，额外处理
				handleSpecialEntity(ruleSetResult, context, returnMapping,dataView,fieldMappings,epImpInfos);
				return ;
			}
		}
		
		// 2016-12-06 liangzc：若调用的方法没有对应的方法输出，则报错。此处为方法实体变量。
		IRuleSetResultItem item =ruleSetResult.getRuleSetResultItem(sourceName) ;
		if (item == null) {
			if(null != epImpInfos){//有ep实现的时，方法输出不存在时，暂时不报错	Task20190830062
				logger.warn("后台方法-活动集编号[" + context.getRuleChainName() + "]，规则编号["
						+ context.getConfigVo().getRuleInstanceId() + "]：无法获取返回值[" + sourceName
						+ "]，请检查所调用的方法里面是否有对应的方法输出！");
				
			}else{
				logger.error("后台方法-活动集编号[" + context.getRuleChainName() + "]，规则编号["
						+ context.getConfigVo().getRuleInstanceId() + "]：无法获取返回值[" + sourceName
						+ "]，请检查所调用的方法里面是否有对应的方法输出！");
				throw new BusinessException("活动集编号[" + context.getRuleChainName() + "]：无法获取返回值[" + sourceName
						+ "]！");
			}
			return ; 
		}
		///////////////////////////////////////////////////////

		IDataView sourceDataView = (IDataView) item.getItemValue();
		if (null != sourceDataView && sourceDataView.size() > 0) {
			//ContextVariableType targetType = getContextVariableTarget(targetType);
			if(reference){
				setOutputDataViewReference(context,dataView,targetType,sourceDataView,targetName); 
			}else{
				setOutputDataViewCopy(context,dataView,returnMapping,sourceDataView,fieldMappings, epImpInfos);
			}
		}
	}
	private void setOutputDataViewReference(IRuleContext context
			,IDataView targetDataView
			,ContextVariableType targetType
			, IDataView sourceDataView
			,String targetName) {
		///按引用传递
		//2020-10-26 taoyz 直接把源dataView，作为引用赋值给输出实体
		//dataView = sourceDataView;//不可以,不能生效
		//dataView.insertDataObject(sourceDataView.getDatas());//可以，但这样数据拷贝也不快吧
		
		//亮哥说输出的实体跟源实体要做检查：必须要字段一一对应。（例如：输出的实体多了字段，或者源实体多了字段，都不行），要走数据复制。
		//第1种情况，有可能是因为这个API方法演进了几版了之后，API的实体输出增加了字段，导致跟比当前接收输出的规则链上下文变量，输出变量实体字段多了。
		//第2种情况应该不会发生，，正常来说开发系统会限制，如果接收输出的规则链上下文变量，输出变量的字段比源头多，应该不允许配置引用的。另外API演进新版本也不会减少字段。
		//为了不影响性能，只做第一种情况的判断。
		boolean needToCopyData = false;
		Set<String> targetColumnNames = new HashSet<String>();
		try{
			//检查输出目标的列，跟源的列，是不是完全一致
			//Collection<IColumn> targetColumns =  ((IH2MetaData)dataView.getMetadata()).getNoHiddenColumns();
			//List<IColumn> sourcetColumns = ((IH2MetaData)sourceDataView.getMeta()).getNoHiddenColumns();
			Collection<IColumn> targetColumns = targetDataView.getMetadata().getColumns();
			Collection<IColumn> sourcetColumns = sourceDataView.getMetadata().getColumns();
			Set<String> sourcetColumnNames = new HashSet<String>();
			if(targetColumns!=null){
				for(IColumn vColumn : targetColumns){
					targetColumnNames.add(vColumn.getColumnName());
				}
			}
			if(sourcetColumns!=null){
				for(IColumn vColumn : sourcetColumns){
					sourcetColumnNames.add(vColumn.getColumnName());
				}
			}
			for(String sourceCol : sourcetColumnNames){
				if(!targetColumnNames.contains(sourceCol)){
					//存在源多的字段
					needToCopyData = true;
					break;
				}
			}
		}catch(Exception e){
			logger.warn("检查源实体的字段是否比目标实体的字段更多时出错", e);
		}
		
		if(needToCopyData){
			logger.info("退化为dataView数据复制：" +context.getRuleChainName());
			Map<String, String> sourceFieldMap=new HashMap<String, String>();
			Map<String, Object> expressMap=new HashMap<String, Object>();
			if(targetColumnNames!=null && !targetColumnNames.isEmpty()){
				//构造一个目标实体所有字段的一一对应的影射
				for(String targetCol : targetColumnNames){
					sourceFieldMap.put(targetCol, targetCol);
				}
			}
			//从源实体复制数据到目标实体
			List<Map<String, Object>> targetRecords = sourceDataView.getDatas(sourceFieldMap, expressMap);
			targetDataView.insertDataObject(targetRecords);
		}else{
			if(ContextVariableType.RuleSetVar.equals(targetType) ||  ContextVariableType.RuleSetOutput.equals(targetType)) {
				//直接把源dataView设置到输出变量
				context.getVObject().setContextObject(targetType, targetName, sourceDataView);
			}
			else {
				throw new ConfigException("执行活动集目标实体变量赋值失败，返回目标类型" + targetType
						+ "不正确，目前只支持类型ruleSetVariant及ruleSetOutput");
			}
			/*
			if ("ruleSetVariant".equals(targetType)) {
				RuleSetVariableUtil.setContextVariable(context, targetName, sourceDataView);
			} else if ("ruleSetOutput".equals(targetType)) {
				RuleSetVariableUtil.setOutputVariable(context, targetName, sourceDataView);
			}*/
		}
	}
	private void setOutputDataViewCopyUpdateRecord(IDataView dataView,List<IDataObject> sourceRecords ,List<Map<String, Object>> fieldMappings){
		//实体字段属性列表
		
		// 更新记录的情况下，只取来源的第一条记录，赋值到目标的第一条记录上
		// 目标DataView没有记录则创建一条
		IDataObject targetRecord = null;
		if (dataView.size() > 0) {
			targetRecord = dataView.select().get(0);
		} else {
			targetRecord = dataView.insertDataObject();
		}
		IFormulaEngine engine = VDS.getIntance().getFormulaEngine();
		
		IDataObject firstRecord = sourceRecords.get(0);
		//实体字段属性列表
		Set<String> propertys =  firstRecord.getPropertys();
		
		// 循环字段配置信息，对目标记录赋值
		for (Map<String, Object> fieldMapping : fieldMappings) {
			String source = (String) fieldMapping.get("srcValue");
			String sourceType = (String) fieldMapping.get("srcValueType");
			String targetField = (String) fieldMapping.get("destField");
			Object targetValue = null;
			if ("expression".equals(sourceType)) {
				targetValue = engine.eval(source);
			} else {
				
				targetValue = getTargetVale(firstRecord, source, propertys);//sourceRecord.get(source);
			}

			targetRecord.set(targetField, targetValue);
		}
	}
	private void setOutputDataViewCopy(IRuleContext context, IDataView dataView, 
			Map<String, Object> returnMapping,IDataView sourceDataView,List<Map<String, Object>> fieldMappings
			,EpImplVo epImpInfos) {

		//过滤排序处理逻辑提前执行，这些条件仅对来源实体有效，目标实体不应该受影响   liangzc  Task20180802101
		resultFilterByCondition(sourceDataView, returnMapping);
		String updateMode = (String) returnMapping.get("updateDestEntityMethod");
		List<IDataObject> sourceRecords = sourceDataView.select();
		if(null == sourceRecords || sourceRecords.size() < 1){
			return ;
		}
		
		if ("updateRecord".equals(updateMode)) { 
			setOutputDataViewCopyUpdateRecord(dataView,sourceRecords,fieldMappings);
		}
		else {
			setOutputDataViewCopySub(dataView,sourceRecords,fieldMappings,epImpInfos);
		}
	}
	private void setOutputDataViewCopySub(IDataView dataView, List<IDataObject> sourceRecords
			,List<Map<String, Object>> fieldMappings,EpImplVo epImpInfos) {
		IDataObject firstRecord = sourceRecords.get(0);
		//实体字段属性列表
		Set<String> propertys =  firstRecord.getPropertys();
		
		// 缓存表达式的值，只取一次
		Map<String, Object> targetExpMapping = null;
		// 默认都按照insertOrUpdateBySameId执行
//			propertys  =  sourceRecords.get(0).getPropertys();
		Map<String, IDataObject> id2Record = new LinkedHashMap<String, IDataObject>();
		List<IDataObject> noIdsRecords = new ArrayList<IDataObject>();
		for (IDataObject sourceRecord : sourceRecords) {
			if (VdsUtils.string.isEmpty(sourceRecord.getId())) {
				noIdsRecords.add(sourceRecord);
			} else {
				id2Record.put(sourceRecord.getId(), sourceRecord);
			}
		}

		// 修改记录处理
		Set<String> ids = id2Record.keySet();
		Map<String, Object> idParams = new HashMap<String, Object>();
		idParams.put("id", ids);
		List<IDataObject> updateRecords=new ArrayList<IDataObject>();
		for (IDataObject dataObject : dataView.select()) {
			if(ids.contains(dataObject.getId())){
				updateRecords.add(dataObject);
			}
		}
		IFormulaEngine engine = VDS.getIntance().getFormulaEngine();
//			List<DataObject> updateRecords = dataView.select(" id in (:id) ", idParams);
		for (IDataObject targetRecord : updateRecords) {
			String id = targetRecord.getId();
			IDataObject sourceRecord = id2Record.get(id);
			// 循环字段配置信息，对目标记录赋值
			for (Map<String, Object> fieldMapping : fieldMappings) {
				String source = (String) fieldMapping.get("srcValue");
				String sourceType = (String) fieldMapping.get("srcValueType");
				String targetField = (String) fieldMapping.get("destField");
				Object targetValue = null;
				if ("expression".equals(sourceType)) {
					if (targetExpMapping != null && targetExpMapping.containsKey(targetField)) {
						targetValue = targetExpMapping.get(targetField);
					} else {
						targetValue = engine.eval(source);
						if (targetExpMapping == null) {
							targetExpMapping = new HashMap<String, Object>();
						}
						targetExpMapping.put(targetField, targetValue);
					}
				} else {
					targetValue = getTargetVale(sourceRecord, source, propertys);//sourceRecord.get(source);
				}
				targetRecord.set(targetField, targetValue);
			}
			ids.remove(id);
		}

		// 新增记录处理
		List<Map<String, Object>> datas = new ArrayList<Map<String, Object>>();
		for (String insertId : ids) {
			IDataObject sourceRecord = id2Record.get(insertId);
			Map<String, Object> data = new HashMap<String, Object>();
			// 循环字段配置信息，对目标记录赋值
			for (Map<String, Object> fieldMapping : fieldMappings) {
				String source = (String) fieldMapping.get("srcValue");
				String sourceType = (String) fieldMapping.get("srcValueType");
				String targetField = (String) fieldMapping.get("destField");
				Object targetValue = null;
				if(source.indexOf("#")!=-1){//特殊映射
					if(source.equals("#methodCode#")){
						source = "metaCode";
					}
					if(null != epImpInfos){
						targetValue = epImpInfos.getKey(source.replaceAll("#", ""));
					}
					data.put(targetField, targetValue);
					continue;
				}
				if ("expression".equals(sourceType)) {
					// 增加缓存处理逻辑，如果两次执行表达式相同，以后就从缓存取值，如果不相同就每次执行表达式
					if (targetExpMapping != null && targetExpMapping.containsKey(targetField)) {
						boolean isCache = false;
						//兼容处理
						if(targetExpMapping.containsKey(targetField + "Cache")){
							isCache = Boolean.parseBoolean(targetExpMapping.get(targetField + "Cache")
									.toString());
						}
//							Boolean isCache = Boolean.valueOf(targetExpMapping.get(targetField + "Cache")
//									.toString());
						if (isCache) {
							targetValue = targetExpMapping.get(targetField);
						} else {
							targetValue = engine.eval(source);
							Object targetValue2 = targetExpMapping.get(targetField);
							if (targetValue != null && targetValue2 != null
									&& targetValue.equals(targetValue2)) {
								targetExpMapping.put(targetField + "Cache", true);
							}
						}
					} else {
						targetValue = engine.eval(source);
						if (targetExpMapping == null) {
							targetExpMapping = new HashMap<String, Object>();
						}
						targetExpMapping.put(targetField, targetValue);
						targetExpMapping.put(targetField + "Cache", false);
					}
				} else {
					targetValue = getTargetVale(sourceRecord, source, propertys);//sourceRecord.get(source);
				}
				data.put(targetField, targetValue);
			}
			datas.add(data);
		}
		for (IDataObject sourceRecord : noIdsRecords) {
			Map<String, Object> data = new HashMap<String, Object>();
			// 循环字段配置信息，对目标记录赋值
			for (Map<String, Object> fieldMapping : fieldMappings) {
				String source = (String) fieldMapping.get("srcValue");
				String sourceType = (String) fieldMapping.get("srcValueType");
				String targetField = (String) fieldMapping.get("destField");
				Object targetValue = null;
				if ("expression".equals(sourceType)) {
					// 增加缓存处理逻辑，如果两次执行表达式相同，以后就从缓存取值，如果不相同就每次执行表达式
					if (targetExpMapping != null && targetExpMapping.containsKey(targetField)) {
						Boolean isCache = Boolean.valueOf(targetExpMapping.get(targetField + "Cache")
								.toString());
						if (isCache) {
							targetValue = targetExpMapping.get(targetField);
						} else {
							targetValue = engine.eval(source);
							Object targetValue2 = targetExpMapping.get(targetField);
							if (targetValue != null && targetValue2 != null
									&& targetValue.equals(targetValue2)) {
								targetExpMapping.put(targetField + "Cache", true);
							}
						}
					} else {
						targetValue = engine.eval(source);
						if (targetExpMapping == null) {
							targetExpMapping = new HashMap<String, Object>();
						}
						targetExpMapping.put(targetField, targetValue);
						targetExpMapping.put(targetField + "Cache", false);
					}
				} else {
					targetValue = getTargetVale(sourceRecord, source, propertys);//sourceRecord.get(source);
				}
				data.put(targetField, targetValue);
			}
			datas.add(data);
		}
		dataView.insertDataObject(datas); 
	}
	
	/**
	 * 处理执行活动集的输出信息
	 * 
	 * @param context
	 * @param instanceCode
	 * @param returnMappings
	 * @param ruleSetResult
	 * @param epImpInfos		ep实现信息，可能为空
	 */
	@SuppressWarnings("unchecked")
	private void handleRuleSetOutputParams(IRuleContext context, String instanceCode,
			List<Map<String, Object>> returnMappings, IRuleSetResult ruleSetResult,EpImplVo epImpInfos) {

		long startTime = System.currentTimeMillis();

		if (CollectionUtils.isEmpty(returnMappings) || ruleSetResult == null) {
			return;
		}
		IRuleSet ruleSet = ruleSetResult.getRuleSet();
		// 如果没有权限执行构件方法，则抛异常退出
		if (!ruleSetResult.isPermitted()) {
			String componentCode = ruleSet.getComponentCode();
			String metaCode = ruleSet.getMetaCode();
			throw new ConfigException( "你没有权限执行该构件方法：构件编号为" + componentCode + ",方法编码为:" + metaCode);
			// throw new BusinessException("你没有权限执行该构件方法：构件编号为" + componentCode + ",方法编码为:" + metaCode);
		}
		IRuleVObject vobject = context.getVObject();
		
		for (Map<String, Object> returnMapping : returnMappings) {
			ContextVariableType targetType = getContextVariableTarget((String) returnMapping.get("destType"));
			
			
			String targetName = (String) returnMapping.get("dest");
			
			//2020-10-26 taoyz，增加输出实体参数的类型判断，传递方式dataPassType：按值传递(byValue)或按引用传递(byReference)
			String dataPassType = (String) returnMapping.get("dataPassType");
			boolean isByReference = this.isReferenceValue(dataPassType);

			List<Map<String, Object>> fieldMappings = (List<Map<String, Object>>) returnMapping.get("destFieldMapping");

			// 是否DataView的输出
			if (!CollectionUtils.isEmpty(fieldMappings) || isByReference) {
				IDataView dv= getOutputDataViewByContext(targetName,vobject, targetType);
				setOutputDataView(context, dv,isByReference,returnMapping
						,ruleSetResult,fieldMappings,epImpInfos
						,targetType,targetName);
				//
			} else {
 
				// 获取来源值
				setOutputValue( context,returnMapping
						, targetName,  ruleSetResult, epImpInfos, targetType);
			}
		}

		long endTime = System.currentTimeMillis();
		long wasteTime = endTime - startTime;
		
		if (ExecuteRuleSet.isCalInOutWasteTime && ruleSet != null) {
				logger.info("执行构件方法" + ruleSet.getComponentCode() + "." + ruleSet.getMetaCode()
						+ "的处理RuleSet输出变量，消耗时间：【" + wasteTime + "】毫秒");
		}
	}

	private void setOutputValue(IRuleContext context,Map<String, Object> returnMapping
			,String targetName, IRuleSetResult ruleSetResult,EpImplVo epImpInfos,ContextVariableType targetType) {
		String source = (String) returnMapping.get("src");
		String sourceType = (String) returnMapping.get("srcType");
		Object targetValue = null;
		if ("returnValue".equals(sourceType)) {
			// 2016-12-06 liangzc：若调用的方法没有对应的方法输出，则报错。此处为其他类型的变量
			IRuleSetResultItem item =ruleSetResult.getRuleSetResultItem(source) ;
			if (item == null) {
				if(null != epImpInfos){//有ep实现的时，方法输出不存在时，暂时不报错	Task20190830062
					logger.warn("后台方法-活动集编号[" + context.getRuleChainName() + "]，规则编号["
							+ context.getConfigVo().getRuleInstanceId() + "]：无法获取返回值[" + source
							+ "]，请检查所调用的方法里面是否有对应的方法输出！");
				}else{
					logger.error("后台方法-活动集编号[" + context.getRuleChainName() + "]，规则编号["
							+ context.getConfigVo().getRuleInstanceId() + "]无法获取返回值[" + source
							+ "]，请检查所调用的方法里面是否有对应的方法输出！");
					throw new ConfigException( "活动集编号[" + context.getRuleChainName() + "]：无法获取返回值[" + source
							+ "]！");
//					throw new BusinessException("活动集编号[" + context.getRuleChainName() + "]：无法获取返回值[" + source
//							+ "]！");
				}
				return ;
			}
			// 从活动集返回值获取
			targetValue = item.getItemValue();
		} else if ("expression".equals(sourceType)) {
			// 从表达式获取

			IFormulaEngine engine = VDS.getIntance().getFormulaEngine();
			targetValue = engine.eval(source);
		} else {
			// TODO 暂不抛异常

		}
		IRuleVObject vobject = context.getVObject(); 
		if(ContextVariableType.SystemVariant.equals(targetType) 
				|| ContextVariableType.RuleSetOutput.equals(targetType)
				|| ContextVariableType.RuleSetVar.equals(targetType) ) {
			vobject.setContextObject(targetType,targetName,targetValue);
		}
		else {
			throw new ConfigException( "未能识别构件" + ruleSetResult.getRuleSet().getComponentCode() + "的后台活动集"
					+ ruleSetResult.getRuleSet().getMetaCode() + "的返回目标" + targetName + "的类型:" + targetType);
//			throw new BusinessException("未能识别构件" + ruleSetResult.getRuleSet().getComponentCode() + "的后台活动集"
//					+ ruleSetResult.getRuleSet().getMetaCode() + "的返回目标" + targetName + "的类型:" + targetType);
		}
		/*
		// 设置目标变量
		if ("ruleSetVariant".equals(targetType)) {
			RuleSetVariableUtil.setContextVariable(context, targetName, targetValue);
		} else if ("ruleSetOutput".equals(targetType)) {
			RuleSetVariableUtil.setOutputVariable(context, targetName, targetValue);
		} else if ("systemVariant".equals(targetType)) {
			ISystemVariableManagerFactory.getService().setSystemVariableValue(targetName, targetValue);
		} */
	}
	private ContextVariableType getContextVariableTarget(String targetType) {
		ContextVariableType type = null;
		if ("ruleSetVariant".equals(targetType)) {
			type = ContextVariableType.RuleSetVar;
			//RuleSetVariableUtil.setContextVariable(context, targetName, targetValue);
		} else if ("ruleSetOutput".equals(targetType)) {
			//RuleSetVariableUtil.setOutputVariable(context, targetName, targetValue);
			type = ContextVariableType.RuleSetOutput;
		} else if ("systemVariant".equals(targetType)) {
			//ISystemVariableManagerFactory.getService().setSystemVariableValue(targetName, targetValue);
			type = ContextVariableType.SystemVariant;
		}
		return type;
	}
	private ContextVariableType getContextVariableSource(String paramSource) {
		ContextVariableType type = null;
		if("ruleSetInput".equalsIgnoreCase(paramSource)){
			type = ContextVariableType.RuleSetInput;
		}
		else if ("ruleSetVar".equalsIgnoreCase(paramSource)) {
			type = ContextVariableType.RuleSetVar;
		}
		return type;
	}
	/**
	 * 对结果进行过滤和排序
	 * 
	 * @param 未进行过滤和排序的数据源
	 * @param 映射字段和条件
	 * @return
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private IDataView resultFilterByCondition(IDataView dataView, Map<String, Object> returnMapping) {
		Object queryOrderBy = returnMapping.get("orderBy");
		if (queryOrderBy == null && returnMapping.get("filterCondition") == null)
			return dataView;
//		String src = returnMapping.get("src").toString();
		List<Map<String, Object>> fieldMappings = (List<Map<String, Object>>) returnMapping.get("destFieldMapping");
		//提前执行，不需要转换
		Object queryParamsObj = returnMapping.get("filterCondition");//TranformQueryParam(returnMapping.get("filterCondition"), fieldMappings, src);// 转换查询参数里面对应的字段映射关系
		String orderStr = "";
		if (queryOrderBy != null) {
			orderStr = getOrderbyString(queryOrderBy, fieldMappings);// 获取order
																		// by
																		// 字符串
		}
		// 初始化查询参数
		Map<String, Object> queryParams = new HashMap<String, Object>();
		List<Map> condSql = new ArrayList<Map>();
		if (queryParamsObj != null && queryParamsObj instanceof List)
			condSql = (List<Map>) queryParamsObj;
		String whereCond = "";
		if (CollectionUtils.isNotEmpty(condSql)) {
			IConditionParse parse = VDS.getIntance().getVSqlParse();
			//ISQLBuf sb = QueryConditionUtil.parseConditionsNotSupportRuleTemplate(condSql);
			ISQLBuf sb = parse.parseConditionsJson(condSql);
			whereCond = sb.getSQL();
			queryParams.putAll(sb.getParams());
		}
		List<IDataObject> dataObjects  ;// dataView.select(whereCond,
											// orderStr, queryParams);
		if (orderStr == null || orderStr.length() == 0) { 
			dataObjects = dataView.select(whereCond, queryParams);
		} else {
			dataObjects = dataView.select(whereCond, orderStr, queryParams);
		}
		Set<String> propertys = null;
		if (dataObjects != null) {
			List<Map<String, Object>> tar_datas = new ArrayList<Map<String, Object>>();
			for (IDataObject dataObject : dataObjects) {
				if(null == propertys){
					propertys = dataObject.getPropertys();
				}
				Map<String, Object> data = new HashMap<String, Object>();
				Iterator<String> it = propertys.iterator();  
				while (it.hasNext()) {  
				  String column = it.next();  
				  data.put(column, dataObject.get(column));
				}
//				// 循环字段配置信息，对目标记录赋值
//				for (Map<String, Object> fieldMapping : fieldMappings) {
//					 String source = (String) fieldMapping.get("destField");
//					//已改为提前执行，不能用目标字段
//					String srcValue = (String) fieldMapping.get("srcValue");
//					Object targetValue = null;
//					String type = (String) fieldMapping.get("srcValueType");
//					if(type.equals("expression")){
//						targetValue = FormulaEngineFactory.getFormulaEngine().eval(srcValue);
//					}else if(type.equals("field")){
//						targetValue = dataObject.get(srcValue);
//					}
//					data.put(source, targetValue);
//				}
				tar_datas.add(data);
			}
			if (tar_datas != null) {
				List<IDataObject> dataObjects1 = dataView.select();
				if (dataObjects1 != null) {
					for (IDataObject dataObject : dataObjects1) {
						dataObject.remove();
					}
				}
				dataView.insertDataObject(tar_datas);
			}
		}
		// dataView = dataView.excuteSql(??, queryParams, "", dataView);
		return dataView;
	}

	/**
	 * 获取order by 字符串
	 * 
	 * @param queryOrderBy
	 * @return
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private String getOrderbyString(Object queryOrderBy, List<Map<String, Object>> fieldMappings) {
		
		List<Map<String, Object>> orderFieldList = null;
		if (queryOrderBy == null || !(queryOrderBy instanceof List) 
				|| (orderFieldList = (List) queryOrderBy).isEmpty()) {
			return null;
		}
		
		StringBuilder orderStr = new StringBuilder();
		for (Map<String, Object> map : orderFieldList ) { 
			String fieldName = (String) map.get("field");
			String type = (String) map.get("type");
			if (type == null) {
				type = "asc";
			}
			if (orderStr.length() > 0) {
				orderStr.append( ",");
			}
			if (fieldName.indexOf(".") != -1) {
				fieldName = fieldName.substring(fieldName.lastIndexOf(".") + 1);
				//不需要判断是否存在，因为都是来源实体的字段
//						boolean flag = false;
//						for (Map<String, Object> map2 : fieldMappings) {
//							if (map2.get("destField").equals(fieldName)) {
//								fieldName = map2.get("destField").toString();
//								flag = true;
//								break;
//							}
//						}
//						if (!flag)
//							throw new BusinessException("未能找到映射字段！");
			}
			orderStr.append(fieldName ).append(" ").append(type);
		} 
		return orderStr.toString();
	}

//	/**
//	 * 转换查询参数里面对应的字段映射关系
//	 * 
//	 * @param 查询的参数
//	 * @param 映射字段
//	 * @param 目标名称
//	 * @return
//	 */
//	@SuppressWarnings({ "unchecked", "rawtypes" })
//	private List TranformQueryParam(Object queryParams, List<Map<String, Object>> fieldMappings, String src) {
//		List result = new ArrayList();
//		if (queryParams != null && queryParams instanceof List) {
//			List<Map<String, Object>> orderFieldList = (List<Map<String, Object>>) queryParams;
//			if (CollectionUtils.isNotEmpty(orderFieldList)) {
//				for (Map<String, Object> amMap : orderFieldList) {
//					Map<String, Object> map = new HashMap<String, Object>();
//					map.put("columnType", amMap.get("columnType"));
//					map.put("fieldType", amMap.get("fieldType"));
//					map.put("leftBracket", amMap.get("leftBracket"));
//					map.put("logicOperation", amMap.get("logicOperation"));
//					map.put("operation", amMap.get("operation"));
//					map.put("rightBracket", amMap.get("rightBracket"));
//					map.put("value", amMap.get("value"));
//					map.put("valueType", amMap.get("valueType"));
//					String fieldName = (String) amMap.get("field");
//					if (fieldName.indexOf(".") != -1) {
//						fieldName = fieldName.substring(fieldName.lastIndexOf(".") + 1);
//					}
//					boolean flag = false;
//					for (Map<String, Object> map2 : fieldMappings) {
//						if (map2.get("srcValue").equals(fieldName)) {
//							fieldName = map2.get("srcValue").toString();
//							flag = true;
//							break;
//						}
//					}
//					if (!flag)
//						throw new BusinessException("未能找到映射字段！");
//					map.put("field", src + "." + fieldName);
//					result.add(map);
//				}
//			}
//		}
//		return result;
//	}

	private void handleSpecialEntity(IRuleSetResult ruleSetResult, 
			IRuleContext context,
			Map<String,Object> returnMapping, 
			IDataView dataView, 
			List<Map<String, Object>> fieldMappings,
			EpImplVo epImpInfo){
		String updateMode = (String) returnMapping.get("updateDestEntityMethod");
		if ("updateRecord".equals(updateMode)) {
			//实体字段属性列表
		
			// 更新记录的情况下，只取来源的第一条记录，赋值到目标的第一条记录上
			// 目标DataView没有记录则创建一条
			IDataObject targetRecord = null;
			if (dataView.size() > 0) {
				targetRecord = dataView.select().get(0);
			} else {
				targetRecord = dataView.insertDataObject();
			}

			// 循环字段配置信息，对目标记录赋值
			for (Map<String, Object> fieldMapping : fieldMappings) {
				String source = (String) fieldMapping.get("srcValue");
				String targetField = (String) fieldMapping.get("destField");
				Object targetValue = null;
				//if(epImpInfo.containsKey(source)){
					targetValue = epImpInfo.getKey(source);//epImpInfo.get(source);
				//}
				targetRecord.set(targetField, targetValue);
			}
		} else {
			List<Map<String, Object>> datas = new ArrayList<Map<String,Object>>();
			Map<String, Object> data = new HashMap<String, Object>();
			IFormulaEngine engine = VDS.getIntance().getFormulaEngine();
			
			// 循环字段配置信息，对目标记录赋值
			for (Map<String, Object> fieldMapping : fieldMappings) {
				String source = (String) fieldMapping.get("srcValue");
				String targetField = (String) fieldMapping.get("destField");
				Object targetValue = null;
				if(fieldMapping.get("srcValueType").equals("expression")){
					targetValue = engine.eval(source);
				}else if(source.indexOf("#")!=-1){//特殊映射
					if(source.equals("#methodCode#")){
						source = "metaCode";
					}
					if(null != epImpInfo){
						targetValue = epImpInfo.getKey(source.replaceAll("#", ""));
					}
				}else{
					// 2016-12-06 liangzc：若调用的方法没有对应的方法输出，则报错。此处为其他类型的变量
					IRuleSetResultItem item = ruleSetResult.getRuleSetResultItem(source);
					if (item == null) {
						if(null != epImpInfo){//有ep实现的时，方法输出不存在时，暂时不报错	Task20190830062
							logger.warn("后台方法-活动集编号[" + context.getRuleChainName() + "]，规则编号["
									+ context.getConfigVo().getRuleInstanceId() + "]：无法获取返回值[" + source
									+ "]，请检查所调用的方法里面是否有对应的方法输出！");
							continue;
						}else{
							logger.error("后台方法-活动集编号[" + context.getRuleChainName() + "]，规则编号["
									+ context.getConfigVo().getRuleInstanceId() + "]无法获取返回值[" + source
									+ "]，请检查所调用的方法里面是否有对应的方法输出！");
							throw new BusinessException("活动集编号[" + context.getRuleChainName() + "]：无法获取返回值[" + source
									+ "]！");
						}
					}
					// 从活动集返回值获取
					targetValue = item.getItemValue();
				}
				data.put(targetField, targetValue);
			}
			datas.add(data);
			dataView.insertDataObject(datas);
		}
	}
}