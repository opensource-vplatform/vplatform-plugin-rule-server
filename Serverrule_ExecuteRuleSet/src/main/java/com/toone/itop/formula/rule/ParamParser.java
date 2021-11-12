package com.toone.itop.formula.rule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.toone.itop.formula.rule.ExecuteRuleSet;
import com.yindangu.v3.business.VDS;
import com.yindangu.v3.business.formula.api.IFormulaEngine;
import com.yindangu.v3.business.jdbc.api.model.ColumnType;
import com.yindangu.v3.business.jdbc.api.model.IDataSetMetaData;
import com.yindangu.v3.business.metadata.api.IDAS;
import com.yindangu.v3.business.metadata.api.IDataView;
import com.yindangu.v3.business.plugin.business.api.rule.ContextVariableType;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleContext;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleVObject;
import com.yindangu.v3.business.plugin.execptions.BusinessException;
import com.yindangu.v3.business.plugin.execptions.ConfigException;
import com.yindangu.v3.business.ruleset.api.IRuleSetQuery;
import com.yindangu.v3.business.ruleset.api.factory.IRemoteRuleSetService;
import com.yindangu.v3.business.ruleset.api.model.IRuleSet;
import com.yindangu.v3.business.ruleset.api.model.IRuleSetVariable;
import com.yindangu.v3.business.ruleset.api.model.IRuleSetVariableColumn;
import com.yindangu.v3.business.ruleset.api.model.constants.RuleSetInterfaceType;
import com.yindangu.v3.business.ruleset.api.model.constants.RuleSetVariableScopeType;
import com.yindangu.v3.business.ruleset.apiserver.remote.IRemoteVServerAddress;
import com.yindangu.v3.business.vcomponent.manager.api.IComponentManager;
import com.yindangu.v3.business.vcomponent.manager.api.IMetaCodeVo;
import com.yindangu.v3.business.vcomponent.manager.api.component.ComponentMetaType;
import com.yindangu.v3.business.vcomponent.manager.api.product.ISpiConfigVo;
import com.yindangu.v3.business.vcomponent.manager.api.product.ISpiRuleSetVariable;
import com.yindangu.v3.platform.plugin.util.VdsUtils;

public class ParamParser {
	private final Logger logger = LoggerFactory.getLogger(ParamParser.class);
	
	/**这里只是记录引用的实体名字以便校验抛异常*/
	private List<IDataView> refdataViews = new ArrayList<IDataView>();;
	
	@SuppressWarnings("deprecation")
	public Map<String, Object> initRuleSetInputParams(IRuleContext context, IRuleSet ruleSet,
			List<Map<String, Object>> invokeParams, String remoteUrl ) {

		long startTime = System.currentTimeMillis();

		String componentCode = ruleSet.getComponentCode();
		String ruleSetCode = ruleSet.getMetaCode();

		// 解析调用目标活动集的输入参数
		Map<String, Object> ruleSetInputParams = createRuleSetInputParams(context, ruleSet, invokeParams);
		// 如果是spi的情况下，需要查找configDatas中的配置，如果有则要替换原输入参数
		if (VdsUtils.string.isEmpty(remoteUrl)) {
			RuleSetInterfaceType t = VDS.getIntance().getRuleSetService().getRuleSetInterfaceType(componentCode, ruleSetCode);
			if (t ==RuleSetInterfaceType.Spi ) {
				handleRuleSetInputParams(ruleSet, ruleSetInputParams);
			}
		}

		// TODO liangmf 2015-06-09 由于目前流程事件不能穿透执行活动集，暂时加一个使用
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
	 * 创建活动集输入参数
	 * 
	 * @param invokeParams
	 * @return
	 */
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
			}

			inputParam.put(paramCode, paramValue);
		}
		return inputParam;
	}
	
	private boolean isReferenceValue(String dataPassType) {
		return (dataPassType!=null && "byReference".equals(dataPassType)); //
	}
	
	@SuppressWarnings({ "unchecked", "deprecation" })
	private IDataView createRuleSetInputEntity(IRuleVObject vobject,String paramCode,IRuleSet ruleSet
			,Map<String, Object> invokeParam,String paramSource,boolean isReference) {
		//2020-09-22 taoyz 增加实体参数的传递方式dataPassType：按值传递(byValue)或按引用传递(byReference)
		//按值传递：逻辑跟以前一样复制出实体作为参数。
		//按引用传递：逻辑改为把源实体直接传递作为参数。
		IRuleSetVariable variable = ruleSet.getRuleSetVariableByName(RuleSetVariableScopeType.InputVariable,paramCode);
		if (null == variable) {
			throw new ConfigException( "未能对构件" + ruleSet.getComponentCode() + "的后台活动集" + ruleSet.getMetaCode()
					+ "的实体变量" + paramCode + "赋值，因为活动集信息中找不到该实体变量的定义信息，请检查活动集是否已经部署");
		}

		List<IRuleSetVariableColumn> columns = variable.getColumns();
		if (CollectionUtils.isEmpty(columns)) {
			throw new ConfigException("未能对构件" + ruleSet.getComponentCode() + "的后台活动集" + ruleSet.getMetaCode()
					+ "的实体变量" + paramCode + "赋值，该实体变量未定义字段信息，请检查活动集是否已经部署");
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
		sourceDataView.appendByreferece(referName,true);
		sourceDataView.setEdit(false);
		addRefdataViews(sourceDataView);
		return sourceDataView;
	}
	
	private void addRefdataViews(IDataView dv) {
		refdataViews.add(dv);
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
	public IRuleSet getRuleSet(InvokeTargetVo invokeVo) {
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
	 * 生成远程调用地址信息
	 * 
	 * @param remoteUrl
	 * @return
	 */
	private IRemoteVServerAddress getRemoteAddress(String remoteUrl) {
		IRemoteVServerAddress address = VDS.getIntance().getRemoteRuleSetService().getRemoteAddress(remoteUrl);
		return address;
	}
}
