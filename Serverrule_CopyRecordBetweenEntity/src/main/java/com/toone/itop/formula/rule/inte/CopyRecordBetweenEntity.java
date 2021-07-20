package com.toone.itop.formula.rule.inte;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.toone.itop.rule.apiserver.util.AbstractRule4Tree;
import com.yindangu.v3.business.VDS;
import com.yindangu.v3.business.formula.api.IFormulaEngine;
import com.yindangu.v3.business.jdbc.api.model.DataState;
import com.yindangu.v3.business.metadata.api.IDataObject;
import com.yindangu.v3.business.metadata.api.IDataView;
import com.yindangu.v3.business.plugin.business.api.rule.IRule;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleContext;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleOutputVo;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleVObject;
import com.yindangu.v3.business.plugin.execptions.ConfigException;
import com.yindangu.v3.business.rule.api.parse.IConditionParse;
import com.yindangu.v3.business.rule.api.parse.ISQLBuf;
import com.yindangu.v3.business.vsql.apiserver.IFieldMapping;
import com.yindangu.v3.business.vsql.apiserver.IParamFieldVo;
import com.yindangu.v3.business.vsql.apiserver.IVSQLConditions;
import com.yindangu.v3.business.vsql.apiserver.IVSQLQuery;
import com.yindangu.v3.business.vsql.apiserver.IVSQLQueryUpdate;
import com.yindangu.v3.business.vsql.apiserver.IVSQLQueryUpdate.OptionType;
import com.yindangu.v3.business.vsql.apiserver.IVSQLQueryUpdate.UpdateType;
import com.yindangu.v3.platform.plugin.util.VdsUtils;

 
public class CopyRecordBetweenEntity  extends AbstractRule4Tree implements IRule{

	private static final Logger			logger				= LoggerFactory.getLogger(CopyRecordBetweenEntity.class);
	public static final String D_RULE_NAME = "实体间复制记录";
	public static final String D_RULE_CODE = "CopyRecordBetweenEntity";
	public static final String D_RULE_DESC = "";
	
	private final static String				SUM					= "sum";
	private final static String				MAX					= "max";
	private final static int copyMaxCount=50000 ;

	/* 在得到最后结果才处理的表达式 */
	private static final List<String>	LASTHANDLEREXPER	= new ArrayList<String>(Arrays.asList("GetSerialNumberFunc"));

	/* sql函数表达式 */
	private static final List<String>	SQLFUNCEXPER		= new ArrayList<String>(Arrays.asList("RandomFunc", "Random", "GenerateUUID"));

	private class MappingFieldVo{
		private final List<String> mergeField;
		private final Map<String,String > mappingMap;
		private MappingFieldVo( List<String> field,Map<String,String > mp) {
			mergeField = field;
			mappingMap = mp;
		}
		public List<String> getMergeField() {
			return mergeField;
		}
		public Map<String, String> getMappingMap() {
			return mappingMap;
		}
	}
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private MappingFieldVo getMergeField(CopyEntityParamVo cpm ,List<Map> mappingItems) {
		List<String> mergeField = new ArrayList<String>() ;
		Map<String,String > mappingMap = new HashMap<>();
		// 处理合并字段到mappingItems中的isMerge(增加判断条件，操作类型为4合并时)
		if (cpm.getMageitems().size()>0 && cpm.getType() == OperType.Merge) { 
		//if (mageitems != null && mageitems instanceof List && operType.equals("4")) { 
			List<String> mageitemList = (List<String>) cpm.getMageitems();
			Map<String, String> mageitemMap = new HashMap<String, String>();
			for (String string : mageitemList) {
				mergeField.add(string);
				mageitemMap.put(string, "true");
			}
			for (Map<String,String> map : mappingItems) {
				if (mageitemMap.get(map.get("destName")) != null) {
					map.put("isMerge", "true");
				} else {
					map.put("isMerge", "false");
				}
				String key = (String) map.get("destName");
				String value = (String) map.get("sourceName");
				mappingMap.put(key, value);
			}
		} 
		else {
			for (Map<String,String> map : mappingItems) {
				map.put("isMerge", "false");
				String key = (String) map.get("destName");
				String value = (String) map.get("sourceName");
				mappingMap.put(key, value);
			}
		}
		return new MappingFieldVo(mergeField,mappingMap);
	}
	/** 构造 源和目标重复判定字段 的list<Map>*/
	@SuppressWarnings("rawtypes")
	private List<Map<String,String>> getCheckItems(List<String> paramCheckItems,Map<String, String> mappingField,Map sqlexper){
		List<Map<String,String>> checkitems = new ArrayList<>();
		for (String string : paramCheckItems) {
			String value = mappingField.get(string);
			if (value.indexOf(".") != -1) {
				value = value.substring(value.lastIndexOf(".") + 1);
			}
			if (string.indexOf(".") != -1) {
				string = string.substring(string.lastIndexOf(".") + 1);
			}
			String fn = (String)sqlexper.get(string);
			if (fn != null) {
				value = "@" + fn;
			}
			Map<String,String> map = new HashMap<>();
			map.put("destName", string);
			map.put("sourceName", value);
			checkitems.add(map);
		}
		return checkitems;
	}
	
	private class QuerysVo{
		private String condition;
		private Map<String, Object> params;
		@SuppressWarnings("rawtypes")
		private List<Map> fieldMap;

		@SuppressWarnings("rawtypes")
		private QuerysVo(String condition,Map<String, Object> params,List<Map> fieldMap) {
			this.condition = condition;
			this.params = params;
			this.fieldMap = fieldMap;
		}
		public String getCondition() {
			return condition;
		}
		public Map<String, Object> getParams() {
			return params;
		}
		@SuppressWarnings("rawtypes")
		public List<Map> getFieldMap() {
			return fieldMap;
		}
	}
	@SuppressWarnings({ "deprecation", "unchecked", "rawtypes" })
	private QuerysVo getQueryParams(CopyEntityParamVo cpm,MappingVo fieldMappingItems){
		IConditionParse parse = VDS.getIntance().getVSqlParse();
		ISQLBuf sql = parse.parseConditionsJson(null, cpm.getCondition(), cpm.getQueryParam());
		//SQLBuf sql = QueryConditionUtil.parseNotSupportRuleTemplate(conditionMap, queryParamMap);
		String queryCondition = sql.getSQL();

		Map<String, Object> params = sql.getParams();
		IVSQLQuery sqlQuery = VDS.getIntance().getVSQLQuery();
		
		IFieldMapping fm = new IFieldMapping() {
			@SuppressWarnings("rawtypes")
			@Override
			public SourceMappingType getSourceType(Map row) {
				if (VdsUtils.collection.isEmpty(row)) {
					throw new ConfigException("参数传入错误！row is null");
				}
				String sourceType = (String) row.get("valueType");
				if (VdsUtils.string.isEmpty(sourceType)) {
					throw new ConfigException("参数传入错误！参数[row]中没有包含对应的[sourceType]");
				}
				// 数据来源："expression : 表达式； sqlExpression ： sql表达式"
				SourceMappingType type = null;
				if (sourceType.toLowerCase().equals("expression")) {
					type = SourceMappingType.Expression;
				} else if (sourceType.toLowerCase().equals("sqlexpression")) {
					type = SourceMappingType.SQLExpression;
				} else {
					throw new ConfigException("参数传入错误！不支持[sourceType]类型.");
				}
				return type;
			}

			@Override
			public String getSourceFieldName() {
				return "sourceName";
			}

			@Override
			public String getDestFieldName() {
				return "destName";
			}
		};			
		IParamFieldVo  paramField = sqlQuery.parseParam(fieldMappingItems.getMapping(), fm); 
				
		/*ParamField paramField = new ParamFieldUtil(mappingItems, new FieldMapping() {

			@Override
			public SourceType getSourceType(Map row) {
				if (CollectionUtils.isEmpty(row)) {
					throw new ExpectedException("参数传入错误！row is null");
				}
				String sourceType = (String) row.get("sourceType");
				if (StringUtils.isEmpty(sourceType)) {
					throw new ExpectedException("参数传入错误！参数[row]中没有包含对应的[sourceType]");
				}
				SourceType type = null;
				if (sourceType.equalsIgnoreCase("expression")) {
					type = SourceType.Expression;
				} else if (sourceType.equalsIgnoreCase("entityField")) {
					type = SourceType.TableField;
				}
				return type;
			}

			@Override
			public String getSourceFieldName() {
				return "sourceName";
			}

			@Override
			public String getDestFieldName() {
				return "destName";
			}
		}).parse();*/

		List<Map> mappingItems = paramField.getItemsConverted();
		params.putAll(paramField.getParamMap());
		
		return new QuerysVo(queryCondition, params,mappingItems);
	}
	@Override
	@SuppressWarnings({ "unchecked", "rawtypes", "deprecation"  })
	public IRuleOutputVo evaluate(IRuleContext context) {
		//Map<String, Object> inParams = (Map) context.getRuleConfig().getConfigParams();// ruleCfgMap.get("inParams");
		// Map<String, Object> runtimeParams = context.getInputParams();
		// 执行的操作类型：1 ：追加；2：忽略；3：替换；4：合并
		CopyEntityParamVo cpm = new CopyEntityParamVo();
		cpm.setTypes((String)  context.getPlatformInput("operType"))
			.setSourceName((String) context.getPlatformInput("sourceName"))
			.setSourceType( (String) context.getPlatformInput("sourceType"))
			.setDestName((String) context.getPlatformInput("destName"))
			.setDestType((String) context.getPlatformInput("destType"))
			.setCheckitems((List<String>)context.getPlatformInput("checkitems"))
			.setMageitemObject(context.getPlatformInput("mergeitems"))
			.setCondition((List<Map>) context.getPlatformInput("condition"))
			.setQueryParam((List<Map>) context.getPlatformInput("itemqueryparam"))
			.setAddRecordObject(context.getPlatformInput("isAddRecord"));

		MappingVo fieldMappingItems = copyMappingItem((List<Map>) context.getPlatformInput("items"));// 复制映射信息 
		MappingFieldVo mappingField = getMergeField(cpm,fieldMappingItems.getMapping());
		
		// 构造 源和目标重复判定字段 的list<Map>
		List<Map<String,String>> checkitems = getCheckItems(cpm.getCheckitems(),mappingField.getMappingMap(),fieldMappingItems.getSqlExper());
		QuerysVo querys = getQueryParams(cpm,fieldMappingItems);
		Map<String, Object> experParams = handleSqlFuncExper(querys.getParams(), fieldMappingItems.getSqlExper());

		IRuleVObject contextObject = context.getVObject();
		IDataView sourceDV =(IDataView)contextObject.getContextObject(cpm.getSourceName(), cpm.getSourceType());//getDataViewWithType(context, cpm.getSourceName(),cpm.getSourceType());
		IDataView destDV = (IDataView)contextObject.getContextObject(cpm.getDestName(), cpm.getDestType());//getDataViewWithType(context, destName, destType);
		IDataView operDataView = null;
		
		switch (cpm.getType()) {
			case Append:// 追加
				operDataView = appendRecord( sourceDV, querys , destDV , experParams);
				break;
			case Ignore:// 忽略
				//isAddRecord = true;
				operDataView = updateRecords(sourceDV, querys ,destDV, checkitems, OptionType.Leave,
						mappingField.getMergeField(), true, experParams, true);
				break;
			case Replace:// 替换
				operDataView = updateRecords(sourceDV, querys, destDV, checkitems, OptionType.Replace
						,mappingField.getMergeField(), cpm.isAddRecord(), experParams, true);
				break;
			case Merge:// 合并
				operDataView = updateRecords(sourceDV, querys, destDV,  checkitems, OptionType.Replace
						, mappingField.getMergeField(), cpm.isAddRecord(), experParams, true);
				break;
			default:
				break;
		}
		// 处理流水号函数
		operDataView = handleLastFuncExper(operDataView, fieldMappingItems.getLastExper());
		if (null != operDataView) { 
			//setDataViewWithType(context, cpm.getDestName(), cpm.getDestType(), operDataView);
			contextObject.setContextObject(cpm.getDestType(), cpm.getDestName(), operDataView);
		}
		return context.newOutputVo();
	}

	/**操作方式的映射字段 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private MappingVo copyMappingItem(List<Map> mapping1) {
		List<Map> mapping = new ArrayList<Map>();
		for (int i = 0; i < mapping1.size(); i++) {
			Map _tmp_ = new HashMap<Object, Object>();
			for (Object map : mapping1.get(i).keySet()) {
				_tmp_.put(map, mapping1.get(i).get(map));
			}
			mapping.add(_tmp_);
		}
		MappingVo rd = new MappingVo();
		Map<String, Object> mp = changeMappingExper(mapping);
		rd.setMapping((List<Map>) mp.get("mapping"))
			.setSqlExper((Map<String, Object>) mp.get("sqlExper"))
			.setLastExper((Map<String, Object>) mp.get("lastExper"));
		
		return rd;
	}

	/**
	 * 解析mapping，获取随机函数，并将流水号函数换成其他表达式，避免出现缺失一个流水号的情况
	 * 
	 * @param mapping
	 * @return
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Map<String, Object> changeMappingExper(List<Map> mapping) {
		Map<String, Object> result = new HashMap<String, Object>();
		// sql类型的函数表达式
		Map<String, Object> sqlFuncExper = new HashMap<String, Object>();
		// 在得到最后结果才进行处理的函数表达式
		Map<String, Object> lastHandlerExper = new HashMap<String, Object>();
		for (Map map : mapping) {
			if (map.get("sourceType").equals("expression")) {
				// 20161010 liangzc:处理表达式中的随机函数
				boolean isexist = false;
				String sourceName = map.get("sourceName") != null ? map.get("sourceName").toString() : "";
				String destName = map.get("destName") != null ? map.get("destName").toString() : "";
				for (String lastexper : LASTHANDLEREXPER) {
					if (sourceName.contains(lastexper)) {
						isexist = true;
						map.put("sourceName", "0");
						lastHandlerExper.put(destName.split("\\.")[1], sourceName);
						break;
					}
				}
				if (!isexist) {
					for (String sqlexper : SQLFUNCEXPER) {
						if (sourceName.contains(sqlexper) && sourceName.indexOf(sqlexper) == 0) {
							isexist = true;
							String tmp_sourceName = parseParam(sqlexper, sourceName);
							sqlFuncExper.put(destName.split("\\.")[1], tmp_sourceName);
							break;
						}
					}
				}
			}
		}
		result.put("sqlExper", sqlFuncExper);
		result.put("lastExper", lastHandlerExper);
		result.put("mapping", mapping);
		return result;
	}

	/**
	 * 解析随机函数参数
	 * 
	 * @param sqlfunc 随机函数名
	 * @param sourceName 源表达式
	 * @return
	 */
	private String parseParam(String sqlfunc, String sourceName) {
		String result = "";
		String tmp_result = "ConvertSqlFormat" + sourceName.substring(sqlfunc.length(), sourceName.length());
		// 2016-11-23 liangzc：此处将Random改成RandomFunc，但参数不不变
		String suffix = "";
		if (sqlfunc.equals("Random")) {
			suffix = "Func";
		}
		IFormulaEngine en = VDS.getIntance().getFormulaEngine();
		result = sqlfunc + suffix + en.eval(tmp_result);
		return result;
	}

	/**
	 * 处理不能在sql语句中处理的随机函数
	 * 
	 * @param operDataView
	 * @param lastHandlerExper
	 * @return
	 */
	private IDataView handleLastFuncExper(IDataView operDataView, Map<String, Object> lastHandlerExper) {
		if (lastHandlerExper.keySet().isEmpty() ) {
			return operDataView;
		}
		IFormulaEngine en = VDS.getIntance().getFormulaEngine();
		List<IDataObject> allDataObjects = operDataView.select();
		for (IDataObject dataObject : allDataObjects) {
			for (String map : lastHandlerExper.keySet()) {
				String ex = lastHandlerExper.get(map).toString();
				dataObject.set(map, en.eval(ex));
			}
		}
		return operDataView;
	}

	/**
	 * 处理能够在sql语句中处理的随机函数
	 * 
	 * @param params 原本的参数
	 * @param sqlFuncExper 可以放入sql语句中处理随机函数
	 * @return
	 */
	private Map<String, Object> handleSqlFuncExper(Map<String, Object> params, Map<String, Object> sqlFuncExper) {
		Map<String, Object> result = new HashMap<String, Object>();
		for (String code : params.keySet()) {
			String tmp_code = code.substring(0, code.lastIndexOf("_"));
			if (sqlFuncExper.containsKey(tmp_code)) {
				result.put(code, "@" + sqlFuncExper.get(tmp_code));
			} else {
				// result.put(code, params.get(code));
			}
		}
		return result;
	}

	/**
	 * 赋值目标实体
	 * 
	 * @param context
	 * @param destName
	 * @param destType
	 * @param destDataView
	 
	private void setDataViewWithType(RuleContext context, String destName, String destType, DataView destDataView) {
		switch (VariableType.getInstanceType(destType)) {
			case RuleSetInput:
				RuleSetVariableUtil.setInputVariable(context, destName, destDataView);
				break;
			case RuleSetVar:
				RuleSetVariableUtil.setContextVariable(context, destName, destDataView);
				break;
			case RuleSetOutput:
				RuleSetVariableUtil.setOutputVariable(context, destName, destDataView);
				break;
			case Window:
			default:
				throw new ExpectedException("不支持类型[" + destType + "]的变量值复制.");
		}
	}*/

	/**
	 * 取来源实体
	 * 
	 * @param context
	 * @param sourceName
	 * @param sourceType
	 * @return
	private IDataView getDataViewWithType(IRuleContext context, String sourceName, String sourceType) {
		context.getVObject().getContextObject(key, type);
		IDataView sourceDV = null;
		switch (VariableType.getInstanceType(sourceType)) {
			case RuleSetInput:
				sourceDV = (DataView) RuleSetVariableUtil.getInputVariable(context, sourceName);
				break;
			case RuleSetVar:
				sourceDV = (DataView) RuleSetVariableUtil.getContextVariable(context, sourceName);
				break;
			case RuleSetOutput:
				sourceDV = (DataView) RuleSetVariableUtil.getOutputVariable(context, sourceName);
				break;
			case Window:
			default:
				throw new ExpectedException("不支持类型[" + sourceType + "]的变量值设置.");
		}
		if (sourceDV == null) {
//			logger.error("指定的数据源【" + sourceName + "】不存在！");
			throw new ConfigException(ErrorCodeServerRules.paramIsNullException.getErrorCode(),"指定的数据源【" + sourceName + "】不存在！");
		}
		return sourceDV;
	}
	 */
 
	 
	/**
	 * 
	 * @param sourceView 9
	 * @param querys 8
	 * @param destView 7 
	 * @param checkField 6
	 * @param operType 5
	 * @param mergeField 4
	 * @param isAddRecord 3
	 * @param tmp_params 2
	 * @param isFilterSource 1是否过滤来源实体的重复记录，现在限定在忽略和替换的情况下过滤掉。
	 * @return
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private IDataView updateRecords(IDataView sourceView, QuerysVo querys , IDataView destView
			,  List<Map<String, String>> checkField, OptionType operType,List<String> mergeField
			, boolean isAddRecord, Map<String, Object> experParams, boolean isFilterSource) {
		
		//VSQLQueryUpdate queryUpdate = new VSQLQueryUpdate();
		IVSQLQueryUpdate queryUpdate = VDS.getIntance().getVSQLQuery().newQueryUpdate();
		if (isAddRecord == false && operType == OptionType.Replace) {// 如果不需要新增
			queryUpdate.setUpdateType(UpdateType.Update);
		} else {
			queryUpdate.setUpdateType(UpdateType.Insert);
		}
		queryUpdate.setVSqlConditions(getVSqlConditions(querys.getCondition()));
		Map<String, Map<String, String>> opFields = buildOpFields( querys.getFieldMap(), experParams);
		Map<String, String> setMap = opFields.get("setMap");
		queryUpdate.setSetMap(setMap);// item 映射
		Map<String, String> _mfield = opFields.get("setOpMap");
		for (String _str : mergeField) {
			_mfield.put(_str.split("\\.")[1], "accumulate");
		}
		queryUpdate.setSetOpMap(_mfield);
		queryUpdate.setSrcRowUnique(isFilterSource);
		Map<String, String> fieldFuncs = (Map<String, String>) opFields.get("fieldFuncs");

		queryUpdate.setFieldFuncs(fieldFuncs);

		List<String> sourceNames = new ArrayList<String>();
		List<String> destNames = new ArrayList<String>();
		Map<String, String> checkFields = new HashMap<String, String>();
		for (Map fields : checkField) {
			String sourceName = (String) fields.get("sourceName");
			String destName = (String) fields.get("destName");
			checkFields.put(destName, sourceName);
			sourceNames.add(sourceName);
			destNames.add(destName);
		}

		// 全复制或者忽略插入优化性能处理
		if ( mergeField.isEmpty() && experParams.isEmpty() && isFilterSource
				&& _mfield.isEmpty() && fieldFuncs.isEmpty() && sourceNames.size() > 0) {
			boolean isSetFieldAll = true;
			for (Map<String, String> map :  querys.getFieldMap()) {
				String sourceType = map.get("sourceType");
				if (!"entityField".equals(sourceType)) {
					isSetFieldAll = false;
					break;
				}
			}
			String cdt = querys.getCondition();
			String queryCondString= ("( 1=1 )".equals(cdt)|| VdsUtils.string.isEmpty(cdt)?"":cdt);//+ " and " + IH2MetaData.H2_STATE_FIELD + "!=" + DataState.Deleted.getState();
			if (isSetFieldAll) {				
				if(operType == OptionType.Replace){
					List<IDataObject> destDatas = destView.select();
					if(isAddRecord&&destDatas.size()==0){
						boolean isCopy=copyInsertRecord(sourceView, destView, queryCondString, setMap, querys.getParams(), sourceNames,destNames);
						if(isCopy){
							return destView;
						}
					}else{
						//如果目标实体为空则不需要更新直接返回目标实体
						if(destDatas.size()==0){
							return destView;
						}
						List<IDataObject> datas = sourceView.select(queryCondString,querys.getParams());
						//这里由于50000条以上批量更新的时间比循环时间更快
						if(datas.size()<copyMaxCount){
							Set<String> destNameSet = setMap.keySet();
							Map<String, Map<String, Object>> sourceDataMap = new HashMap<String, Map<String, Object>>(datas.size());
							for (IDataObject data : datas) {
								if(DataState.Deleted != data.getStates()){
									String id = "";
									for (String srcName : sourceNames) {
										id = id + "#" + data.get(srcName);
									}
									Map<String, Object> dataMap = new HashMap<String, Object>();
									for (String setName : destNameSet) {
										String srcName = setMap.get(setName);
										dataMap.put(setName, data.get(srcName));
									}
									sourceDataMap.put(id, dataMap);
								}							
							}
							
							for (IDataObject destData : destDatas) {
								String id = "";
								for (String destName : destNames) {
									id = id + "#" + destData.get(destName);
								}
								Map<String, Object> dataMap = sourceDataMap.get(id);
								if(dataMap == null){
									continue;
								}
								
								for (String setName : destNameSet) {
									destData.set(setName, dataMap.get(setName));
								}
							}
							//如果含有增加记录则插入并更新
							if(isAddRecord){								
								copyInsertSelectData(datas, destView, setMap, sourceNames,destNames);
							}
							return destView;
							
						}
					}
					
					
				}else if(operType == OptionType.Leave  && isAddRecord){
					boolean isCopy=copyInsertRecord(sourceView, destView, queryCondString, setMap, querys.getParams(), sourceNames,destNames);
					if(isCopy){
						return destView;
					}
					
				}
				
			}
		}
		
		

		queryUpdate.setUniqueFields(checkFields);
		queryUpdate.setOpType(operType);
		return destView.excuteSql(queryUpdate, querys.getParams(), sourceView,null);
	}
	
	/**
	 * 拷贝插入记录
	 * @param sourceView 源实体
	 * @param destView 目标实体
	 * @param queryCondString
	 * @param setMap
	 * @param params
	 * @param sourceNames
	 * @return
	 */
	private boolean copyInsertRecord(IDataView sourceView,IDataView destView,String queryCondString
			,Map<String, String>  setMap,Map<String, Object> params
			,List<String> sourceNames,List<String> destNames){
		if("".equals(queryCondString)){
			//如果是忽略插入
			List<Map<String,Object>> datas = sourceView.getDatas();
			//这里由于10000条以上批量更新的时间比循环时间更快
			if(datas.size()<copyMaxCount){
				List<Map<String,Object>> insertMaps = new ArrayList<Map<String,Object>>();
				List<Map<String,Object>> destDatas = destView.getDatas();
				Set<String> destNameSet = setMap.keySet();
				Set<String> keyMapSet=new HashSet<String>();
				Set<String> keyAddMapSet=new HashSet<String>();
				for (Map<String,Object> destData : destDatas) {
					String id = "";
					for (String descName : destNames) {
						id = id + "#" + destData.get(descName);
					}							
					keyMapSet.add(id);
				}
				
				for (Map<String,Object> sourceData : datas) {
					String id = "";
					for (String sourceName : sourceNames) {
						id = id + "#" + sourceData.get(sourceName);
					}							
					if(!keyMapSet.contains(id)){
						Map<String, Object> insertMap = new HashMap<String, Object>();
						for (String setName : destNameSet) {
							insertMap.put(setName, sourceData.get(setMap.get(setName)));
						}
						if(!keyAddMapSet.contains(id)){
							insertMaps.add(insertMap);
							keyAddMapSet.add(id);
						}																
					}
					
				}
				destView.insertDataObject(insertMaps);
				return true;
			}
		}else{
			//如果是忽略插入
			List<IDataObject> datas = sourceView.select(queryCondString,params);
			//这里由于10000条以上批量更新的时间比循环时间更快
			if(datas.size()<copyMaxCount){
				copyInsertSelectData(datas, destView, setMap, sourceNames,destNames);
				return true;
			}
		}
		return false;
		
		
		
	}
	
	/**
	 * 忽略插入二次过滤后的数据
	 * @param datas
	 * @param destView
	 * @param setMap
	 * @param sourceNames
	 */
	private void copyInsertSelectData(List<IDataObject> datas,IDataView destView,Map<String, String>  setMap,List<String> sourceNames,List<String> descNames){
		List<Map<String,Object>> insertMaps = new ArrayList<Map<String,Object>>();
		List<Map<String,Object>> destDatas = destView.getDatas();
		Set<String> destNameSet = setMap.keySet();
		Set<String> keyMapSet=new HashSet<String>();
		Set<String> keyAddMapSet=new HashSet<String>();
		for (Map<String,Object> destData : destDatas) {
			String id = "";
			for (String descName : descNames) {
				id = id + "#" + destData.get(descName);
			}							
			keyMapSet.add(id);
		}
		
		for (IDataObject sourceData : datas) {
			//if(DataState.Deleted.getState()!=(Integer)sourceData.get(IH2MetaData.H2_STATE_FIELD)){
			if(DataState.Deleted == sourceData.getStates()) {
				continue ;
			}
			
			String id = "";
			for (String sourceName : sourceNames) {
				id = id + "#" + sourceData.get(sourceName);
			}							
			if(!keyMapSet.contains(id)){
				Map<String, Object> insertMap = new HashMap<String, Object>();
				for (String setName : destNameSet) {
					insertMap.put(setName, sourceData.get(setMap.get(setName)));
				}
				if(!keyAddMapSet.contains(id)){
					insertMaps.add(insertMap);
					keyAddMapSet.add(id);
				}																
			} 															
		}
		destView.insertDataObject(insertMaps);
	}
 
	private IDataView appendRecord(IDataView sourceView, QuerysVo querys, IDataView destView , Map<String, Object> experParams) {
		IVSQLQueryUpdate queryUpdate = VDS.getIntance().getVSQLQuery().newQueryUpdate();
		//VSQLQueryUpdate queryUpdate = new VSQLQueryUpdate();
		//queryUpdate.setUpdateType(VSQLConst.OpInsert);
		queryUpdate.setUpdateType(UpdateType.Insert);
		queryUpdate.setVSqlConditions(getVSqlConditions(querys.getCondition()));
		Map<String, Map<String, String>> opFields = buildOpFields(querys.getFieldMap(), experParams);
		queryUpdate.setSetMap(opFields.get("setMap"));// item 映射
		queryUpdate.setSetOpMap(opFields.get("setOpMap"));
		queryUpdate.setFieldFuncs(opFields.get("fieldFuncs")); 
		IDataView _dv = destView.excuteSql(queryUpdate, querys.getParams(), sourceView,null);
		return _dv;
	}

	/**
	 * 要操作的字段 key=目标表字段，value=源表字段 <br>
	 * 要操作字段的更新方式：key=目标表字段，value=字段更新处理方式：（累加/覆盖）<br>
	 * 相同记录判定的字段 key=目标表字段，value=源表字段
	 * 
	 * @param fields
	 * @return
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected Map<String, Map<String, String>> buildOpFields(  List<Map> fieldMappings, Map<String, Object> tmp_params) {
		Map<String, Map<String, String>> result = new HashMap<String, Map<String, String>>();
		Map<String, String> setMap = new HashMap<String, String>();
		Map<String, String> setOpMap = new HashMap<String, String>();
		Map<String, String> fieldFuncs = new HashMap<String, String>();
		result.put("setMap", setMap);
		result.put("setOpMap", setOpMap);
		result.put("fieldFuncs", fieldFuncs);
		for (Map<String, String> map : fieldMappings) {
			String queryField = (String) map.get("sourceName");
			if (queryField.indexOf(".") != -1) {
				queryField = queryField.substring(queryField.lastIndexOf(".") + 1);
			}
			String tableField = (String) map.get("destName");
			if (tableField.indexOf(".") != -1) {
				tableField = tableField.substring(tableField.lastIndexOf(".") + 1);
			}
			if (VdsUtils.string.isEmptyAny(queryField,tableField)) {
				continue;
			}
			String isMerge = String.valueOf(map.get("isMerge")).toString();
			String sourceType = map.get("sourceType");
			if (sourceType.equals("expression")) {
				if (null != queryField) {
					String tmp_key = queryField.substring(1, queryField.length());
					if (tmp_params.containsKey(tmp_key) && tmp_params.get(tmp_key) != null) {
						setMap.put(tableField, tmp_params.get(tmp_key).toString());
					} else {
						setMap.put(tableField, queryField);
					}
				} else {
					setMap.put(tableField, queryField);
				}
			} else {
				setMap.put(tableField, queryField);
			}
			if (null != isMerge && "true".equalsIgnoreCase(isMerge)) {
				setOpMap.put(tableField, OptionType.Replace.getValue());
				fieldFuncs.put(tableField, SUM);
			}
		}
		return result;
	}

	/**
	 * 要操作的字段 key=目标表字段，value=源表字段 <br>
	 * 要操作字段的更新方式：key=目标表字段，value=字段更新处理方式：（累加/覆盖）<br>
	 * 相同记录判定的字段 key=目标表字段，value=源表字段
	 * 
	 * @param fields
	 * @return
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected Map<String, Map<String, String>> buildOpFields(String updateType, List<Map> fieldMappings) {
		Map<String, Map<String, String>> result = new HashMap<String, Map<String, String>>();
		Map<String, String> setMap = new HashMap<String, String>();
		Map<String, String> setOpMap = new HashMap<String, String>();
		Map<String, String> fieldFuncs = new HashMap<String, String>();
		result.put("setMap", setMap);
		result.put("setOpMap", setOpMap);
		result.put("fieldFuncs", fieldFuncs);
		for (Map<String, String> map : fieldMappings) {
			String queryField = (String) map.get("sourceName");
			if (queryField.indexOf(".") != -1) {
				queryField = queryField.substring(queryField.lastIndexOf(".") + 1);
			}
			String tableField = (String) map.get("destName");
			if (tableField.indexOf(".") != -1) {
				tableField = tableField.substring(tableField.lastIndexOf(".") + 1);
			}
			if (VdsUtils.string.isEmptyAny(queryField,tableField)) {
				continue;
			}
			String isMerge = String.valueOf(map.get("isMerge")).toString();
			setMap.put(tableField, queryField);
			if (null != isMerge && "true".equalsIgnoreCase(isMerge)) {
				setOpMap.put(tableField,OptionType.Replace.getValue());
				fieldFuncs.put(tableField, SUM);
			}
		}
		return result;
	}

	/**
	 * 获取查询的条件
	 * 
	 * @param conditions
	 * @return
	 */
	private IVSQLConditions getVSqlConditions(String conditions) {
		if (VdsUtils.string.isEmpty(conditions)) {
			return null;
		}
		IVSQLQuery parse = VDS.getIntance().getVSQLQuery();
		IVSQLConditions cdt = parse.getVSQLConditions(conditions);
		//IVSQLConditions vSqlConditions = IVSQLConditionsFactory.getService().init();
		//vSqlConditions.setSqlConStr(conditions);
		//vSqlConditions.setLogic(VSQLConst.LogicAnd);
		return cdt;
	}

	/**
	 * 字段更新类型(行重复处理方式为更新时有效)
	 * 
	 * @param fieldUpdateType
	 * @return
	 */
	/*
	 * protected String translateFieldUpdateType(String fieldUpdateType) { if (StringUtils.isEmpty(fieldUpdateType)// ||
	 * FIELD_UPDATE_TYPE_IGNORE.equals(fieldUpdateType)) return VSQLConst.OpLeave; if (FIELD_UPDATE_TYPE_REPLACE.equals(fieldUpdateType)) return
	 * VSQLConst.OpReplace; if (FIELD_UPDATE_TYPE_ADD.equals(fieldUpdateType)) return VSQLConst.OpAccumulate; if
	 * (FIELD_UPDATE_TYPE_REDUCE.equals(fieldUpdateType)) return VSQLConst.OpDescend; return null; }
	 */

	/**
	 * 检验更新时， 会不会出现所有更新列为空的情况
	 * 
	 * @param setOpMap
	 * @return
	 */
	protected boolean isValidate(Map<String, String> setOpMap) {
		boolean validate = false;
		for (Entry<String, String> entry : setOpMap.entrySet()) {
			String value = entry.getValue();
			if (!OptionType.Leave.getValue().equals(value)) {
				validate = true;
			}
		}
		return validate;
	}

}
