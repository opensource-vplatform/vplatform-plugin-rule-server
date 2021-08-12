package com.toone.itop.formula.rule.inte;

import java.net.URLEncoder;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.toone.itop.rule.apiserver.util.AbstractRule4Tree;
import com.yindangu.v3.business.VDS;
import com.yindangu.v3.business.formula.api.IFormulaEngine;
import com.yindangu.v3.business.jdbc.api.model.IColumn;
import com.yindangu.v3.business.jdbc.api.model.IDataSetMetaData;
import com.yindangu.v3.business.jdbc.api.model.IQueryParamsVo;
import com.yindangu.v3.business.jdbc.api.model.ITable;
import com.yindangu.v3.business.metadata.api.IDAS;
import com.yindangu.v3.business.metadata.api.IDataObject;
import com.yindangu.v3.business.metadata.api.IDataView;
import com.yindangu.v3.business.metadata.apiserver.IMdo;
import com.yindangu.v3.business.plugin.business.api.rule.ContextVariableType;
import com.yindangu.v3.business.plugin.business.api.rule.IRule;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleContext;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleOutputVo;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleVObject;
import com.yindangu.v3.business.plugin.execptions.BusinessException;
import com.yindangu.v3.business.plugin.execptions.ConfigException;
import com.yindangu.v3.business.plugin.execptions.EnviException;
import com.yindangu.v3.business.rule.api.parse.IQueryParse;
import com.yindangu.v3.business.rule.api.parse.ISQLBuf;
import com.yindangu.v3.business.vsql.apiserver.IVSQLConditions;
import com.yindangu.v3.business.vsql.apiserver.IVSQLQuery;
import com.yindangu.v3.business.vsql.apiserver.IVSQLUtil;
import com.yindangu.v3.business.vsql.apiserver.IVSQLUtil.FieldType;
import com.yindangu.v3.business.vsql.apiserver.IVSQLUtil.IFieldMap;
import com.yindangu.v3.business.vsql.apiserver.VSQLConditionLogic;
import com.yindangu.v3.platform.plugin.util.VdsUtils;

/**
 * 从数据库加载数据到界面实体
 * @author jiqj
 *
 */
public class DataBaseDataToInterfaceEntity  extends AbstractRule4Tree implements IRule {
	
	public static final String D_RULE_NAME = "从数据库加载数据到界面实体";
	public static final String D_RULE_CODE = "DataBaseDataToInterfaceEntity";
	public static final String D_RULE_DESC = "";
	
	//private final static String IDENTIFIER_FIELD_NAME = "id";
	//private final static String DATASOURCE_ID_FIELD_NAME = "__ds_id__";

	private static final String Param_ItemsConfig = "itemsConfig";
    
	private static final String Param_SourceName = "sourceName";
	private static final String Param_EntityName = "entityName";
	private static final String Param_EntityType = "entityType";
	private static final String Param_FileAutoMapping = "isFieldAutoMapping";
	private static final String Param_DsWhere = "dsWhere";
	private static final String Param_Items = "items";
	private static final String Param_pager = "pager";
	private static final String Param_OrderBy = "orderBy";
	private static final String Param_Dataload = "dataLoad";
	private static final String Param_treeStruct = "treeStruct";
//	private static final String Param_QueryFields = "";
	private static final String Param_QueryParams = "itemqueryparam";

	private static final Logger	log	= LoggerFactory.getLogger(DataBaseDataToInterfaceEntity.class);
	
	private class PageSizeVo{
		private int pageSize;
		private int recordStart;
		@SuppressWarnings("rawtypes")
		private List<Map> totalRecordSave;
		public PageSizeVo() {
			this(-1,null,-1);
		}
		@SuppressWarnings({ "rawtypes", "unchecked" })
		public PageSizeVo(int pageSize,List records,int recordStart) {
			this.pageSize = pageSize;
			this.recordStart = recordStart;
			this.totalRecordSave =records;// (records == null ? Collections.EMPTY_LIST : records);
		}
		/**默认-1*/
		public int getPageSize() {
			return pageSize;
		}
		/**默认-1*/
		public int getRecordStart() {
			return recordStart;
		}
		/**默认 null */
		@SuppressWarnings("rawtypes")
		public List<Map> getTotalRecordSave() {
			return totalRecordSave;
		}
	}
	@SuppressWarnings("rawtypes")
	private PageSizeVo getQueryPageNo(List<Map> pageData) {
		
		int pageSize  = -1;
		int recordStart  = -1;
		List totalRecordSave = null ;
		IFormulaEngine engine = VDS.getIntance().getFormulaEngine();
		
		for(Map _map : pageData){
			Boolean _flag = (Boolean) _map.get("isPaging");
			if(_flag!=null &&  _flag){
				Object pageNo = _map.get("pageNo");
				Object pSize = _map.get("pageSize");
				int pn = 0;
				int ps = 1;
				
				if(pageNo instanceof String){ 
					//if (!StringUtils.isEmpty(pageNo.toString())) {
					if(VdsUtils.string.isEmpty((String)pageNo)) {
						throw new BusinessException("函数第一个参数不能为空");
					}
					else {
						//Object pnObject = FormulaEngineFactory.getFormulaEngine().eval(pageNo.toString());
						Object pnObject = engine.eval((String)pageNo);
						if (pnObject instanceof Double) {
							pn = ((Double)pnObject).intValue();
						}else if(pnObject instanceof Integer){
							pn = ((Integer)pnObject).intValue();
						}
						
					}
				}else if(pageNo instanceof Integer){
					pn =((Integer) pageNo).intValue();
				}
				
				if(pSize instanceof String){
					if (VdsUtils.string.isEmpty((String)pSize )) {
						throw new BusinessException("函数第二个参数不能为空");
					}
					else{
						//Object pnObject = FormulaEngineFactory.getFormulaEngine().eval(pSize.toString());
						Object pnObject = engine.eval((String)pSize);
						if (pnObject instanceof Double) {
							ps = ((Double)pnObject).intValue();
						}else if(pnObject instanceof Integer){
							ps = ((Integer)pnObject).intValue();
						}
						pageSize = ps;
					}
				}else if(pSize instanceof Integer){
					ps = ((Integer) pSize).intValue();
					pageSize = ps ;
				}
				recordStart = (pn - 1) * ps + 1;
			}
			totalRecordSave = (List)_map.get("totalRecordSave");
		}
		return new PageSizeVo(pageSize, totalRecordSave, recordStart);
	}
	private class QueryParamsVo{
		private String dataSourceName;
		private String whereCondition;
		private String tempCondition;
		private String orders;
		private Map<String, Object> queryParams;
 
		public String getDataSourceName() {
			return dataSourceName;
		}
		public QueryParamsVo setDataSourceName(String dataSourceName) {
			this.dataSourceName = dataSourceName;
			return this;
		}
		public String getWhereCondition() {
			return whereCondition;
		}
		public QueryParamsVo setWhereCondition(String whereCondition) {
			this.whereCondition = whereCondition;
			return this;
		}
		public String getTempCondition() {
			return tempCondition;
		}
		public QueryParamsVo setTempCondition(String tempCondition) {
			this.tempCondition = tempCondition;
			return this;
		}
		/**查询参数*/
		public Map<String, Object> getQueryParams() {
			return queryParams;
		}
		/**查询参数*/
		public QueryParamsVo setQueryParams(Map<String, Object>  queryParams) {
			this.queryParams = queryParams;
			return this;
		}
 
		public String getOrders() {
			return orders;
		}
		public QueryParamsVo setOrders(String orders) {
			this.orders = orders;
			return this;
		}
	}
	private IDataView loadDataView(QueryParamsVo queryVo ,boolean isQuery ,PageSizeVo pageSizeVo) {
		//String tmp_condition = (String) result.get("condition");
		long start=System.currentTimeMillis();
		
		try{				
			IDataView dataView;
			if(isQuery) {//if (vtable.isQuery()) {
				//dataView = findFromQuery(dataSourceName, whereCond, queryParams, recordStart, pageSize,tmp_condition);
				dataView = findFromQuery(queryVo,pageSizeVo);
			} else {
				//dataView = findFromTable(dataSourceName, queryFields, whereCond, orderStr, queryParams, recordStart,pageSize,tmp_condition);
				List<String> queryFields = null; // TODO 预留可使用部分字段作为查询条件
				dataView = findFromTable(queryVo,pageSizeVo,queryFields );
			}
			long dua=System.currentTimeMillis()-start;
			if(dua > 50){
				loggerInfo("加载数据库记录查询耗时：【"+dua+"】毫秒");
			}
			return dataView;
		}catch (OutOfMemoryError e) {
			String errorMsg="";
			//String componentCode=CompContext.getCompCode();
			//componentCode=StringUtils.isEmpty(componentCode)?"":componentCode+".";
			if(isQuery) {//if (vtable.isQuery()) {
				errorMsg="查询:"+queryVo.getDataSourceName()+"加载错误";
			}else{
				errorMsg="表:"+queryVo.getDataSourceName()+"加载错误";
				
			}
			if(pageSizeVo.getPageSize()  <= 0){
				String qs = (queryVo.getQueryParams() == null? "": queryVo.getQueryParams().toString());
				errorMsg=errorMsg+",可能查询大量数据导致内存溢出，请检查配置，查询条件:"+queryVo.getWhereCondition()+",查询参数为:"+ qs;
				throw new ConfigException(errorMsg,e);
			}else{
				throw new EnviException(errorMsg,e);
			}								
			// TODO: handle exception
		}
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes", "deprecation" })
	private QueryParamsVo getQueryParamsVo(IRuleContext context,Map<String, Object> selectData,List<Map> queryCondition,String dataSourceName) {
		Object queryParamsObj = selectData.get(Param_DsWhere);
		String entityName = (String) selectData.get(Param_EntityName);
		List<Map<String, Object>> oriQueryParams = (List<Map<String, Object>>) selectData.get(Param_QueryParams);
		// 初始化查询参数

		IFormulaEngine engine = VDS.getIntance().getFormulaEngine();
		Map<String, Object> queryParams = new HashMap<String, Object>();
		if (!VdsUtils.collection.isEmpty(oriQueryParams)) {
			for (Map<String, Object> paramConfig : oriQueryParams) {
				String paramKey = (String) paramConfig.get("queryfield");
				Object paramValue = null;
				String paramValueExpr = (String) paramConfig.get("queryfieldValue");
				if (!VdsUtils.string.isEmpty(paramValueExpr)) {
					paramValue = engine.eval(paramValueExpr);
				}
				queryParams.put(paramKey, paramValue);
			}
		}
		
		List<Map> condSql = new ArrayList<Map>();
		if (queryParamsObj != null && queryParamsObj instanceof List) {
			condSql = (List<Map>) queryParamsObj;
		}
		String whereCond = "";
		if (!VdsUtils.collection.isEmpty(condSql)) {
			IQueryParse vparse= VDS.getIntance().getVSqlParse(); 
    		ISQLBuf sb = vparse.parseConditionsJson(null,condSql,null);
			//SQLBuf sb = QueryConditionUtil.parseConditionsNotSupportRuleTemplate(condSql);
			
			whereCond = sb.getSQL();
			queryParams.putAll(sb.getParams());
		}
		
		Map<String, Object> result = new LinkedHashMap<String, Object>();
		//获取加载层级参数
		String dynamicLoad = (String)selectData.get(Param_Dataload);
		if (dynamicLoad != null && !dynamicLoad.equals("-1") && !dynamicLoad.equals("0")){
			//Map<String, Object> treeStruct = getTreeStruct(entityName,(List<Map<String, Object>>)ruleConfig.getConfigParamValue(Param_treeStruct));
			List<Map<String, Object>>  treeParam = (List<Map<String, Object>>)context.getPlatformInput(Param_treeStruct); 
			Map<String, Object> treeStruct = getTreeStruct(entityName,treeParam);
			if (treeStruct != null) {
				
				Map<String, Object> whereObj = new LinkedHashMap<String, Object>();
				whereObj.put("condition", "1=1");
				whereObj.put("parameters", new LinkedHashMap<String,Object>());
				//String whereObjJson = URLEncoder.encode(JsonUtils.toJson(whereObj));
				
				String whereObjJson = URLEncoder.encode(VdsUtils.json.toJson(whereObj));
				//将实体的树结构转为表的树结构
				Map<String, Object> sourceTreeStruct = dest2SourceTreeStruct(queryCondition, treeStruct);
				//String treeStructJson = JsonUtils.toJson(treeStruct);
				String treeStructJson = VdsUtils.json.toJson(sourceTreeStruct);
				treeStructJson = URLEncoder.encode(treeStructJson);
				String levelPress = "DynamicLoadCondition(\"" + dataSourceName + "\",\"" + dynamicLoad + "\", \"" + treeStructJson + "\",\"" +
						whereObjJson + "\")";
				
				
				result = engine.eval(levelPress);
			}
		}
		
		QueryParamsVo queryVo = new QueryParamsVo();
		queryVo.setDataSourceName(dataSourceName)
			.setQueryParams(queryParams)
			.setWhereCondition(whereCond)
			.setTempCondition( (String) result.get("condition")) ;
		return queryVo;
	}
	
	@SuppressWarnings({ "deprecation", "rawtypes" })
	private void doSaveTotalRecord(IRuleVObject ruleVObject ,IDataView dataView, List<Map> totalRecordSave) {
		for(Map map : totalRecordSave){
			Boolean saveTotalRecord = (Boolean) map.get("isSaveTotalRecord");
			if(saveTotalRecord != null && saveTotalRecord){
				ContextVariableType contextType ;
				String returnValue = map.get("target").toString();
				String returntype = map.get("targetType").toString();
				if(returntype.equals("methodVariant")){
					//returnValue = "BR_VAR_PARENT."+returnValue;
					contextType = ContextVariableType.RuleSetVar;
					
				}else if(returntype.equals("methodOutput")){
					contextType = ContextVariableType.RuleSetOutput;
					//returnValue = "BR_OUT_PARENT."+returnValue;
				}else{
					throw new BusinessException("定义的类型不正确 - "+returntype);
				}
				long size = dataView.size();
				//RuleSetVariableUtil.setVariable(context, returnValue, size);
				ruleVObject.setContextObject(contextType, returnValue,Long.valueOf(size));
			}
		}
	}
	/**
	 * 清空目标DataView
	 */
	private int clearDataView(IDataView dataView) {
		if(dataView==null || dataView.getDatas().isEmpty()) {
			return 0;
		} 
		List<IDataObject> list = dataView.select();
		for (IDataObject dataObject : list) {
			// 实体记录清除
			dataObject.remove();
		} 
		return list.size();
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public IRuleOutputVo evaluate(IRuleContext context) {
		long start1=System.currentTimeMillis();
		/*RuleConfig ruleConfig = context.getRuleConfig();
		List<Map<String, Object>> selectDatas = (List<Map<String, Object>>) ruleConfig
				.getConfigParamValue(Param_ItemsConfig);
		*/
		List<Map<String, Object>> selectDatas = (List<Map<String, Object>>)context.getPlatformInput(Param_ItemsConfig);
		if(VdsUtils.collection.isEmpty(selectDatas)) {
			return context.newOutputVo();//.setMessage("没有映射数据").setSuccess(false);
		} 
		
		//List totalrecordsave = null;
		PageSizeVo pageSizeVo = new PageSizeVo();
		IRuleVObject ruleVObject = context.getVObject();
		IMdo mdo = VDS.getIntance().getMdo(); 
		
		for (Map<String, Object> selectData : selectDatas) {
			String dataSourceName = (String) selectData.get(Param_SourceName);
			if (VdsUtils.string.isEmpty(dataSourceName) || !mdo.hasTable(dataSourceName)) {
				continue;
			}
			
			Object queryOrderBy = selectData.get(Param_OrderBy);
			Object queryPageObject = selectData.get(Param_pager);
			List<Map> queryCondition = (List<Map>) selectData.get(Param_Items);
			String entityName = (String) selectData.get(Param_EntityName);
			String entityTypes = (String) selectData.get(Param_EntityType); 
			
			ContextVariableType entityType = ContextVariableType.getInstanceType(entityTypes);
			IDataView destDataView = (IDataView)  ruleVObject.getContextObject(entityName, entityType);
			//IDataView destDataView = (IDataView) getDataViewWithType(context, entityName, entityType);
			
			//Integer recordStart = new Integer(-1);
			//Integer pageSize = new Integer(-1);
			
			List<Map> pageData = new ArrayList<Map>();
			if (queryPageObject != null && queryPageObject instanceof List) {
				pageData = (List<Map>) queryPageObject;
			}
			
			if(pageData!=null){
				pageSizeVo = getQueryPageNo(pageData);
			}
			///////////
			ITable vtable = mdo.getTable(dataSourceName);
			String orderStr = getOrderbyString(queryOrderBy);

			Boolean isFileAutoMapping = (Boolean)selectData.get(Param_FileAutoMapping)  ;
			if(isFileAutoMapping!=null &&  isFileAutoMapping){
				queryCondition = handleAutoMappingField(destDataView, vtable, queryCondition, entityName, dataSourceName);
			}
			
			QueryParamsVo queryVo = getQueryParamsVo(context, selectData, queryCondition, dataSourceName);
			queryVo.setOrders(orderStr);
			
			long start2=System.currentTimeMillis();
			if(start2 -start1 >10){
				loggerInfo("加载数据库记录查询耗时前1：【"+ (start2 -start1)+"】毫秒");
			}
			
			IDataView dataView = loadDataView(queryVo,vtable.isQuery(),pageSizeVo);
			long start3=System.currentTimeMillis();
			if(pageSizeVo.getTotalRecordSave()!=null) {
				doSaveTotalRecord(ruleVObject, dataView, pageSizeVo.getTotalRecordSave());
			}

			long start4 =System.currentTimeMillis(); 
			if(start4 - start3>10){
				loggerInfo("加载数据库记录查询耗时前2：【"+(start4 - start3)+"】毫秒");
			}
			//清空目标DataView
			clearDataView(destDataView);
		
			if (null != dataView && !VdsUtils.collection.isEmpty(queryCondition)) { 
				insertDataObject(dataView,queryCondition,destDataView);
			}
			long start5=System.currentTimeMillis();
			if(start5-start4>10){
				loggerInfo("加载数据库记录查询耗时4：【"+(start5-start4)+"】毫秒");
			}
		}
		return context.newOutputVo();
	}
	private class FieldMapVo implements IFieldMap{
		private final String destField,sourceField;
		private final FieldType type;
		public FieldMapVo(String sourceField,FieldType type ,String destField) {
			this.destField = destField;
			this.sourceField = sourceField;
			this.type = type;
		}

		@Override
		public String getDestField() { 
			return destField;
		}

		@Override
		public String getSourceField() { 
			return sourceField;
		}

		@Override
		public FieldType getType() { 
			return type;
		}
		
	}
	private void insertDataObject(IDataView sourceDataView,List<Map> queryCondition,IDataView destDataView) {
		List<IFieldMap> fields = new ArrayList<IFieldMap>();
		for (Map map : queryCondition) {
			String destName =  (String) map.get("destName");
			String sourceName =  (String) map.get("sourceName");
			String types  = (String) map.get("type"); 
			FieldType type ;
			if (types.equalsIgnoreCase("entityField")) {
				// 当字段出现.的时候，必须把.去除
				/*if(sourceName.indexOf(".") != -1) {
					sourceName = sourceName.substring(sourceName.lastIndexOf(".") + 1);
				}
				if(destName.indexOf(".") != -1) {
					destName = destName.substring(destName.lastIndexOf(".") + 1);
				}
				if(sourceFieldMap.containsKey(sourceName)){
					sourceFieldMap.put(sourceName, sourceFieldMap.get(sourceName)+","+destName);
				}else{
					sourceFieldMap.put(sourceName, destName);
				}*/
				type = FieldType.EntityField;
				
			} else if (types.equals("expression")) {	
				//value = engine.eval(sourceName);
				//emptyData.put(destName, value);
				type = FieldType.Expression;
			} 
			else {
				throw new ConfigException("不支持的类型:" + types);
			}
			IFieldMap e = new FieldMapVo(sourceName, type, destName);
			fields.add(e);
		}
		IVSQLUtil util = VDS.getIntance().getVSQLUtil();
		util.copyDataView(sourceDataView, fields, destDataView);
	}
	/**
	 * 转实体的树结构为表的树结构
	 * @param mappings
	 * @param treeStructMap
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	private Map<String, Object> dest2SourceTreeStruct(List<Map> mappings, Map<String, Object> treeStructMap){
		// 获取字段映射关系
		List<Map> mappingFields = new ArrayList<Map>();
		for (Map map : mappings) {
			String destName = (String) map.get("destName");
			String sourceName = (String) map.get("sourceName");
			String type = (String) map.get("type");
			if (type.equals("entityField")) {
				// 当字段出现.的时候，必须把.去除
				if (sourceName.indexOf(".") != -1) {
					sourceName = sourceName.substring(sourceName.lastIndexOf(".") + 1);
				}
				if (destName.indexOf(".") != -1) {
					destName = destName.substring(destName.lastIndexOf(".") + 1);
				}
				Map<String, Object> fieldMap = new HashMap<String, Object>();
				fieldMap.put("sourceName", sourceName);
				fieldMap.put("destName", destName);
				mappingFields.add(fieldMap);
			}
		}
		Map<String, Object> newSourceTreeStructMap = new HashMap<String, Object>();
		// 转实体的表结构为表的树结构
		for (String key : treeStructMap.keySet()) {
			boolean isMappingExist = true;
			Object item = treeStructMap.get(key);
			newSourceTreeStructMap.put(key, item);
			if (key.equals("pidField") || key.equals("treeCodeField") || key.equals("orderField")
					|| key.equals("isLeafField")) {
				isMappingExist = checkMappingExist(item, mappingFields);
			}
			if (item != null) {
				if (isMappingExist) {
					for (Map map : mappingFields) {
						if (item.equals(map.get("destName"))) {
							Object value = map.get("sourceName");
							newSourceTreeStructMap.put(key, value);
							break;
						}
					}
				}
				else {
					throw new ConfigException("树结构字段[" + key + "]的映射[" + item + "]不存在");
				}
			}

		}
		return newSourceTreeStructMap;	
	}
	@SuppressWarnings("rawtypes")
	private List<Map> handleAutoMappingField(IDataView source,ITable table, List<Map> mappings, String entityName,String tableName){
		IDataSetMetaData data = source.getMetadata();
		int size =(mappings == null ? 0 : mappings.size());
		List<Map> newMappings = new ArrayList<Map>(size);
		Set<String> existCodes = new HashSet<String>(size);
		
		for (int i = 0; i < size; i++) {
			Map map = mappings.get(i);
			newMappings.add(map);
			String colName = (String) map.get("destName");
			existCodes.add(colName.split("\\.")[1]);
		}
		Map<String,String> sourceInfos = new HashMap<String, String>();
		Set<String> sourceFields = null;
		try {
			//sourceFields = source.getMeta().getColumnNames();
			sourceFields = data.getColumnNames();
		} catch (SQLException e1) {
			log.error("发生SQL错误，忽略",e1);
		}
		List<String> charType = Arrays.asList("char","date","longdate","text");
		List<String> numberType = Arrays.asList("number","integer","float","double","bigdecimal");
		for (String code : sourceFields) {
			if(!table.hasColumn(code)){
				continue;
			}
			String type = "";
			try {
				type = data.getMetaColumnType(code).toString().toLowerCase();
			} catch (SQLException e) {
				log.error("发生SQL错误，忽略",e);
			}
			if(!VdsUtils.string.isEmpty(type)){
				if(charType.indexOf(type)!=-1){
					sourceInfos.put(code, "char");
				}else if(numberType.indexOf(type)!=-1){
					sourceInfos.put(code, "number");
				}else{
					sourceInfos.put(code, type);
				}
			}
		}
		List<IColumn> s = table.getColumns();
		for (IColumn vColumn : s) {
			String type = vColumn.getColumnType().toString().toLowerCase();
			type = charType.indexOf(type)!=-1 ? "char" : (numberType.indexOf(type)!=-1 ? "number" : type);
			String code = vColumn.getColumnName();
			if(!existCodes.contains(code) && sourceInfos.containsKey(code) && sourceInfos.get(code).equals(type)){//如果没配置映射，并且来源有类型和编码都一致的字段
				Map<String,Object> map = new HashMap<String, Object>();
				map.put("destName", entityName + "." + code);
				map.put("sourceName", tableName + "." + code);
				map.put("type", "entityField");
				newMappings.add(map);
				
			}
		}
		return newMappings;
	}
	/**
	 * 判断树结构的映射字段是否存在
	 * @param item
	 * @param mappingFields
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	private boolean checkMappingExist(Object item,List<Map> mappingFields) {
		for (Map map : mappingFields) {
			if(item.equals(map.get("destName"))){
				return true;
			}
		}
	   return false;
	}
			
	/**
	 * 收集日志
	 * @param loggerMessage
	 */
	private static void loggerInfo(String loggerMessage) {
		
		StringBuffer logSb = getLogSb();
		if (logSb == null) {
			log.info(loggerMessage);
		} else {			
			SimpleDateFormat SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS");
			String dateStr = SimpleDateFormat.format(new Date());
			logSb.append(dateStr).append(" - ").append(loggerMessage).append("\n");			
		}
		
		
	}
	
	private static StringBuffer getLogSb() {
		StringBuffer logSb = null;//(StringBuffer) ThreadLocalManager.get("vbase_logger_info");
		return logSb; 
	}
    
	/*
	 * 获取树结构信息
	 */
	private Map<String, Object> getTreeStruct(String tableName, List<Map<String, Object>>treeStructMaps) {
		if (treeStructMaps == null)
			return null;
		for (int  i = 0; i < treeStructMaps.size(); i++) {
			Map<String, Object> treeStructMap = treeStructMaps.get(i);
			if (treeStructMap != null && treeStructMap.get("tableName").equals(tableName)) {
				return treeStructMap;
			}
		}
		return null;
	};
	/**
	 * 
	 * @param context
	 * @param sourceName
	 * @param sourceType
	 * @return 
	private IDataView getDataViewWithType(IRuleContext context, String sourceName, String sourceType) {
		IRuleVObject ruleVObject = context.getVObject();
		ContextVariableType type = ContextVariableType.getInstanceType(sourceType);
		ruleVObject.getContextObject(sourceName, type);
		
		DataView sourceDV = null;
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
		return sourceDV;
	} */

	/**
	 * 获取order by 字符串
	 * 
	 * @param queryOrderBy
	 * @return
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private String getOrderbyString(Object queryOrderBy) {
		
		if (queryOrderBy == null || !(queryOrderBy instanceof List)) {
			return null;
		}
		List<Map<String, Object>> orderFieldList = (List<Map<String, Object>>) queryOrderBy;
		if (VdsUtils.collection.isEmpty(orderFieldList)) {
			return null;
		}
		/////////////////
		String orderStr = "";
		for (Map<String, Object> map: orderFieldList) {
			String fieldName = (String) map.get("field");
			String type = (String) map.get("type");
			if (type == null) {
				type = "asc";
			}
			if (orderStr.length() > 0) {
				orderStr += ",";
			}
			if (fieldName.indexOf(".") != -1) {
				fieldName = fieldName.substring(fieldName.lastIndexOf(".") + 1);
			}
			orderStr += fieldName + " " + type;
		} 
		return orderStr;
	}

	/**
	 * 获取物理表数据
	 * 
	 * @param tableName
	 * @param queryFields
	 * @param queryCondition
	 * @param queryOrderBy
	 * @param queryParams
	 * @param recordStart
	 * @param pageSize
	 * @return
	 */
	/*private IDataView findFromTable(String tableName, List<String> queryFields, String queryCondition,
			String queryOrderBy, Map<String, Object> queryParams, Integer recordStart, Integer pageSize,String condition) {*/
	/**
	 * 获取物理表数据
	 * @param queryVo 
	 * @param queryFields 预留可使用部分字段作为查询条件
	 * @return
	 */
	private IDataView findFromTable(QueryParamsVo queryVo,PageSizeVo pageSizeVo, List<String> queryFields ) { 
		String sql = generateSelectExpression(queryVo.getDataSourceName()
				, queryFields
				, queryVo.getWhereCondition()
				, queryVo.getOrders()
				,queryVo.getTempCondition());

		//DAS das = IMetaDataFactory.getService().das();
		IDAS das = VDS.getIntance().getDas();
		IDataView dataView ;

		Map<String, Object> queryParams = queryVo.getQueryParams();
		if (null == queryParams) {
			queryParams = new HashMap<String, Object>();
		}

		if (pageSizeVo.getRecordStart() <= 0 || pageSizeVo.getPageSize() <= 0) {
			// 无分页的查询
			dataView = das.find(sql, queryParams);
		} else {
			// 有分页的查询
			dataView = das.find(sql, pageSizeVo.getRecordStart(), pageSizeVo.getPageSize(), queryParams);
		}

		return dataView;
	}

	
	/**
	 * 获取自定义查询数据
	 * 
	 * @param queryName
	 * @param extraQueryCondition
	 * @param queryParams
	 * @param recordStart
	 * @param pageSize
	 * @return
	 
	private IDataView findFromQuery(String queryName, String extraQueryCondition, Map<String, Object> queryParams,
			Integer recordStart, Integer pageSize,String condition) {
		*/
	/**
	 * 获取自定义查询数据
	 * @param queryVo
	 * @return
	 */
	private IDataView findFromQuery(QueryParamsVo queryVo ,PageSizeVo pageSizeVo) {
		
		String queryName = queryVo.getDataSourceName();
		String extraQueryCondition = queryVo.getWhereCondition();
		if(!VdsUtils.string.isEmpty(queryVo.getTempCondition())){
			if(VdsUtils.string.isEmpty(extraQueryCondition)){
				extraQueryCondition = "("+queryVo.getTempCondition()+")";
			}else{
				extraQueryCondition = "("+extraQueryCondition + " and "+ queryVo.getTempCondition()+")";
			}
		}
		
		Map<String, Object> queryParams = queryVo.getQueryParams();
		if (null == queryParams) {
			queryParams = new HashMap<String, Object>();
		}
		
		// 调用时，后台可能会有缺参数的情况，所以这里需要做一下补全
		IMdo mdo = VDS.getIntance().getMdo();
		List<IQueryParamsVo> vQueryParams = mdo.getTable(queryName).getQueryParams();
		
		for (IQueryParamsVo param : vQueryParams) {
			String paramName = param.getParamName();
			if (!queryParams.containsKey(paramName)) {
				queryParams.put(paramName, null);
			}
		}
		

		// 查询附加条件组装
		IVSQLConditions extraSqlConditions = null;
		if(!VdsUtils.string.isEmpty(extraQueryCondition)){
			IQueryParse vparse= VDS.getIntance().getVSqlParse();
			extraSqlConditions = vparse.getVSQLConditions(extraQueryCondition);
			//extraSqlConditions = IVSQLConditionsFactory.getService().init();
			//extraSqlConditions.setSqlConStr(extraQueryCondition);
			extraSqlConditions.setLogic(VSQLConditionLogic.AND);
		}
		long start=System.currentTimeMillis();
		IVSQLQuery query = VDS.getIntance().getVSQLQuery();
		
		IDataView dataView ;
		if (pageSizeVo.getRecordStart() <= 0 || pageSizeVo.getPageSize() <= 0) {
			dataView = query.loadQueryData(queryName, queryParams, extraSqlConditions, true);
		} else {
			dataView = query.loadQueryData(queryName, queryParams, extraSqlConditions,
					pageSizeVo.getRecordStart(), pageSizeVo.getPageSize(), true);
		}
		long dua=System.currentTimeMillis()-start;
		if(dua>0){
			loggerInfo("加载数据库记录查询内部耗时1：【"+dua+"】毫秒");
		}			
		return dataView;
	}

	/**
	 * 生成查询sql语句
	 * 
	 * @param tableName
	 * @param queryFields
	 * @param queryCondition
	 * @param queryOrderBy
	 * @return
	 */
	private String generateSelectExpression(String tableName, List<String> queryFields, String queryCondition,
			String queryOrderBy,String condition) {
		
		// 添加查询列
		StringBuffer selectFields = new StringBuffer();
		if (VdsUtils.collection.isEmpty(queryFields)) {
			selectFields.append(" * ");
		} else { 
			for (String fieldName : queryFields) {
				if (selectFields.length() > 0) {
					selectFields.append(", ");
				}
				selectFields.append(fieldName);
			}
		}

		// 添加condition
		if (VdsUtils.string.isEmpty(queryCondition)) {
			queryCondition = "1 = 1";
		}

		// 添加orderby列
		StringBuffer orderBy = new StringBuffer("");
		if (!VdsUtils.string.isEmpty(queryOrderBy)) {
			orderBy.append(" order by ").append(queryOrderBy);
		}

		StringBuffer sql = new StringBuffer("");
		sql.append(" select ");
		sql.append(selectFields);
		sql.append(" from ");
		sql.append(tableName);
		sql.append(" where ");
		if(condition!=null && !condition.equals("")){
			queryCondition =" ( " + queryCondition + " and ("+condition+") ) ";
		}
		sql.append(queryCondition);
		sql.append(orderBy);

		return sql.toString();
	}
}
