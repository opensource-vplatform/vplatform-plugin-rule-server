package com.toone.itop.formula.rule.inte;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
import com.yindangu.v3.business.jdbc.api.model.IColumn;
import com.yindangu.v3.business.jdbc.api.model.IDataSetMetaData;
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
import com.yindangu.v3.business.plugin.execptions.ConfigException;
import com.yindangu.v3.platform.plugin.util.VdsUtils;

public class UpdateRecord extends AbstractRule4Tree implements IRule {
	private static final Logger logger = LoggerFactory.getLogger(UpdateRecord.class);

	public static final String D_RULE_NAME = "保存实体到数据库";
	public static final String D_RULE_CODE = "UpdateRecord";
	public static final String D_RULE_DESC = "";
	
	private static final String D_treeStruct = "treeStruct";
	private static final String D_dataSourceMap = "dataSourceMap";
	private static final String D_ColName="colName",D_DataMap="dataMap";
	
	@SuppressWarnings("rawtypes")
	private boolean initTreeStruct(IDAS das,List<Map> treeStructMapList) {
		//List<Map> treeStructMapList = (List<Map>) context.getPlatformInput(D_treeStruct);
		boolean rs = (treeStructMapList!=null && treeStructMapList.size()>0);
		if(rs){
			das.getContext().init();//DASRuntimeContextFactory.getService().init();
			das.getContext().addTreeStructMaps(treeStructMapList);
		}
		return rs;
	}
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public IRuleOutputVo evaluate(IRuleContext context) {
		//Map<String, Object> inParams = (Map<String, Object>) context.getRuleConfig().getConfigParams();
		boolean isTreeStruct = false;
		IDAS das =  VDS.getIntance().getDas(); 
		try{
			isTreeStruct = initTreeStruct(das, (List<Map>) context.getPlatformInput(D_treeStruct));
			List<Map> saveDatas = (List<Map>) context.getPlatformInput(D_dataSourceMap);
			for (Map saveData : saveDatas) {
				LoadMetaVo loadDatas = loadDatabase(context,saveData );
				Map<String, Map> targetNameMapping = getTargetNameMapping(loadDatas.getMappings());

				Boolean commitType = (Boolean) saveData.get("isSaveAll");
				if(commitType != null && commitType) {
					insertOrUpateTrue(loadDatas,targetNameMapping);
					IDataView dataView = loadDatas.getDataView();
					List<IDataObject> deleteList = dataView.getChanges(DataState.Deleted);
					deleteLogic(das,loadDatas.getTargetName(), deleteList);
				}
				else {
					insertOrUpateFalse(loadDatas,targetNameMapping);
				}
			}
			return context.newOutputVo();
		}finally{
			if(isTreeStruct){
				das.getContext().clear();//DASRuntimeContextFactory.getService().clear();
			}
		}
	}
	
	/**
	 * 加载数据库数据
	 * @param context
	 * @param saveData
	 * @param mappings 操作方式的映射字段
	 * @return 返回数据
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private LoadMetaVo loadDatabase(IRuleContext context, Map<String,Object> saveData) {
		IRuleVObject ruleVObject = context.getVObject();
		// 源表 内存表名
		String sourceName = (String) saveData.get("dataSource");
		String dataSourceTypes = (String) saveData.get("dataSourceType");
		// 目标表
		String targetName = (String) saveData.get("destTab"); 

		if (VdsUtils.string.isEmptyAny(sourceName,targetName)/* || CollectionUtils.isEmpty(mappings)*/) {
			logger.error("规则配置信息错误：来源变量实体名或者目标表名为空.");
			throw new ConfigException("规则配置信息错误！");
		}
		
		//IDataView dataView = (IDataView) getDataViewWithType(context, sourceName, dataSourceType);
		ContextVariableType dataSourceType = ContextVariableType.getInstanceType(dataSourceTypes);
		@SuppressWarnings("deprecation")
		IDataView dataView = (IDataView)ruleVObject.getContextObject( sourceName, dataSourceType);
		if (dataView == null) {
			logger.error("指定的数据源【" + sourceName + "】不存在！");
			throw new ConfigException("指定的数据源【" + sourceName + "】不存在！");
		} 

		List<Map> mappings = (List<Map>) saveData.get(D_DataMap);
		//boolean isFileAutoMapping = saveData.containsKey("isFieldAutoMapping") && (Boolean)saveData.get("isFieldAutoMapping") ? true : false;
		Boolean isFileAutoMapping =  (Boolean)saveData.get("isFieldAutoMapping") ;
		if(isFileAutoMapping!=null && isFileAutoMapping){
			mappings = handleAutoMappingField(dataView, mappings, sourceName, targetName);
		}
		Map<String, Map> targetNameMapping = getTargetNameMapping(mappings);
		
		Map<String, Object> defaultMap = new HashMap<String, Object>();
		Map<String, String> src2Dest = new HashMap<String, String>();
		Map<String, String> expression = new HashMap<String, String>();
		IFormulaEngine en = null; 
		
		for(Entry<String, Map> entry : targetNameMapping.entrySet()) {
			Map map = entry.getValue();
			String key = entry.getKey();
			if (key.indexOf(".") != -1) {
				int index = key.lastIndexOf(".") + 1;
				key = key.substring(index);
			}
			String srcField = (String) map.get("colValue");
			String operType = (String) map.get("valueType");
			
			
			ValueType type = ValueType.getInstanceType(operType);
			switch (type) {
			case TableField:
				if (srcField.indexOf(".") != -1) {
					int index = srcField.lastIndexOf(".") + 1;
					srcField = srcField.substring(index);
				}
				if(src2Dest.containsKey(srcField)){
					String value=src2Dest.get(srcField);
					value=value+","+key;
					src2Dest.put(srcField, value);
				}else{
					src2Dest.put(srcField, key);
				}
//				targetObj.set(key, srcObj.get(srcField));
				break;
			case Expression:
				if(srcField.toUpperCase().contains("GENERATEUUID()") || srcField.toUpperCase().contains("RANDOM(")) {							
					expression.put(key, srcField);
				}
				if(en==null) {
					en = VDS.getIntance().getFormulaEngine();
				}
				defaultMap.put(key, en.eval(srcField)); 
				break;
			default:
				throw new ConfigException("不支持的映射中的操作类型！");
			}
		}
		
		List<Map<String, Object>> dataList = dataView.getDatas(src2Dest, defaultMap);
		if(!VdsUtils.collection.isEmpty(dataList) && expression.size() > 0) { 
			for(Map<String, Object> data : dataList) {
				for(Entry<String, String> entry : expression.entrySet()) {
					data.put(entry.getKey(), en.eval(entry.getValue()));							
				}
			}
		}
		LoadMetaVo sm = new LoadMetaVo();
		sm.setSourceName(sourceName)
			.setTargetName(targetName)
			.setDataView(dataView)
			.setDataList(dataList)
			.setMappings(mappings);;
		return sm;
	}
	@SuppressWarnings("rawtypes")
	private Map<String, Map> getTargetNameMapping(List<Map> mappings) {
		//boolean isContainIdColumn = false;
		Map<String, Map> targetNameMapping = new HashMap<String, Map>(); 
		for (Map map : mappings) {
			String destField = (String) map.get(D_ColName);
			if (destField != null && destField.length()>0) {
				String _destField = destField;
				if (_destField.indexOf(".") != -1) {
					_destField = _destField.split("\\.")[1];
				}
				/*if ("id".equalsIgnoreCase(_destField)) {
					isContainIdColumn = true;
				} */
				targetNameMapping.put(_destField, map);
			}
		} 
		return targetNameMapping;
	}
	@SuppressWarnings("rawtypes")
	private boolean isContainIdColumn(Map<String, Map> targetNameMapping) {
		boolean rs = targetNameMapping.containsKey("_id") 
				|| targetNameMapping.containsKey("_ID") 
				|| targetNameMapping.containsKey("_Id")
				|| targetNameMapping.containsKey("_iD");
		return rs;
	}
	
	private class LoadMetaVo{
		private String sourceName;
		private String targetName;
		private List<Map<String, Object>> dataList;  
		
		@SuppressWarnings("rawtypes")
		private List<Map> mappings;
		private IDataView dataView;
		
		public String getSourceName() {
			return sourceName;
		}
		public LoadMetaVo setSourceName(String sourceName) {
			this.sourceName = sourceName;
			return this;
		}
		public String getTargetName() {
			return targetName;
		}
		public LoadMetaVo setTargetName(String targetName) {
			this.targetName = targetName;
			return this;
		}
		public List<Map<String, Object>> getDataList() {
			return dataList;
		}
		public LoadMetaVo setDataList(List<Map<String, Object>> dataList) {
			this.dataList = dataList;
			return this;
		}
		/*public Map<String, Map> getTargetNameMapping() {
			return targetNameMapping;
		}
		public SaveMetaVo setTargetNameMapping(Map<String, Map> targetNameMapping) {
			this.targetNameMapping = targetNameMapping;
			return this;
		}*/
		public IDataView getDataView() {
			return dataView;
		}
		public LoadMetaVo setDataView(IDataView dataView) {
			this.dataView = dataView;
			return this;
		}
		/**操作方式的映射字段*/
		@SuppressWarnings("rawtypes")
		public List<Map> getMappings() {
			return mappings;
		}
		/**操作方式的映射字段*/
		@SuppressWarnings("rawtypes")
		public LoadMetaVo setMappings(List<Map> mappings) {
			this.mappings = mappings;
			return this;
		}
	}
	/** 
	 * 
	 * @param datalist 对应 DataView.getDatas
	 * @param isContainIdColumn 选择“InsertOrUpate方式更新， 提交全部内存表数据
	 */
	@SuppressWarnings("rawtypes")
	private void insertOrUpateTrue(LoadMetaVo loadDataVo,Map<String, Map> targetNameMapping) { 
		List<Map<String, Object>> dataList = loadDataVo.getDataList();
		String targetName = loadDataVo.getTargetName(),sourceName = loadDataVo.getSourceName();
		IDAS das = VDS.getIntance().getDas();
		
		boolean isContainIdColumn = isContainIdColumn(targetNameMapping);
		Map<String, Map<String, Object>> dataListMap = new HashMap<String, Map<String, Object>>();
		Map<String, String> idPairs = Collections.emptyMap();
		if(isContainIdColumn) {
			idPairs = getNewIdAndOldIdPair( dataList, targetNameMapping);
		}
		List<String> idParams = new ArrayList<String>();
		for (Map<String, Object> obj : dataList) {
			String id = (String) obj.get("id");
			if (!VdsUtils.string.isEmpty(id)) {
				String idKey = idPairs.get(id);
				if(VdsUtils.string.isEmpty(idKey)) {
					idKey = id  ;
				}
//					
				idParams.add(idKey);
				Map<String, Object> temp = dataListMap.get(idKey);
				if (temp != null) {
					throw new ConfigException("实体"+ sourceName+"或"+loadDataVo.getTargetName()
						+"的ID列重复,id=[" + idKey + "],重复记录数据的原始id=["
						+ temp.get("id") + ", " + id + "]");
				}
				dataListMap.put(idKey, obj);
			}
		}
		Map<String, List<String>> params = new HashMap<String, List<String>>();
		params.put("ids", idParams);
		IDataView targetDV = null;
		if (VdsUtils.collection.isEmpty(idParams)) {
			targetDV = das.find("select * from " + targetName + " where 1<>1");
		}
		else {
			targetDV = das
					.findWithNoFilter("select * from " + targetName + " where id in (:ids)", params);
		}
		if (targetDV != null) {
			List<IDataObject> targetDataList = targetDV.select();
			for (IDataObject obj : targetDataList) {
				Map<String, Object> srcObj = dataListMap.get(obj.getId());
				if (srcObj != null) {
					dealObject(srcObj, null, obj, targetNameMapping);
					dataList.remove(srcObj);
				}
			}
		}
		
		if(dataList.size() > 0) {
			targetDV.insertDataObject(dataList);
		}

		targetDV.acceptChanges();
	}
	/** 
	 * 
	 * @param datalist 对应 DataView.getDatas
	 * @param isContainIdColumn 选择“InsertOrUpate方式更新， 提交全部内存表数据
	 */
	@SuppressWarnings("rawtypes")
	private void insertOrUpateFalse(LoadMetaVo loadDataVo,Map<String, Map> targetNameMapping)  {// 提交的数据为修改过的数据，即新增、修改、删除的
		IDataView dataView = loadDataVo.getDataView();
		String targetName = loadDataVo.getTargetName(),sourceName = loadDataVo.getSourceName();
		IDAS das = VDS.getIntance().getDas();
		boolean isContainIdColumn = isContainIdColumn(targetNameMapping);
		// 配置了ID项的
		if (isContainIdColumn) {
			List<IDataObject> addList = dataView.getChanges(DataState.Added);
			List<IDataObject> modifiedList = dataView.getChanges(DataState.Modified);
			// 1. 检查计算后的ID重复异常， 2. 不管什么情况，先新增，后修改
			Map<String, IDataObject> addDataObjects = new HashMap<String, IDataObject>();
			for (IDataObject obj : addList) {
				if (VdsUtils.string.isEmpty(obj.getId())) {
					addDataObjects.put(VdsUtils.uuid.generate(), obj);
				} else {
					String newId = (String) getColumnValue("id", null, obj, targetNameMapping);
					IDataObject isContain = addDataObjects.get(newId);
					if (null != isContain) {
						throw new ConfigException("实体"+sourceName+"或"+targetName+"的ID列重复,id=[" + newId + "],重复记录数据的原始id=["
								+ obj.getId() + "]");
					}
					addDataObjects.put(newId, obj);
				}
			}
			addLogic(das, targetName, targetNameMapping, addList);
			updateLogic(das,targetName, targetNameMapping, modifiedList);
		} else {
			List<IDataObject> addList = dataView.getChanges(DataState.Added);
			addLogic(das,targetName, targetNameMapping, addList);
		}
		List<IDataObject> deleteList = dataView.getChanges(DataState.Deleted);
		deleteLogic(das,targetName, deleteList); 
	}

	@SuppressWarnings("rawtypes")
	private List<Map> handleAutoMappingField(IDataView source, List<Map> mappings, String entityName,String tableName){
		//VTable table = IMDOFactory.getService().getTable(tableName);
		IMdo mdo = VDS.getIntance().getMdo();
		ITable table = mdo.getTable(tableName); 
		
		Set<String> existCodes = new HashSet<String>();
		List<Map> newMappings = new ArrayList<Map>();
		int size = (mappings == null ? 0 : mappings.size());
		for (int i = 0; i < size; i++) {
			Map map = mappings.get(i);
			newMappings.add(map);
			String colName = (String) map.get("colName");
			existCodes.add(colName.split("\\.")[1]);
		} 
		
		final List<String> charType = Arrays.asList("char","date","longdate","text");
		final List<String> numberType = Arrays.asList("number","integer","float","double","bigdecimal");
		Map<String,String> sourceInfos = getSourceFieldInfo(source, charType, numberType);
		
		List<IColumn> s = table.getColumns();
		for (IColumn vColumn : s) {
			String type = vColumn.getColumnType().toString().toLowerCase();
			type = charType.indexOf(type)!=-1 ? "char" : (numberType.indexOf(type)!=-1 ? "number" : type);
			String code = vColumn.getColumnName();
			if(!existCodes.contains(code) && sourceInfos.containsKey(code) && sourceInfos.get(code).equals(type)){//如果没配置映射，并且来源有类型和编码都一致的字段
				Map<String,Object> map = new HashMap<String, Object>();
				map.put("colValue", entityName + "." + code);
				map.put("colName", tableName + "." + code);
				map.put("valueType", "entityField");
				newMappings.add(map);
				
			}
		}
		return newMappings;
	}
	private Map<String,String> getSourceFieldInfo(IDataView source ,List<String> charType ,List<String> numberType){
		IDataSetMetaData data = source.getMetadata();
		Set<String> sourceFields = null;
		try {
			sourceFields = data.getColumnNamesAll();
		} catch (SQLException e1) {
			logger.warn("sql错误,忽略", e1);
		}
		if(VdsUtils.collection.isEmpty(sourceFields)) {
			return Collections.emptyMap();
		}
			
		Map<String,String> sourceInfos = new HashMap<String, String>();
		for (String code : sourceFields) {
			String type = null;
			try {
				type = data.getMetaColumnType(code).toString().toLowerCase();
			} catch (SQLException e) {
				logger.warn("getMetaColumnType发生SQL错误，忽略",e);
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
		
		return sourceInfos;
	}
	/**
	 * 将modifiedList中的数据，按照targetNameMapping映射关系转化之后再更新到targetName中
	 * 
	 * @param targetName
	 * @param targetNameMapping
	 * @param addList
	 */
	@SuppressWarnings("rawtypes")
	private void updateLogic(IDAS das, String targetName, Map<String, Map> targetNameMapping, List<IDataObject> modifiedList) {
		if (VdsUtils.collection.isEmpty(modifiedList)) {
			return;
		}
		Map<String, List<String>> params = new HashMap<String, List<String>>();
		List<String> idParams = new ArrayList<String>();
		Map<String, IDataObject> modifiedMap = new HashMap<String, IDataObject>();
		for (IDataObject obj : modifiedList) {
			// obj.getId()如果为空，不需要处理，使用UUID产生的数据在数据库中不可能存在
			if (!VdsUtils.string.isEmpty(obj.getId())) {
				String newId = (String) getColumnValue("id", null, obj, targetNameMapping);
				if (!VdsUtils.string.isEmpty(newId)) {
					modifiedMap.put(newId, obj);
					idParams.add(newId);
				}
			}
		}
		params.put("ids", idParams);
		IDataView targetDV = das // IMetaDataFactory.getService().das()
				.findWithNoFilter("select * from " + targetName + " where id in (:ids)", params);
		List<IDataObject> updateList = targetDV.select();
		for (IDataObject targetObj : updateList) {
			IDataObject object = modifiedMap.get(targetObj.getId());
			dealObject(null, object, targetObj, targetNameMapping);
		}
		targetDV.acceptChanges();
	}

	/**
	 * 将addList中的数据，按照targetNameMapping映射关系转化之后再插入到targetName中
	 * 
	 * @param targetName
	 * @param targetNameMapping
	 * @param addList
	 */
	@SuppressWarnings("rawtypes")
	private void addLogic(IDAS das,String targetName, Map<String, Map> targetNameMapping, List<IDataObject> addList) {
		//IDataView targetDV = IMetaDataFactory.getService().das().find("select * from " + targetName + " where 1<>1");
		IDataView targetDV = das.find("select * from " + targetName + " where 1<>1");
		for (IDataObject object : addList) {
			IDataObject targetObj = targetDV.insertDataObject();
			dealObject(null, object, targetObj, targetNameMapping);
			if (VdsUtils.string.isEmpty(targetObj.getId())) {
				targetObj.setId(VdsUtils.uuid.generate());
			}
		}
		targetDV.acceptChanges();
	}

	/**
	 * 从targetName中删除deleteList中的数据
	 * 
	 * @param targetName
	 * @param deleteList
	 */
	private void deleteLogic(IDAS das,String targetName, List<IDataObject> deleteList) {
		if (VdsUtils.collection.isEmpty(deleteList)) {
			return;
		}
		//IDAS das = VDS.getIntance().getDas();
		List<String> idParams = new ArrayList<String>();
		for (IDataObject dataObject : deleteList) {
			if (!VdsUtils.string.isEmpty(dataObject.getId())) {
				idParams.add(dataObject.getId());
			}
		}
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("ids", idParams);
		das.executeUpdate("delete from " + targetName + " where id in (:ids)", params);
	}

	/**
	 * 如果目标列中存在id列的设置， 计录老id和新id之间的关系
	 * 
	 * @param isContainIdColumn
	 * @param dataList
	 * @param targetNameMapping
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	private Map<String, String> getNewIdAndOldIdPair(List<Map<String, Object>> dataList,
			Map<String, Map> targetNameMapping) {
		Map<String, String> idPairs = new HashMap<String, String>(); 
		for (Map<String, Object> obj : dataList) {
			if (!VdsUtils.string.isEmpty((String)obj.get("id"))) {
				String newId = (String) getColumnValue("id", obj, null, targetNameMapping);
				idPairs.put((String)obj.get("id"), newId);
			}
		} 
		return idPairs;
	}

	/**
	 * 获取某一列数据的值
	 * 
	 * @param columnName
	 * @param data
	 * @param targetNameMapping
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	private Object getColumnValue(String columnName, Map<String, Object> data, IDataObject obj, Map<String, Map> targetNameMapping) {
		Object value = null;
		Map map = (Map) targetNameMapping.get(columnName);
		if (map == null) {
			throw new ConfigException("映射信息中不存在该列数据，列名[" + columnName + "]！");
		}
		String operType = (String) map.get("valueType");
		String srcField = (String) map.get("colValue");
		// 字段操作
		ValueType type = ValueType.getInstanceType(operType);
		switch (type) {
		case TableField:
			if(data == null) {
				value = obj.get(srcField);
			} else {
				value = data.get(srcField);					
			}
			break;
		case Expression:
			IFormulaEngine en = VDS.getIntance().getFormulaEngine();
			value = en.eval(srcField);//FormulaEngineFactory.getFormulaEngine
			break;
		default:
			throw new ConfigException("不支持的映射中的操作类型！");
		}
		return value;
	}

	/**
	 * 按照映射关系，设置目标对象的值
	 * 
	 * @param srcObj
	 * @param targetObj
	 * @param targetNameMapping
	 * @throws SQLException
	 */
	@SuppressWarnings({ "rawtypes" })
	private void dealObject(Map<String, Object> srcObjMap, IDataObject srcObj, IDataObject targetObj, Map<String, Map> targetNameMapping) {
		for (Iterator it = targetNameMapping.keySet().iterator(); it.hasNext();) {
			String key = (String) it.next();
			String mapKey = key;
			if (key.indexOf(".") != -1) {
				int index = key.lastIndexOf(".") + 1;
				key = key.substring(index);
			}
			Map map = (Map) targetNameMapping.get(mapKey);
			if (map != null) {
				String operType = (String) map.get("valueType");
				String srcField = (String) map.get("colValue");
				// 字段操作
				ValueType type = ValueType.getInstanceType(operType);
				switch (type) {
				case TableField:
					if (srcField.indexOf(".") != -1) {
						int index = srcField.lastIndexOf(".") + 1;
						srcField = srcField.substring(index);
					}
					if(srcObjMap == null) {						
						targetObj.set(key, srcObj.get(srcField));
					} else {						
						targetObj.set(key, srcObjMap.get(key));
					}
					break;
				case Expression://FormulaEngineFactory.getFormulaEngine()
					IFormulaEngine en = VDS.getIntance().getFormulaEngine();
					targetObj.set(key, en.eval(srcField));
					break;
				default:
					throw new ConfigException("不支持的映射中的操作类型！");
				}
			}
		}
	}

	enum ValueType {

		TableField("entityField"),

		Expression("expression"),
		// 活动集输出变量
		RuleSetOutput("ruleSetOutput"),
		// 活动集输出变量
		RuleSetInput("ruleSetInput"),
		// 活动集上下文变量
		RuleSetVar("ruleSetVar"),
		// 窗体实体
		Window("window");

		private String type;

		private ValueType(String type) {
			this.type = type;
		}

		public static ValueType getInstanceType(String key) {
			ValueType ret = null;
			for (ValueType type : ValueType.values()) {
				if (key.equals(type.type)) {
					ret = type;
				}
			}
			return ret;
		}
	}

	/**
	 * 
	 * @param context
	 * @param sourceName
	 * @param sourceType
	 * @return
	
	private DataView getDataViewWithType(RuleContext context, String sourceName, String sourceType) {
		DataView sourceDV = null;
		switch (ValueType.getInstanceType(sourceType)) {
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
}