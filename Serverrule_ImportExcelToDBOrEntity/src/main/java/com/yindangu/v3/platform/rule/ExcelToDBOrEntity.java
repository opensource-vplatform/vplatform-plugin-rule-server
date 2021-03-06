package com.yindangu.v3.platform.rule;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.toone.v3.platform.rule.model.ColumnImportCfg;
import com.toone.v3.platform.rule.model.ExcelImportDataCfg;
import com.toone.v3.platform.rule.model.FModelMapper;
import com.toone.v3.platform.rule.model.FProperty;
import com.toone.v3.platform.rule.model.TreeType;
import com.toone.v3.platform.rule.util.ObjPropertyUtil2;
import com.yindangu.commons.CaseInsensitiveLinkedMap;
import com.yindangu.v3.business.VDS;
import com.yindangu.v3.business.formula.api.IFormulaEngine;
import com.yindangu.v3.business.jdbc.api.model.ColumnType;
import com.yindangu.v3.business.jdbc.api.model.ITable;
import com.yindangu.v3.business.metadata.api.IDataObject;
import com.yindangu.v3.business.metadata.api.IDataView;
import com.yindangu.v3.business.plugin.business.api.rule.ContextVariableType;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleContext;
import com.yindangu.v3.business.plugin.execptions.ConfigException;
import com.yindangu.v3.business.vds.IVDS;
import com.yindangu.v3.platform.excel.MergedType;
import com.yindangu.v3.platform.plugin.util.VdsUtils;
import com.yindangu.v3.platform.rule.SaveDataBaseAction.ExcelContextType;
import com.yindangu.v3.platform.rule.SaveDataBaseAction.ExcelResultVo;
import com.yindangu.v3.platform.rule.SaveDataBaseAction.ParamsVo;
import com.yindangu.v3.platform.rule.SaveDataBaseAction.RepeatType;

class ExcelToDBOrEntity{
	private static final Logger logger = LoggerFactory.getLogger(ExcelToDBOrEntity.class);
    private static final String IMPORTBY = "importBy";
    private static final String BIZCODEFIELD = "filiationField";
    private static final String BIZCODEFORMAT = "filiationFormat";
    private static final String BIZCODECONFIG = "filiationConfig";
    
    
//    private ExcelResultVo result;
//    private ParamsVo params;
//    
//    protected class CellValue{
//    	private final int row,col;
//    	private int sheetNo;
//    	private Object value;
//    	private String sheetName;
//    	/**
//    	 * @param row ?????????1??????
//    	 * @param col ?????????1??????
//    	 */
//    	public CellValue(int row,int col) {
//    		this.row = row;
//    		this.col = col;
//    	}
//		public int getRow() {
//			return row;
//		}
//		public int getCol() {
//			return col;
//		}
//		public int getSheetNo() {
//			return sheetNo;
//		}
//		public Object getValue() {
//			return value;
//		}
//		public String getSheetName() {
//			return sheetName;
//		}
//    }
    /** 
     * ?????????
     * @param context
     * @param cfgMap
     * @param inputStream
     * @param type
     * @return ??????sheetName
     */
	public ExcelResultVo importToDB(IRuleContext context, Map<String, Object> cfgMap, InputStream inputStream,MergedType type) {
		try {
			SaveDataBaseAction act = new SaveDataBaseAction();
			ParamsVo params = parseParamsVo(context, cfgMap);
			params.setMergedType(type);
	    	ExcelResultVo excelVo =  act.saveDataBase(params, inputStream);
	    	
	    	IDataView dv = excelVo.getDataView();
	    	// ????????????
	        handleResultDataView(dv, params.getTableName(), params.getTargetType(), context); 
	        
	    	return excelVo  ;
		}
		finally {
			;//close(inputStream); //?????????????????????
		}
    }


	private IVDS getVDS() {
        return VDS.getIntance();
    }
	private int findIgnoreCase(List<String> rds,String key) {
		int size = (rds == null ? 0 : rds.size());
		int rs =-1;
		for(int i =0 ; i < size;i++){
			if(key.equalsIgnoreCase(rds.get(i))) {
				rs = i;
				break;
			}
		}
		return rs;
	}
    @SuppressWarnings("unchecked")
	private SaveDataBaseAction.ParamsVo parseParamsVo(IRuleContext context, Map<String, Object> cfgMap) {
    	 List<Map<String, Object>> mappings = (List<Map<String, Object>>) cfgMap.get("mapping");// ????????????
         String tableName = (String) cfgMap.get("target"); // ?????????????????????
         ExcelContextType targetType =ExcelContextType.getInstanceType((String) cfgMap.get("targetType")); // ????????????
         //boolean isPhyscTable = targetType.equals("table"); // ???????????????
         //int startRow = Integer.valueOf((String) cfgMap.get("dataStartRow")); //????????????


         ParamsVo vo = new ParamsVo();
         vo.setTargetType(targetType);
         // ???????????????????????????????????????
         String repeatOperation = (String) cfgMap.get("repeatOperation");
         vo.setRepeatOperation(VdsUtils.string.isEmpty(repeatOperation) ? RepeatType.Repeat: RepeatType.getType(repeatOperation));

         {
	         Map<String, Object> expressionFieldMap = new HashMap<String, Object>(); // ????????????????????????
	         boolean isImportId = getOtherData(mappings, expressionFieldMap); // ????????????id??????
	         List<String> checkItems = (List<String>) cfgMap.get("checkItems"); // ???????????????????????????
	         // ???????????????id???????????????id????????????????????????id
	         if (checkItems == null) {
	 			checkItems = Collections.emptyList();
	 		 }
	         if (isImportId ) { //??????id?????????????????????id????????????(???????????????????????????????????????)
	        	 if(checkItems.isEmpty()) {
	        		 checkItems = Collections.singletonList("id");
	        	 }
	        	 else if(findIgnoreCase(checkItems,"id") == -1) { 
	        		 //????????????id?????????????????????id?????????????????????????????????????????????????????????????????????
	        		 throw new ConfigException("??????id??????????????????????????????????????????????????????id?????????");
	        	 }
	         }
	         vo.setCheckItems(checkItems);
	         vo.setExpressionFieldMap(expressionFieldMap);
	         vo.setImportId(isImportId);
         }

         String cfgStr = VdsUtils.json.toJson(cfgMap);
         ExcelImportDataCfg dCfg = VdsUtils.json.fromJson(cfgStr, ExcelImportDataCfg.class);
         IDataView sourceDataView = null;
     

         IDataView dataView;
         if (vo.isPhyscTable()) {
        	 dataView = getVDS().getDas().createDataViewByName(tableName);// ?????????
         } else {
        	 dataView = getDataViewWithType(context, tableName, targetType);// ?????????
        	 sourceDataView = dataView;//getDataViewWithType(context, tableName, targetType);// ????????????????????????
         }

         FModelMapper mapper = getModelMapperByExcelImportCfg(dCfg, sourceDataView);
         int sheetIndex = -2;
         try {
        	 Object sheetNum = cfgMap.get("sheetNum");
             sheetNum = getVDS().getFormulaEngine().eval(sheetNum.toString());
             sheetIndex = Integer.parseInt(sheetNum.toString());
         } catch (Exception e) {
             throw new ConfigException("???????????????sheet????????????", e);
         }

         String importBy = (String) cfgMap.get(IMPORTBY);
         vo.setImportBy(("filiation").equalsIgnoreCase(importBy));
         
         vo.setTargetDataView(dataView);
         vo.setSourceDataView(sourceDataView);
         //vo.setPhyscTable(isPhyscTable);
         vo.setModelMapper(mapper);
         vo.setSheetIndex(sheetIndex);
         vo.setTableName(tableName);
         
         {
        	// ?????????(???)
             List<Map> treeStruct = (List<Map>) cfgMap.get("treeStruct");
             Map treeStructMap = (treeStruct == null || treeStruct.isEmpty() ? null : treeStruct.get(0));//
             boolean isTree =( treeStructMap != null && !TreeType.BizCode.equals(treeStructMap.get("type")));
             vo.setTree(isTree);
             vo.setTreeStructMap(treeStructMap);
         }
         
         String bizCodeField = (String) cfgMap.get(BIZCODEFIELD)  ;
         String bizCodeFormat = (String) cfgMap.get(BIZCODEFORMAT);
         String bizCodeConfig = (String)cfgMap.get(BIZCODECONFIG);
         vo.setBizCodeConfig(bizCodeConfig);
         vo.setBizCodeFormat(bizCodeFormat);
         vo.setBizCodeField(bizCodeField);
         return vo;
    }
  //private void handleResultDataView(IDataView dataView, String tableName, String targetType, IRuleContext context) {
    private void handleResultDataView(IDataView dataView, String tableName, ExcelContextType targetType, IRuleContext context) {
        if (dataView == null) {
        	return ;
        }
        	/*
            try {
                if (targetType.equals("ruleSetInput") || targetType.equals("ruleSetOutput") || targetType.equals("ruleSetVar")) {
                    context.getVObject().setContextObject(ContextVariableType.getInstanceType(targetType), tableName, dataView);
                } else if (targetType.equals("table")) {
                    try {
                        dataView.acceptChanges();
                    } catch (Exception e) {
                        throw new ConfigException("?????????????????????" + e.getMessage(), e);
                    }
                } else {
                    throw new ConfigException("???????????????[" + targetType + "]??????????????????.");
                }
            } catch (Exception e) {
                throw new BusinessException("??????Excel: ???????????????" + e.getMessage(), e);
            }*/
        if(targetType == null) {
        	throw new ConfigException("??????Excel: ????????????,VariableType???????????????");
        }
        switch (targetType) {
		case RuleSetInput:
		case RuleSetOutput:
		case RuleSetVar:
			context.getVObject().setContextObject(targetType.getType(), tableName, dataView);
			break;
		case Table:
			//???????????????
			//dataView.acceptChanges();
			break;
		default:
			throw new ConfigException("???????????????[" + targetType + "]??????????????????.");
		}
    }
    /**
	 * ???????????????
	 * 
	 * @param context ???????????????
	 * @param sourceName ????????????
	 * @param sourceType ????????????
	 * @return
	 */
	private IDataView getDataViewWithType(IRuleContext context, String sourceName, ExcelContextType sourceType) {
		
		IDataView sourceDV = null;
		if (ExcelContextType.Table == sourceType) { 
			sourceDV = getVDS().getDas().createDataViewByName(sourceName);
			return sourceDV;
		}
		/*
		 * if ("ruleSetInput".equalsIgnoreCase(sourceType) || "ruleSetOutput".equalsIgnoreCase(sourceType) || "ruleSetVar".equalsIgnoreCase(sourceType)) {
            ContextVariableType instanceType = ContextVariableType.getInstanceType(sourceType);
            sourceDV = (IDataView) context.getVObject().getContextObject(sourceName, instanceType);
        } else if ("table".equalsIgnoreCase(sourceType)) {   // ????????????
            sourceDV = getVDS().getDas().createDataViewByName(sourceName);
        } else {
            throw new ConfigException("??????Excel?????? : ???????????????[" + sourceType + "]??????????????????.");
        }
		 */
		
		ContextVariableType t = sourceType.getType();// ContextVariableType.getInstanceType(sourceType);
		if(t == null) {
			throw new ConfigException("??????Excel?????? : ??????????????????????????????[" + sourceType + "]??????????????????.");
		}
		switch (t) {
			case RuleSetInput:
			case RuleSetOutput:
			case RuleSetVar:
				sourceDV = (IDataView) context.getVObject().getContextObject(sourceName,t);
				break;
			 
			default:
				throw new ConfigException("??????Excel?????? : ????????????????????????[" + sourceType + "]??????????????????.");
				// throw new ExpectedException("??????Excel?????? : ???????????????[" + sourceType + "]??????????????????.");
		}
		return sourceDV;
	}

    /**
     * 
     * @param dCfg
     * @param sourceDataView ?????????null??????????????????
     * @return
     */
    @SuppressWarnings("unchecked")
	private FModelMapper getModelMapperByExcelImportCfg(ExcelImportDataCfg dCfg, IDataView sourceDataView) {
        FModelMapper mapper = new FModelMapper();
        mapper.setStartRow(dCfg.getDataStartRow());
        if (mapper.getStartRow() <= 0) {
            throw new ConfigException("Excel??????????????????????????????????????????1");
        }
        List<ColumnImportCfg> list = dCfg.getMapping();{
	        List<String> fieldNameList = ObjPropertyUtil2.getPropertyList(list, "fieldCode");
	        Set<String> fieldNameSet = new LinkedHashSet<String>(fieldNameList);
	        if (fieldNameSet.size() < fieldNameList.size()) {
	            throw new ConfigException("Excel??????????????????????????????");
	        }
        }
        ITable table = (sourceDataView == null ? getVDS().getMdo().getTable(dCfg.getTarget())  : null);
        Map<String, FProperty> properties = new CaseInsensitiveLinkedMap();
        for (int i = 0; i < list.size(); i++) {
            ColumnImportCfg columnCfg = list.get(i);
            String source = columnCfg.getSourceType();

            // ?????????????????????excelColName???excelColNum????????????excel???????????????
            if (source != null && !source.equals("excelColName") && !source.equals("excelColNum")) {
                continue;
            }
            FProperty property = new FProperty();
            property.setName(columnCfg.getFieldCode());
            if (!isNotEmpty(columnCfg.getSourceValue())) {
                if (isNotEmpty(columnCfg.getFieldName()))
                    property.setTitle(columnCfg.getFieldName().trim());
                else
                    property.setTitle(columnCfg.getFieldCode());
            } else {
                property.setTitle(columnCfg.getSourceValue().trim());
            }

            if (source.equals("excelColNum")) {// ????????????????????????
                if (property.getTitle().toUpperCase().matches("[A-Z]+")) {
                    int columnIndex = 0;
                    char[] chars = property.getTitle().toUpperCase().toCharArray();
                    for (int j = 0; j < chars.length; j++) {
                        columnIndex += ((int) chars[j] - (int) 'A' + 1) * (int) Math.pow(26, chars.length - j - 1);
                    }
                    property.setColumn(columnIndex - 1);
                }
            }

            if (isNotEmpty(dCfg.getTarget())) {
                String columnName = columnCfg.getFieldCode();
                ColumnType type ;
                if (sourceDataView == null) {
                    type = table.getColumn(columnName).getColumnType();
                }
                else {
                    // ???????????????????????????????????????????????????????????????????????????
                    try {
                        type = sourceDataView.getMetadata().getColumn(columnName).getColumnType();
                    } catch (Exception e) {
                        throw new ConfigException("????????????????????????.", e);
                    }
                }
                property.setType(type);
            }

            properties.put(columnCfg.getFieldCode(), property);

        }
        mapper.setProperties(properties);
        return mapper;

    }
    
    private boolean isNotEmpty(String str) {
        return  (str != null && str.length()>0) ;/*{
            return true;
        }

        return false;*/
    }
    /**
     * 1????????????????????????????????????????????????????????????null??? 2 ???????????????????????????id??????
     *
     * @param mappings ????????????
     * @return
     */
    private boolean getOtherData(List<Map<String, Object>> mappings, Map<String, Object> expressionFieldMap) {
        boolean isImportIdTag = false;
        IFormulaEngine en = getVDS().getFormulaEngine();
        for (Map<String, Object> map : mappings) {
        	String fieldCode = (String)map.get("fieldCode");
        	String type = (String) map.get("sourceType");
        	
        	if (!isImportIdTag && "id".equalsIgnoreCase(fieldCode)) {
                isImportIdTag = true;
            }
            
            if ("expression".equals(type)) {
                Object sourceValue = en.eval(map.get("sourceValue").toString());
                if (null == sourceValue) {
                    logger.warn("??????[" + fieldCode + "????????????????????????????????????????????????]");
                }
                else {
                	expressionFieldMap.put(fieldCode, sourceValue);
                }
            }
        }
        return isImportIdTag;
    }
}