package com.yindangu.v3.platform.rule;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
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
import com.yindangu.v3.business.file.api.model.IAppFileInfo;
import com.yindangu.v3.business.jdbc.api.model.ColumnType;
import com.yindangu.v3.business.jdbc.api.model.ITable;
import com.yindangu.v3.business.metadata.api.IDataView;
import com.yindangu.v3.business.plugin.business.api.rule.ContextVariableType;
import com.yindangu.v3.business.plugin.business.api.rule.IRule;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleContext;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleOutputVo;
import com.yindangu.v3.business.plugin.execptions.ConfigException;
import com.yindangu.v3.business.plugin.execptions.PluginException;
import com.yindangu.v3.business.vds.IVDS;
import com.yindangu.v3.platform.excel.MergedType;
import com.yindangu.v3.platform.plugin.util.VdsUtils;
import com.yindangu.v3.platform.rule.SaveDataBaseAction.ExcelContextType;
import com.yindangu.v3.platform.rule.SaveDataBaseAction.ParamsVo;

/**
 * 导入excel数据，第一版，与开发系统绑死的
 * @Author xugang
 * @Date 2021/7/23 16:19
 */
public class ImportExcelToDBOrEntity implements IRule {

    private static final String IMPORTBY = "importBy";
    private static final String BIZCODEFIELD = "filiationField";
    private static final String BIZCODEFORMAT = "filiationFormat";
    private static final String BIZCODECONFIG = "filiationConfig";
    
    
    private static final Logger logger = LoggerFactory.getLogger(ImportExcelToDBOrEntity.class);

    @SuppressWarnings("unchecked")
	@Override
    public IRuleOutputVo evaluate(IRuleContext context) {
        long starTime = System.currentTimeMillis();
        String fileId = getVDS().getFormulaEngine().eval(context.getPlatformInput("fileSource").toString()); // 文件标识
        if (fileId == null || fileId.length()==0) { 
            throw new ConfigException("后台导入Excel规则，获取文件ID标识为空，请检查");
        }
        
        InputStream inputStream = null;
        try {
            List<Map<String, Object>> itemsList = (List<Map<String, Object>>) context.getPlatformInput("items");
            for (Map<String, Object> singleItem : itemsList) {
                IAppFileInfo appFileInfo = getVDS().getFileOperate().getFileInfo(fileId);
                if (appFileInfo == null) {
                    throw new ConfigException("后台导入Excel规则，获取文件标识为【" + fileId + "】文件对象，请检查");
                }
                inputStream = appFileInfo.getDataStream();
                importToDB(context, singleItem, inputStream,MergedType.None);
                close(inputStream);
            }
            
            IRuleOutputVo outputVo = context.newOutputVo();
            outputVo.put(null);
            return outputVo;
        }
        catch(PluginException e) {
        	throw e;
        }
        catch (Exception e) {
            throw new ConfigException("后台导入Excel规则，执行失败！\n" + e.getMessage(), e);
        } finally {
        	logger.info("后台导入Excel数据,总时长：" + (System.currentTimeMillis() - starTime) + "毫秒");
        	close(inputStream);
        }        
    }
    private void close(Closeable os) {
    	if(os == null) {
    		return ;
    	}
    	try {
    		os.close();
    	}
    	catch(IOException e) {
    		logger.error("",e);
    	}
    }
     
    // 20170423 最新
	protected void importToDB(IRuleContext context, Map<String, Object> cfgMap, InputStream inputStream,MergedType type) {
		SaveDataBaseAction act = new SaveDataBaseAction();
    	ParamsVo pars = parseParamsVo(context, cfgMap);
    	pars.setMergedType(type);
    	IDataView dv = act.saveDataBase(pars, inputStream);
    	// 更新数据
        handleResultDataView(dv, pars.getTableName(), pars.getTargetType(), context); 
    	
    }
	private IVDS getVDS() {
        return VDS.getIntance();
    }
    @SuppressWarnings("unchecked")
	private SaveDataBaseAction.ParamsVo parseParamsVo(IRuleContext context, Map<String, Object> cfgMap) {
    	 List<Map<String, Object>> mappings = (List<Map<String, Object>>) cfgMap.get("mapping");// 配置映射
         String tableName = (String) cfgMap.get("target"); // 导入的目标表名
         ExcelContextType targetType =ExcelContextType.getInstanceType((String) cfgMap.get("targetType")); // 目标类型
         //boolean isPhyscTable = targetType.equals("table"); // 是否物理表
         //int startRow = Integer.valueOf((String) cfgMap.get("dataStartRow")); //开始行号


         ParamsVo vo = new ParamsVo();
         vo.setTargetType(targetType);
         // 重复处理方式，目前仅是替换
         String repeatOperation = (String) cfgMap.get("repeatOperation");
         if (VdsUtils.string.isEmpty(repeatOperation)) {
             repeatOperation = "replace";
         }

         {
	         Map<String, Object> varMap = new HashMap<String, Object>(); // 配置了表达式的值
	         boolean isImportId = getOtherData(mappings, varMap); // 是否存在id导入
	         List<String> checkItems = (List<String>) cfgMap.get("checkItems"); // 获取检查重复的字段
	         // 如果不导入id，就不检查id，其他情况都检查id
	         if (VdsUtils.collection.isEmpty(checkItems) ) {
	             checkItems = new ArrayList<String>();
	         }
	         if (isImportId && !checkItems.contains("id")) {
	             checkItems.add("id");
	         }
	         vo.setCheckItems(checkItems);
	         vo.setVariableMap(varMap);
	         vo.setImportId(isImportId);
         }

         String cfgStr = VdsUtils.json.toJson(cfgMap);
         ExcelImportDataCfg dCfg = VdsUtils.json.fromJson(cfgStr, ExcelImportDataCfg.class);
         IDataView sourceDataView = null;
     

         IDataView dataView;
         if (vo.isPhyscTable()) {
        	 dataView = getVDS().getDas().createDataViewByName(tableName);// 物理表
         } else {
        	 dataView = getDataViewWithType(context, tableName, targetType);// 内存表
        	 sourceDataView = dataView;//getDataViewWithType(context, tableName, targetType);// 用于获取字段类型
         }

         FModelMapper mapper = getModelMapperByExcelImportCfg(dCfg, sourceDataView);
         int sheetIndex = -2;
         try {
        	 Object sheetNum = cfgMap.get("sheetNum");
             sheetNum = getVDS().getFormulaEngine().eval(sheetNum.toString());
             sheetIndex = Integer.parseInt(sheetNum.toString());
         } catch (Exception e) {
             throw new ConfigException("获取配置的sheet序号失败", e);
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
        	// 是否树(当)
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
                        throw new ConfigException("数据更新失败！" + e.getMessage(), e);
                    }
                } else {
                    throw new ConfigException("不支持类型[" + targetType + "]的变量值设置.");
                }
            } catch (Exception e) {
                throw new BusinessException("导入Excel: 执行失败！" + e.getMessage(), e);
            }*/
        if(targetType == null) {
        	throw new ConfigException("导入Excel: 执行失败,VariableType不能为空！");
        }
        switch (targetType) {
		case RuleSetInput:
		case RuleSetOutput:
		case RuleSetVar:
			context.getVObject().setContextObject(targetType.getType(), tableName, dataView);
			break;
		case Table:
			dataView.acceptChanges();
			break;
		default:
			throw new ConfigException("不支持类型[" + targetType + "]的变量值设置.");
		}
    }
    /**
	 * 取来源实体
	 * 
	 * @param context 规则上下文
	 * @param sourceName 来源名称
	 * @param sourceType 来源类型
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
        } else if ("table".equalsIgnoreCase(sourceType)) {   // 窗体实体
            sourceDV = getVDS().getDas().createDataViewByName(sourceName);
        } else {
            throw new ConfigException("导入Excel规则 : 不支持类型[" + sourceType + "]的变量值设置.");
        }
		 */
		
		ContextVariableType t = sourceType.getType();// ContextVariableType.getInstanceType(sourceType);
		if(t == null) {
			throw new ConfigException("导入Excel规则 : 不支持上下文变量类型[" + sourceType + "]的变量值设置.");
		}
		switch (t) {
			case RuleSetInput:
			case RuleSetOutput:
			case RuleSetVar:
				sourceDV = (IDataView) context.getVObject().getContextObject(sourceName,t);
				break;
			 
			default:
				throw new ConfigException("导入Excel规则 : 不支持上下文类型[" + sourceType + "]的变量值设置.");
				// throw new ExpectedException("导入Excel规则 : 不支持类型[" + sourceType + "]的变量值设置.");
		}
		return sourceDV;
	}

    /**
     * 
     * @param dCfg
     * @param sourceDataView 如果是null，就取物理表
     * @return
     */
    @SuppressWarnings("unchecked")
	private FModelMapper getModelMapperByExcelImportCfg(ExcelImportDataCfg dCfg, IDataView sourceDataView) {
        FModelMapper mapper = new FModelMapper();
        mapper.setStartRow(dCfg.getDataStartRow());
        if (mapper.getStartRow() <= 0) {
            throw new ConfigException("Excel导入配置的数据起始行必须大于1");
        }
        List<ColumnImportCfg> list = dCfg.getMapping();{
	        List<String> fieldNameList = ObjPropertyUtil2.getPropertyList(list, "fieldCode");
	        Set<String> fieldNameSet = new LinkedHashSet<String>(fieldNameList);
	        if (fieldNameSet.size() < fieldNameList.size()) {
	            throw new ConfigException("Excel导入配置中列名有重复");
	        }
        }
        ITable table = (sourceDataView == null ? getVDS().getMdo().getTable(dCfg.getTarget())  : null);
        Map<String, FProperty> properties = new CaseInsensitiveLinkedMap();
        for (int i = 0; i < list.size(); i++) {
            ColumnImportCfg columnCfg = list.get(i);
            String source = columnCfg.getSourceType();

            // 数据来源只有为excelColName和excelColNum才加入到excel解析模型里
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

            if (source.equals("excelColNum")) {// 当来源是列号时候
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
                    // 改为从实体中获取字段类型，因为前台没有传入字段类型
                    try {
                        type = sourceDataView.getMetadata().getColumn(columnName).getColumnType();
                    } catch (Exception e) {
                        throw new ConfigException("获取字段类型失败.", e);
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
     * 1、获取配置了表达式的值（没有表达式，返回null） 2 、判断是否存在字段id导入
     *
     * @param mappings 映射关系
     * @return
     */
    private boolean getOtherData(List<Map<String, Object>> mappings, Map<String, Object> varMap) {
        boolean isImportIdTag = false;
        for (Map<String, Object> map : mappings) {
            if (!isImportIdTag && map.get("fieldCode").equals("id")) {
                isImportIdTag = true;
            }
            String type = (String) map.get("sourceType");
            if (type.equals("expression")) {
                Object sourceValue = getVDS().getFormulaEngine().eval(map.get("sourceValue").toString());
                if (null == sourceValue) {
                    logger.warn("字段[" + map.get("fieldCode").toString() + "对应的表达式值为空，所以不作处理]");
                    continue;
                }
                varMap.put((String) map.get("fieldCode"), sourceValue);
            }
        }
        return isImportIdTag;
    }
}
