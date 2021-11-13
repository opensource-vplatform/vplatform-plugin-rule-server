package com.yindangu.v3.platform.rule;
 
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.toone.v3.platform.rule.model.FModelMapper;
import com.toone.v3.platform.rule.model.TreeColumn;

import com.yindangu.v3.business.VDS;
import com.yindangu.v3.business.jdbc.api.model.DataState;
import com.yindangu.v3.business.jdbc.api.model.IDataSetMetaData;
import com.yindangu.v3.business.metadata.api.IDAS;
import com.yindangu.v3.business.metadata.api.IDataObject;
import com.yindangu.v3.business.metadata.api.IDataView;
import com.yindangu.v3.business.plugin.business.api.rule.ContextVariableType;
import com.yindangu.v3.business.plugin.execptions.BusinessException;
import com.yindangu.v3.business.plugin.execptions.ConfigException;
import com.yindangu.v3.business.plugin.execptions.EnviException;
import com.yindangu.v3.business.plugin.execptions.PluginException;
import com.yindangu.v3.business.vds.IVDS;
import com.yindangu.v3.platform.excel.MergedType;
import com.yindangu.v3.platform.excel.SheetReader;
import com.yindangu.v3.platform.excel.SheetReader.SheetReaderBuilder;
import com.yindangu.v3.platform.plugin.util.VdsUtils;

/**
 * 导入excel数据到数据库
 * @Author xugang
 * @Date 2021/7/23 16:19
 */
class SaveDataBaseAction   {
    private static final Logger logger = LoggerFactory.getLogger(SaveDataBaseAction.class);
    // 约定的树型编码每级的长度
    private  int innerCodeLength = 5;

    protected static enum ExcelContextType{
    	// 活动集输入变量
		RuleSetInput(ContextVariableType.RuleSetInput),
		// 活动集输出变量
		RuleSetOutput(ContextVariableType.RuleSetOutput),
		// 活动集上下文变量
		RuleSetVar(ContextVariableType.RuleSetVar),
		// 窗体实体
		Table("table");
    	
		private ContextVariableType	 type;
		private String	 otherType;
		
		private ExcelContextType(String otherType) {
			this.otherType = otherType;
		}
		private ExcelContextType(ContextVariableType type) {
			this.type = type;
		}

		public ContextVariableType getType() {
			return type;
		}
		public String getOtherType() {
			return otherType;
		}
		public static ExcelContextType getInstanceType(String name) {
			ExcelContextType ret = null;
			for (ExcelContextType type : ExcelContextType.values()) {
				if (type.name().equalsIgnoreCase(name)) {
					ret = type;
					break;
				}
			}
			return ret;
		}
    }
    /**私有类*/
    protected static class ParamsVo{
    	private IDataView targetDataView;
    	private FModelMapper modelMapper;
    	private int sheetIndex;
    	private String tableName;
    	
    	private boolean tree;
    	private Map treeStructMap;
    	private List<String> checkItems;
    	private Map<String,?> variableMap;
    	
    	private IDataView sourceDataView;
    	//private boolean physcTable;
    	private boolean importBy;
    	private boolean importId;
    	private ExcelContextType targetType;
    	private MergedType mergedType;
    	 /*String bizCodeField = isBizCodeTree ? (String) cfgMap.get(BIZCODEFIELD) : treeCodeColumn;
         String bizCodeFormat = (String) cfgMap.get(BIZCODEFORMAT);
         Object bizCodeConfig = null;
         if (isBizCodeTree)
             bizCodeConfig = getVDS().getFormulaEngine().eval((String) cfgMap.get(BIZCODECONFIG));
             */
    	private String bizCodeField  ;
    	private String bizCodeFormat  ;
    	private String bizCodeConfig  ;
		public IDataView getTargetDataView() {
			return targetDataView;
		}

		public void setTargetDataView(IDataView targetDataView) {
			this.targetDataView = targetDataView;
		}

		public FModelMapper getModelMapper() {
			return modelMapper;
		}

		public void setModelMapper(FModelMapper modelMapper) {
			this.modelMapper = modelMapper;
		}

		public int getSheetIndex() {
			return sheetIndex;
		}

		public void setSheetIndex(int sheetIndex) {
			this.sheetIndex = sheetIndex;
		}

		public boolean isTree() {
			return tree;
		}

		public void setTree(boolean tree) {
			this.tree = tree;
		}

		public Map getTreeStructMap() {
			return treeStructMap;
		}

		public void setTreeStructMap(Map treeStructMap) {
			this.treeStructMap = treeStructMap;
		}
		/**检查重复的字段名*/
		public List<String> getCheckItems() {
			return checkItems;
		}
		/**检查重复的字段名*/
		public void setCheckItems(List<String> checkItems) {
			this.checkItems = checkItems;
		}

		/**配置了表达式的值(不会null)*/
		public Map<String, ?> getVariableMap() {
			return variableMap;
		}
		/**配置了表达式的值*/
		public void setVariableMap(Map<String, ?> variableMap) {
			this.variableMap = variableMap;
		}
		/**内存表,用于获取字段类型*/
		public IDataView getSourceDataView() {
			return (isPhyscTable() ? null : sourceDataView);
		}
		/**内存表*/
		public void setSourceDataView(IDataView sourceDataView) {
			this.sourceDataView = sourceDataView;
		}
		/**true:物理表,false:内存表*/
		public boolean isPhyscTable() {
			return (targetType == ExcelContextType.Table);
		}
		public ExcelContextType getTargetType() {
			return targetType;
		}

		public void setTargetType(ExcelContextType targetType) {
			this.targetType = targetType;
		}
		public String getTableName() {
			return tableName;
		}

		public void setTableName(String tableName) {
			this.tableName = tableName;
		}
		/**  是否导入业务编码*/
		public boolean isImportBy() {
			return importBy;
		}
		/**  是否导入业务编码*/
		public void setImportBy(boolean importBy) {
			this.importBy = importBy;
		}

		public boolean isImportId() {
			return importId;
		}

		public void setImportId(boolean importId) {
			this.importId = importId;
		}

		public String getBizCodeField() {
			return bizCodeField;
		}

		public void setBizCodeField(String bizCodeField) {
			this.bizCodeField = bizCodeField;
		}

		public String getBizCodeFormat() {
			return bizCodeFormat;
		}

		public void setBizCodeFormat(String bizCodeFormat) {
			this.bizCodeFormat = bizCodeFormat;
		}

		public String getBizCodeConfig() {
			return bizCodeConfig;
		}

		public void setBizCodeConfig(String bizCodeConfig) {
			this.bizCodeConfig = bizCodeConfig;
		}
		/**导入Excel存在合并单元格时处理方式*/
		public MergedType getMergedType() {
			return mergedType;
		}

		public void setMergedType(MergedType mergedType) {
			this.mergedType = mergedType;
		}
    	
    }
    // 20170423 最新
	public IDataView saveDataBase(ParamsVo vo, InputStream inputStream) {
    	List<Map<String, Object>> records = readExcelData(vo, inputStream);
    	IDataView dv = updateToDataBase(vo, records);
    	return dv; 
    }
    
    

    /**
     * 读取excel数据
     * @param pars
     * @param inputStream
     * @return
     */
    
    private List<Map<String, Object>> readExcelData(ParamsVo pars, InputStream inputStream){
    	FModelMapper mapper = pars.getModelMapper();
        IDataSetMetaData dataSetMetaData = pars.getTargetDataView().getMetadata(); 
        int sheetIndex = pars.getSheetIndex();
        
        /*int sheetno = 0;
        try {
            sheetNum = getVDS().getFormulaEngine().eval(sheetNum.toString());
            sheetno = Integer.parseInt(sheetNum.toString());
        } catch (Exception e) {
            throw new ConfigException("获取配置的sheet序号失败", e);
        }*/
        List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
        // 读取Excel的数据
        //FPOExcel excel = new FPOExcel();
        SheetReaderBuilder rsb = new SheetReader.SheetReaderBuilder();
        rsb.setInputStream(inputStream)
        		.setTable(dataSetMetaData)
        		.setSheetIndex(sheetIndex)
        		.setFieldMap(mapper)
        		.setMerged(pars.getMergedType())
        		; 
        SheetReader rd = rsb.builder();
        result = rd.readData();
        return result; 
    }
    /**
     * 检查重复， 保存Excel的全部id值，如果不导入id，则无数据。用于判断是否有重复id导入。
     * @param vo
     * @param excelRecords
     * @return 获取检查重复字段在Excel中的数据
     */
	private Map<String/** 字段名 */, Set<Object>/** 字段值 */ > checkExistRecord(ParamsVo vo, List<Map<String, Object>> excelRecords) {
		Map<String, ?> variableMap = vo.getVariableMap();
		List<String> checkItems = vo.getCheckItems();
		Map<String, Set<Object>> sourceFieldData = new LinkedHashMap<String, Set<Object>>(); // 获取检查重复字段在Excel中的数据

		for (Map<String, Object> map : excelRecords) {

			// 把其他表达式的值集合加到数据集里
			map.putAll(variableMap);
			for (String item : checkItems) {
				Object value = map.get(item);
				Set<Object> rds = sourceFieldData.get(item);
				if (rds == null) {
					rds = new HashSet<Object>();
					sourceFieldData.put(item, rds);
				}
				if (value != null) {
					rds.add(value);
				}
			}
		}
		return sourceFieldData;
	}

	/**
	 * 取Excel的全部记录的id字段值，如果不导入id，则无为空list。用于判断是否有重复id导入
	 * 
	 * @param excelRecords
	 * @return
	 */
	private Set<String> getRecordIds(List<Map<String, Object>> excelRecords) {
		Set<String> excelIds = new HashSet<String>();
		int size = 0;
		for (Map<String, Object> map : excelRecords) {
			String rowId = (String) map.get("id");
			if (rowId != null) {
				size++;
				excelIds.add(rowId);
			}
		}
		if (excelIds.size() != size) {
			throw new BusinessException("后台导入Excel规则 ，Excel数据存在id重复，请检查");
		}
		return excelIds;
	}
	private IDataView updateToDataBase(ParamsVo vo, List<Map<String, Object>> excelRecords) { 
		try {
			return updateToDataBase0(vo, excelRecords);
		}
        catch(PluginException ex) {
        	Matcher matcher = Pattern.compile(".*第(\\d+)行.*").matcher(ex.getMessage()); 
        	if(matcher.matches()) {
        		FModelMapper mp = vo.getModelMapper();
                int beforeRow = Integer.valueOf(matcher.group(1));
                int row = beforeRow + (mp == null ?1 : mp.getStartRow());
                String str = ex.getMessage().replaceFirst("第" + beforeRow + "行", "第" + row + "行");
                throw new EnviException(str);
            } else {
                throw ex;
            }
        }
        catch (RuntimeException ex) {
        	throw ex;
        }
	}
    private IDataView updateToDataBase0(ParamsVo vo, List<Map<String, Object>> excelRecords) { 
        //过滤树字段
    	Map treeStructMap = vo.getTreeStructMap(); 
    	Map<String, List<Map<String, Object>>> groupResult =splitGroupExcelData(vo.isTree(),treeStructMap, excelRecords, vo.getVariableMap());
    	List<String> checkItems = vo.getCheckItems();
       //int groupNum = 0; //分组序号
        IDataView targetDataView = vo.getTargetDataView();
        boolean physcTable = vo.isPhyscTable();
       
        
        for (List<Map<String, Object>> groupRecods : groupResult.values()) {
        	Map<String/**字段名*/, Set<Object>/**字段值*/> sourceFieldData = checkExistRecord(vo, excelRecords);
        	Set<String> excelIds = getRecordIds(excelRecords);
           
            // 最后结果
            if (!vo.isTree()) {// 树结构为空，则按照普通表/实体导入 
            	 // 查询重复数据
            	RepeatVo repeatData =  getRepeatData(sourceFieldData, vo, excelIds);

                
                    // 处理数据
                    // 完善异常，报错的行号为--startRow+excel中的行号
                    handleCommonData(repeatData, groupRecods, targetDataView, checkItems, physcTable);
                   //handleResultDataView(dataView, tableName, targetType, context);
                
            }
            else {	           
	          
	            //String selectId = "";
	            // 得到导入树的层级码每级长度
	            String treeCodeLength = "00000";
	            if (isNotEmpty(treeCodeLength)) {
	                innerCodeLength = treeCodeLength.length();
	            }
	            String orderColumn = (String) treeStructMap.get(TreeColumn.OrderField.columnName());
	            String innerCodeColumn = (String) treeStructMap.get(TreeColumn.TreeCodeField.columnName());
	            // 获取重复记录数据
	            RepeatTreeVo repeatData = getRepeatDataTree(vo , sourceFieldData, excelIds, orderColumn, innerCodeColumn);
	            /*if (physcTable) {
	                dataView = getVDS().getDas().createDataViewByName(tableName);
	            } else {
	                if (groupNum == 0) { //每个分组都用同一个内存表
	                    dataView = getDataViewWithType(context, tableName, targetType);
	                }
	                groupNum++;
	            }*/
	           // handelEntityOrTableTree(groupRecods, treeStructMap, targetDataView, selectId, isImportId, isBizCodeTree, cfgMap, tableName, isPhyscTable, repeatData, checkItems);
	            handelEntityOrTableTree(groupRecods, vo, /*selectId, */  repeatData );
	            //handleResultDataView(dataView, tableName, targetType, context);
            }
        } 
        return targetDataView;
    }

    /**
     * 将Excel数据按照过滤树字段进行分组
     * @return
     */
    private Map<String/**分组名*/,List<Map<String, Object>>/**分组数据*/> splitGroupExcelData
    	(boolean tree,Map treeStructMap, List<Map<String, Object>> excelRecords,Map<String,?> variableMap){
    	 String filterTreeFied = null;
         if (tree) {
         	Object fd = treeStructMap.get(TreeColumn.BusiFilterField.columnName());
            filterTreeFied = (String) fd;
         }
         
         Map<String, List<Map<String, Object>>> groupResult = new HashMap<String, List<Map<String, Object>>>();
         if (VdsUtils.string.isEmpty(filterTreeFied)) { //没有过滤树字段
             groupResult.put("nogroup", excelRecords);
         } 
         else {
             //将Excel数据按照过滤树字段进行分组
             for (Map<String, Object> map : excelRecords) {
                 String fiterValue = (String) map.get(filterTreeFied) ;//过滤字段值
                 if(fiterValue == null ) {
                 	fiterValue = (String) variableMap.get(filterTreeFied)  ;
                 }
                 if (VdsUtils.string.isEmpty(fiterValue)) { //过滤字段值为空
                     throw new BusinessException("过滤树字段值为空");
                 }

                 List<Map<String, Object>> fiterList=groupResult.get(fiterValue);
                 if (fiterList == null) {
                     fiterList = new ArrayList<Map<String, Object>>();
                     groupResult.put(fiterValue, fiterList);
                 }
                 fiterList.add(map);
             }
         }
         return groupResult;
    }

    

    /**
     * 判断是否是重复数据
     *
     * @param map
     * @param repeatDatas
     * @param checkItems
     * @return
     */
    private String checkDataRepeatForKey(Map<String, Object> map, Object repeatDatas, List<String> checkItems) {
        Map<String, List<Object>> repeatData = (Map<String, List<Object>>) repeatDatas;
        if (repeatData.keySet().size() < 1 || checkItems.size() < 1)
            return null;
        String tmpKey = "";
        for (int i = 0; i < checkItems.size(); i++) {
            String key = checkItems.get(i);
            Object value = map.get(key);
            if (value != null) {
                tmpKey = tmpKey + key + "=" + value + ",";
            }
        }
        if (!tmpKey.equals("")) {
            tmpKey = "[" + tmpKey.substring(0, tmpKey.length() - 1) + "]";
        } else {
            return null;
        }
        if (repeatData.containsKey(tmpKey)) {
            return tmpKey;
        } else
            return null;
    }

    /**
     * 处理普通表/实体的数据
     *
     * @param repeatDataMap
     * @param excelData
     * @param dataView
     * @param checkItems
     * @param isPhyscTable
     * @return
     */
    private void handleCommonData(RepeatVo repeatDataMap, List<Map<String, Object>> excelData, IDataView dataView, List<String> checkItems, boolean isPhyscTable) {
        try {

            // 符合重复条件的id列表
            Map<String, List<Object>> repeatData = repeatDataMap.getMatchData();//(Map<String, List<Object>>) repeatDataMap.get("matchData");
            if (repeatData.size() == 0) { // 全新增批量处理
                dataView.insertDataObject(excelData);
                return  ;
            }

            // 在表中存在，却不符合检查条件的id列表
            List<String> existId = repeatDataMap.getExistIds();//(List<String>) repeatDataMap.get(D_ExistId);
            
            for (Map<String, Object> map : excelData) {
                String rowId = (String) map.get("id");
                // 判断记录是否为重复记录，tmpKey有值表示是重复记录。
                String tmpKey = checkDataRepeatForKey(map, repeatData, checkItems);
                if (tmpKey != null && !isPhyscTable) {// 当前记录是属于重复记录,更新实体对应的记录即可
                    List<IDataObject> matchDatas =  (List) repeatData.get(tmpKey);
                    for (int i = 0; i < matchDatas.size(); i++) {
                        IDataObject dataObject = matchDatas.get(i);
                        for (String key : map.keySet()) {
                            // 首条记录可以用excel带的id，不是首条重复记录重置id
                            if (key.equals("id") && rowId != null && i != 0) {
                                dataObject.setId(VdsUtils.uuid.generate());
                                continue;
                            }
                            Object value = map.get(key);
                            dataObject.set(key, value);
                        }
                    }
                    continue;
                }
                IDataObject dataObject = dataView.insertDataObject();
                dataObject.setId(VdsUtils.uuid.generate());
                Set<String> set = map.keySet();
                for (String key : set) {
                    Object value = map.get(key);
                    dataObject.set(key, value);
                }
                // 如果在表中存在该id，却不符合检查条件的记录，重新生成id
                if (rowId != null && existId != null && existId.contains(rowId)) {
                    String tmpUUID = VdsUtils.uuid.generate();
                    dataObject.setId(tmpUUID);
                    logger.warn(dataObject.getId() + "已存在，且不符合重复检查条件，已重新设置id为：" + tmpUUID);
                    continue;
                }
                List<String> ids = null;
                if (tmpKey != null && isPhyscTable) {// 校验当前记录是否属于重复记录
                    ids = (List) repeatData.get(tmpKey);
                }
                if (ids != null) {
                    for (String id : ids) {
                        dataObject.setStates(DataState.Modified, id);
                    }
                }
            }
        } catch (PluginException e) {
        	throw e;
        } catch (Exception e) {
            throw new ConfigException("无法更新实体表数据，请检查数据类型与字段类型是否一致.", e);
        } 
    }

    private boolean isNumeric(String str) {
        Pattern pattern = Pattern.compile("[0-9]*");
        Matcher isNum = pattern.matcher(str);
        if (!isNum.matches()) {
            return false;
        }
        return true;
    }

    private static String displayWithComma(String str) {
        str = new StringBuffer(str).toString(); // 先将字符串颠倒顺序
        String str2 = "";
        int size = (str.length() % 2 == 0) ? (str.length() / 2) : (str.length() / 2 + 1); // 每三位取一长度
        for (int i = 0; i < size - 1; i++) { // 前n-1段
            str2 += str.substring(i * 2, i * 2 + 2) + ",";
        }
        for (int i = size - 1; i < size; i++) { // 第n段
            str2 += str.substring(i * 2, str.length());
        }
        str2 = new StringBuffer(str2).toString();

        return str2;
    }

    /**
     * 判断是否是纯正整数
     *
     * @param str
     * @return
     */
    private boolean isNumber(String str) {
        char[] ch = str.toCharArray();
        for (int i = 0; i < ch.length; i++) {
            if (ch[i] < '0' || ch[i] > '9') {
                return false;
            }
        }
        return true;
    }

    private List<Map<String, Object>> getSortData(Object sep, Boolean isBizCode, List<Map<String, Object>> tmpMapData, String fieldCode) {
        final String nullTip = isBizCode ? "业务编码字段存在空值,请检查" : "层级码字段存在空值,请检查";
        Map<String, Object> tmp = tmpMapData.get(0);
        String tmpCode = (String) tmp.get(fieldCode);
        //Boolean num = true;
        if (tmpCode == null || tmpCode.length() ==0) {
            throw new BusinessException(nullTip);
        } else {
            //String pattern = "^\\d+(\\" + sep + "\\d+)?";
            //num = Pattern.matches(pattern, tmpCode);
        }
        // 层级码或父子关系字段
        final String fCode = fieldCode;
        // 分隔符
        final String SEPARTOR = sep + "";
        // 是否是数字code
        //final boolean numCode = num;

        Comparator<Map<String, Object>> compareSepartor = new Comparator<Map<String, Object>>() {
            @Override
            public int compare(Map<String, Object> o1, Map<String, Object> o2) {
                int result = 0;
                String code1 = (String) o1.get(fCode);
                String code2 = (String) o2.get(fCode);

                if (code1 == null || code2 == null)
                    throw new BusinessException(nullTip);

                String tmpSpartor = null;
                if (isNumeric(SEPARTOR)) {
                    tmpSpartor = ",";
                    code1 = displayWithComma(code1);
                    code2 = displayWithComma(code2);
                } else {
                    tmpSpartor = SEPARTOR.equals(".") ? "\\." : SEPARTOR;
                }

                String[] arrayCode1 = code1.split(tmpSpartor);
                String[] arrayCode2 = code2.split(tmpSpartor);
                int loopLen = arrayCode1.length > arrayCode2.length ? arrayCode2.length : arrayCode1.length;

                for (int i = 0; i < loopLen; i++) {
                    String strCode1 = arrayCode1[i];
                    String strCode2 = arrayCode2[i];
                    if (isNumber(strCode1)) {
                        int intCode1 = Integer.parseInt(strCode1);
                        int intCode2 = Integer.parseInt(strCode2);
                        if (intCode1 > intCode2) {
                            result = 1;
                            break;
                        } else if (intCode1 < intCode2) {
                            result = -1;
                            break;
                        } else if (i == (loopLen - 1)) {
                            if (arrayCode1.length > arrayCode2.length) {
                                result = 1;
                                break;
                            } else {
                                result = -1;
                                break;
                            }
                        }
                    } else {
                        if (strCode1.length() > strCode2.length()) {
                            result = 1;
                            break;
                        } else if (strCode1.length() < strCode2.length()) {
                            result = -1;
                            break;
                        } else {
                            if (strCode1.compareTo(strCode2) == 0) {
                                if (i == (loopLen - 1)) {
                                    if (arrayCode1.length < arrayCode2.length) {
                                        result = -1;
                                        break;
                                    } else {
                                        result = 1;
                                        break;
                                    }
                                }
                            } else {
                                result = strCode1.compareTo(strCode2);
                                break;
                            }
                        }
                    }

                }

                return result;
            }
        };
        Collections.sort(tmpMapData, compareSepartor);
        return tmpMapData;
    }

    private List<Map<String, Object>> sortData(List<Map<String, Object>> excelData, String fieldCode, boolean isBizTree) {
        final String sortField = fieldCode;
        final boolean isBizCode = isBizTree;
        List<Map<String, Object>> result = excelData;
        Comparator<Map<String, Object>> bizCompareMap = new Comparator<Map<String, Object>>() {
            @Override
            public int compare(Map<String, Object> o1, Map<String, Object> o2) {
                String code1 = (String) o1.get(sortField);
                String code2 = (String) o2.get(sortField);
                if (code1 == null || code2 == null) {
                    if (isBizCode) {
                        throw new ConfigException("业务编码字段存在空值.请检查");
                    } else {
                        throw new ConfigException("层级码字段存在空值.请检查");
                    }

                }
                int result = code1.compareTo(code2);
                return result;
            }
        };
        // 根据业务编码字段排序
        Collections.sort(result, bizCompareMap);
        return result;
    }

    /**
     * 获取树过滤字段
     *
     * @param treeStruts 树结构
     * @param isImportId 是否导入id
     * @return 下午4:26:47
     */
    private Set<String> getFilterFieldForTree(Map<String, Object> treeStruts, boolean isImportId) {
        String parentColumn = (String) treeStruts.get(TreeColumn.ParentField.columnName());
        String treeCodeColumn = (String) treeStruts.get(TreeColumn.TreeCodeField.columnName());
        String isLeafColumn = (String) treeStruts.get(TreeColumn.IsLeafField.columnName());
        String orderColumn = (String) treeStruts.get(TreeColumn.OrderField.columnName());
        String leftColumn = (String) treeStruts.get(TreeColumn.LeftField.columnName());
        String rightColumn = (String) treeStruts.get(TreeColumn.RightField.columnName());
        String bizCodeColumn = (String) treeStruts.get(TreeColumn.BizCodeField.columnName());
        // 过滤树内置字段
        Set<String> filterFieldSet = new LinkedHashSet<String>();
        if (!isImportId)
            filterFieldSet.add("id");

        if (isNotEmpty(parentColumn)) {
            filterFieldSet.add(parentColumn.toLowerCase());
        }
        if (isNotEmpty(treeCodeColumn)) {
            filterFieldSet.add(treeCodeColumn.toLowerCase());
        }
        if (isNotEmpty(bizCodeColumn)) {
            filterFieldSet.add(bizCodeColumn.toLowerCase());
        }
        if (isNotEmpty(isLeafColumn)) {
            filterFieldSet.add(isLeafColumn.toLowerCase());
        }
        if (isNotEmpty(orderColumn)) {
            filterFieldSet.add(orderColumn.toLowerCase());
        }
        if (isNotEmpty(leftColumn)) {
            filterFieldSet.add(leftColumn.toLowerCase());
        }
        if (isNotEmpty(rightColumn)) {
            filterFieldSet.add(rightColumn.toLowerCase());
        }
        return filterFieldSet;
    }

    private boolean isNotEmpty(String str) {
        return  (str != null && str.length()>0) ;/*{
            return true;
        }

        return false;*/
    }

    //private int getLastOrderNoTableOrEntity( String tableName, String innerCodeField, String orderNoField, IDataView sourceDataView, boolean isPhyscTable, String parentCode) {
    private int getLastOrderNoTableOrEntity( ParamsVo vo, String innerCodeField, String orderNoField,   String parentCode) {
        int orderNo = 1; 
        Map<String, Object> queryParams = new HashMap<String, Object>();
        queryParams.put("innerCodeParam", parentCode + "_____");
        String wherestr = innerCodeField + " like :innerCodeParam order by " + innerCodeField + " desc";
        if (vo.isPhyscTable()) {
            IDAS das = getVDS().getDas();
            String sql = "select * from " + vo.getTableName() + " where " + wherestr;
            IDataView dataView = das.find(sql, queryParams);
            if (dataView != null) {
                List<Map<String, Object>> orderNoMaxData = dataView.getDatas();
                if (orderNoMaxData.size() > 0) {
                    Map<String, Object> singleRecord = orderNoMaxData.get(0);
                    orderNo = (Integer) singleRecord.get(orderNoField);
                    return orderNo;
                }
            }
        } else {
            List<IDataObject> orderNoMaxData = vo.getTargetDataView().select(wherestr, queryParams); //sourceDataView.select(wherestr, queryParams);
            if (orderNoMaxData.size() > 0) {
                IDataObject dataObject = orderNoMaxData.get(0);
                orderNo = (Integer) dataObject.get(orderNoField);
                return orderNo;
            }
        }
        return orderNo;
    }

    private boolean isRootNode(String code, Boolean isBizCodeTree, String bizCodeFormat, Object bizCodeConfig) {
        if (!isBizCodeTree) {// 如果不是业务编码树，树根节点的默认长度是5
            if (code == null || code.length() > 5) {
                return false;
            } else {
                return true;
            }
        } else {
            if (bizCodeFormat.equals("noneSeparativeSign")) {
                int codeBaseLen = (Integer) bizCodeConfig;
                if (code == null || code.length() > codeBaseLen) {
                    return false;
                } else {
                    return true;
                }
            } else {
                if (code == null || code.contains(bizCodeConfig + "")) {
                    return false;
                } else {
                    return true;
                }
            }
        }
    }

    /**
     * 处理实体树
     *
     * @param result 上午11:22:03
     */
    /*private boolean handelEntityOrTableTree(List<Map<String, Object>> result, Map<String, Object> treeStruts, IDataView dataView, String selectId, boolean isImportId, boolean isBizCodeTree,
                                            Map<String, Object> cfgMap, String tableName, boolean isPhyscTable, Map<String, Object> repeatDataMap, List<String> checkItems) {*/
    private boolean handelEntityOrTableTree(List<Map<String, Object>> result, ParamsVo vo,     
             RepeatTreeVo repeatDataMap  ) {
        // 重复的数据
        Map<String, List<Object>> repeatData =  repeatDataMap.getMatchData();//(Map<String, List<Object>>)
        List<String> existId = repeatDataMap.getExistIds();
        // 数据库中根节点的顺序号
        Map<String, List<Integer>> repeatOrderNo = repeatDataMap.getRepeatOrderNo() ;// (Map<String, List<Integer>>) repeatDataMap.get("repeatOrderNo");

        Map treeStruts = vo.getTreeStructMap();
        String parentColumn = (String) treeStruts.get(TreeColumn.ParentField.columnName());
        String treeCodeColumn = (String) treeStruts.get(TreeColumn.TreeCodeField.columnName());
        String isLeafColumn = (String) treeStruts.get(TreeColumn.IsLeafField.columnName());
        String orderColumn = (String) treeStruts.get(TreeColumn.OrderField.columnName());

        // 业务编码配置
        boolean isBizCodeTree = vo.isImportBy();/*
        if (importBy != null && importBy.equals("filiation")) {
            isBizCodeTree = true;
        }*/
        /*
        String bizCodeField = isBizCodeTree ? (String) cfgMap.get(BIZCODEFIELD) : treeCodeColumn;
        String bizCodeFormat = (String) cfgMap.get(BIZCODEFORMAT);
        Object bizCodeConfig = null;
        if (isBizCodeTree)
            bizCodeConfig = getVDS().getFormulaEngine().eval((String) cfgMap.get(BIZCODECONFIG));
*/
        String bizCodeFormat = vo.getBizCodeFormat();
        String bizCodeField ;
        Object bizCodeConfig = null;
        if(isBizCodeTree) {
        	bizCodeField = vo.getBizCodeField() ;
        	bizCodeConfig = getVDS().getFormulaEngine().eval(vo.getBizCodeConfig());
        }
        else {
        	bizCodeField =  treeCodeColumn;
        }
        
        
        // 根据父子关系字段排序
        result = bizCodeConfig != null ? getSortData(bizCodeConfig, isBizCodeTree, result, bizCodeField) : sortData(result, bizCodeField, isBizCodeTree);

        // 临时缓存,用于设置叶子节点,key=code,value=AbstractTreeDataObject
        Map<String, Object> idMap = new LinkedHashMap<String, Object>();

        // 保存某个编码是否属于新增
        Map<String, Boolean> isInsert = new LinkedHashMap<String, Boolean>();

        // 过滤树内置字段
        Set<String> filterFieldSet = getFilterFieldForTree(treeStruts, vo.isImportId());

        // 保存每个编码下有多少个子节点
        Map<String, Integer> codeOrder = new LinkedHashMap<String, Integer>();

        Map<String, String> codeInnerCode = new LinkedHashMap<String, String>();
        // 根节点临时标识
        String rootCode =   repeatDataMap.getParentIden();
        // 标识某个父节点已经获取了第几个重复的顺序号
        Map<String, Integer> innerCodeIndex = new LinkedHashMap<String, Integer>();

        // 2020-07-07
        // ------新增------
        // 不是业务编码树，则判断层级码长度是否大于5.
        if (!isBizCodeTree) {
            for (int i = 0; i < result.size(); i++) {
                // 单条记录
                Map<String, Object> singleMap = result.get(i);
                String code = (String) singleMap.get(bizCodeField);
                int length = code.length() - innerCodeLength;
                if (length < 0) {
                    throw new ConfigException("树形编码= " + code + " 的记录编码格式错误，编码长度应为5的倍数");
                }
            }
        }
        // ------新增------

        for (int i = 0; i < result.size(); i++) {
            // 单条记录
            Map<String, Object> singleMap = result.get(i);
            String rowId = (String) singleMap.get("id");
            // 获取父子关系字段
            String code = (String) singleMap.get(bizCodeField);
            if (codeInnerCode.containsKey(code)) {
                throw new BusinessException("excel里有重复层级码" + code + "不允许导入");
            }
            // 重复key，如果不重复，则为空
            String repeatKey = checkDataRepeatForKey(singleMap, repeatData, vo.getCheckItems());
            // 当前的父子关系节点是否根节点
            boolean isRoot = isRootNode(code, isBizCodeTree, bizCodeFormat, bizCodeConfig);

            String parentCode = "";
            if (!isBizCodeTree) {// 不是业务编码树，则以普通的树计算父节点
                // 父亲节点编码
                parentCode = code.substring(0, code.length() - innerCodeLength);
            } else {
                if (bizCodeFormat.equals("haveSeparativeSign")) {// 有分隔符
                    int lastIndex = getLastIndex(code, (String) bizCodeConfig);
                    parentCode = code.substring(0, lastIndex);
                } else {
                    parentCode = code.substring(0, code.length() - (Integer.parseInt(bizCodeConfig + "")));
                }
            }

            // 当前节点pid
            String pId = null;
            IDataObject parentNode = (IDataObject) idMap.get(parentCode);
            /**
             * 如果为导入数据中的顶级节点，则pid设置为选中目标导入节点id (注意不能以parentCode为空作为判断顶级节点的依据,可能有这种情况：第1个节点code有10位，第2个它的子节点code有15位)
             * 如果是普通层级树，则以层级码长度判断，如果是父子关系字段标识的树，1、没分隔符就用指定位数，2、有分隔符，判断code是否包含分隔符
             */
            // 当前节点的父节点的层级码
            String parentNodeInnerCode = "";
            if (isRoot) {
                pId = "";// selectId;
            } else if (parentNode != null) {
                pId = parentNode.getId();
                parentNodeInnerCode = (String) parentNode.get(treeCodeColumn);
                // 修改父亲节点为非叶子节点
                parentNode.set(isLeafColumn, false);
            } else {
                throw new ConfigException("节点【" + code + "】的父节点【" + parentCode + "】不存在，请检查!");
            }

            // 当前节点在数据库中最大的OrderNo
            int maxOrderNo = -1;
            // 用于查询某个节点下，最后的顺序号
            String queryParentCode = parentNodeInnerCode.equals("") ? rootCode : parentNodeInnerCode;
            String tmpParentCode = parentCode.equals("") ? rootCode : parentCode;
            int orderNo = 1;
            if (repeatKey == null) {// 当前节点属于新增
                if (!codeOrder.containsKey(tmpParentCode)) {
                    // 查询父节点是否属于新增，父节点是新增，当前节点也是新增，且没保存父节点，故当前节点为第一个节点
                    if (isInsert.get(queryParentCode) != null && isInsert.get(queryParentCode)) {
                        maxOrderNo = 0;
                    } else if (isInsert.size() == 0) {
                        maxOrderNo = 0;
                    } else {
                        maxOrderNo = getLastOrderNoTableOrEntity(vo , treeCodeColumn, orderColumn , parentNodeInnerCode);
                        if (maxOrderNo == -1)
                            maxOrderNo = 0;
                    }
                } else {
                    maxOrderNo = codeOrder.get(tmpParentCode);
                }
                orderNo = maxOrderNo + 1;
            } else {
                List<Integer> existOrderNo = repeatOrderNo.get(queryParentCode);
                if (existOrderNo != null) {
                    int index = 0;
                    if (innerCodeIndex.containsKey(queryParentCode)) {
                        index = innerCodeIndex.get(queryParentCode) + 1;
                    }
                    if (existOrderNo.size() > index) {// 这里判断是避免有节点进行了升级或降级,因为升级/降级后，在新节点上面没有对应的顺序号
                        orderNo = existOrderNo.get(index);
                    } else {
                        if (codeOrder.containsKey(tmpParentCode)) {
                            orderNo = codeOrder.get(tmpParentCode) + 1;
                        } else {
                            orderNo = 1;
                        }
                    }
                    innerCodeIndex.put(queryParentCode, index);
                } else {
                    if (codeOrder.containsKey(tmpParentCode)) {
                        orderNo = codeOrder.get(tmpParentCode) + 1;
                    } else {
                        orderNo = 1;
                    }
                }
            }
            codeOrder.put(tmpParentCode, orderNo);
            String innerCode = getInnerCodeByOrderNo(orderNo, codeInnerCode, parentCode);
            codeInnerCode.put(code, innerCode);

            IDataObject dataObject = null;
            if (vo.isPhyscTable()) {
                dataObject = vo.getTargetDataView().insertDataObject();
                dataObject.setId(VdsUtils.uuid.generate());
                Set<String> set = singleMap.keySet();
                for (String key : set) {
                    // 如果是树的内置字段则不设置值
                    if (filterFieldSet.contains(key.toLowerCase())) {
                        continue;
                    }
                    Object value = singleMap.get(key);
                    dataObject.set(key, value);
                }
                String tmpUUID = "";
                // 如果在表中存在该id，却不符合检查条件的记录，重新生成id
                if (rowId != null && existId != null && existId.contains(rowId)) {
                    tmpUUID = VdsUtils.uuid.generate();
                    // logger.warn(dataObject.getId()+"已存在，且不符合重复检查条件，已重新设置id为："+tmpUUID);
                    dataObject.setId(tmpUUID);
                }
                if (repeatKey != null) {
                    List<String> ids = (List) repeatData.get(repeatKey);
                    for (int j = 0; j < ids.size(); j++) {
                        // 如果不检查id的话，更新的时候不更新id
                        if (vo.getCheckItems().indexOf("id") == -1) {
                            dataObject.setId(ids.get(j));
                        }
                        dataObject.setStates(DataState.Modified, ids.get(j));
                    }
                    isInsert.put(innerCode, false);
                } else {
                    isInsert.put(innerCode, true);// 记录当前节点为新增
                }
            } else {
                if (repeatKey != null) {
                    List<IDataObject> ids =  (List) repeatData.get(repeatKey);
                    for (int j = 0; j < ids.size(); j++) {
                        IDataObject rowRecord = ids.get(j);
                        if (j == 0)
                            dataObject = rowRecord;
                        for (String key : singleMap.keySet()) {
                            // 如果是树的内置字段则不设置值
                            if (filterFieldSet.contains(key.toLowerCase())) {
                                continue;
                            }
                            Object value = singleMap.get(key);
                            rowRecord.set(key, value);
                        }
                    }
                    isInsert.put(innerCode, false);
                } else {
                    dataObject =  vo.getTargetDataView().insertDataObject();
                    dataObject.setId(VdsUtils.uuid.generate());
                    for (String key : singleMap.keySet()) {
                        // 如果是树的内置字段则不设置值
                        if (filterFieldSet.contains(key.toLowerCase())) {
                            continue;
                        }
                        Object value = singleMap.get(key);
                        dataObject.set(key, value);
                    }
                    isInsert.put(innerCode, true);// 记录当前节点为新增
                }
            }

            dataObject.set(parentColumn, pId);
            // 默认设置当前节点为叶子节点
            dataObject.set(isLeafColumn, true);
            dataObject.set(orderColumn, orderNo);
            dataObject.set(treeCodeColumn, innerCode);
            idMap.put(code, dataObject);
        }
        idMap.clear();
        return true;
    }

    /**
     * 根据排序号拼装Innercode
     *
     * @param orderNo
     * @param codeInnerCode
     * @param parentCode
     * @return 上午1:37:20
     */
    private String getInnerCodeByOrderNo(int orderNo, Map<String, String> codeInnerCode, String parentCode) {
        String innerCode = orderNo + "";
        int nowLen = innerCode.length();
        for (int i = 5; i > nowLen; i--) {
            innerCode = "0" + innerCode;
        }
        if (!parentCode.equals("") && codeInnerCode.containsKey(parentCode)) {
            innerCode = codeInnerCode.get(parentCode) + innerCode;
        }
        return innerCode;
    }

    /**
     * 获取最后的Index
     *
     * @param code
     * @param spera
     * @return 下午3:48:06
     */
    private int getLastIndex(String code, String spera) {
        int index = 0;
        if (code.contains(spera)) {
            return code.lastIndexOf(spera);
        }
        return index;
    }

    /**
     * 根据重复字段以及来源的字段值，获取与树表重复的数据
     *
     * @param tableName      树表，仅是物理表时候使用该变量
     * @param isPhyscTable   是否物理表
     * @param queryParams    查询的来源值，key 字段，value 来源数据
     * @param sourceDataView 实体对象
     * @return 重复的数据，key 字段+字段值，value 对应重复的记录的id
     */
    private RepeatTreeVo getRepeatDataTree(ParamsVo vo,  Map<String, Set<Object>> queryParams, Set<String> excelIds, String orderNoCode, String innerCode) {
    	RepeatTreeVo returnData = new RepeatTreeVo();
        IDataView dataView = null;
        // 保存表/实体有的id，但是又不符合重复条件的id
        List<String> existIds = getInvalidData(queryParams, vo, excelIds);
        returnData.setExistIds(existIds);
        
        // 保存筛选结果 key对应字段 value对应有哪些是在数据库中重复的。
        Map<String, List<Object>> resultMap = new LinkedHashMap<String, List<Object>>();
        // 顺序号，作为更新根节点时用的。
        List<Integer> orderNos = new ArrayList<Integer>();
        // 保存查询条件 仅在目标是表时候用到该变量
        Map<String, Object> findMap = new LinkedHashMap<String, Object>();
        String whereStr; {
	        StringBuilder sb = new StringBuilder(" 1 = 1 and ");
	        // 保存哪些key是用作重复判断的
	        List<String> checkFields = new ArrayList<String>();
	       
	        for (String key : queryParams.keySet()) {
	            Set<Object> valueList = queryParams.get(key);
	            if (valueList.size() > 0) {
	            	sb.append(key).append(" in(:").append(key).append(") and ");
	                findMap.put(key, queryParams.get(key));
	            }
	            checkFields.add(key);
	        }
	        whereStr = sb.substring(0, sb.length() - 4);
        }
        // 根节点的标识
        String parentIden = VdsUtils.uuid.generate();
        // 保存每条重复的数据中的顺序号，用父节点做key,如果是根节点，则用一个随机编码标识
        Map<String, List<Integer>> repeatOrderNo = new LinkedHashMap<String, List<Integer>>();

        List<?> matchDatas ;
        boolean isPhyscTable = vo.isPhyscTable();
        if (isPhyscTable) {
            IDAS das = getVDS().getDas();
            dataView = das.find("select * from " + vo.getTableName() + " where " + whereStr, findMap);
            matchDatas =  dataView.getDatas();
        } else {
        	IDataView dv = vo.getSourceDataView();
            matchDatas =  dv.select(whereStr, findMap);
            dataView = dv;
        }
        List<String> checkFields = vo.getCheckItems();
        for (int i = 0; i < matchDatas.size(); i++) {
            Object singleRecord = matchDatas.get(i);
            String innercode = getValue(singleRecord, innerCode, isPhyscTable);// 层级码
            // 2020-07-07
            // ------新增------
            // 表中树形数据有可能出现非法的innercode
            if (innercode == null || "".equals(innercode) || innercode.length() < 5) {
                throw new BusinessException("表中数据存在非法层级码字段 值=" + innercode);
            }
            // ------新增------
            String pIden = (innercode.substring(0, innercode.length() - 5)).equals("") ? parentIden : innercode.substring(0, innercode.length() - 5);
            List<Integer> orders = repeatOrderNo.get(pIden);
            if (orders == null) {
                orders = new ArrayList<Integer>();
                repeatOrderNo.put(pIden, orders);
            }
            Object orderNo = getValue(singleRecord, orderNoCode, isPhyscTable);// 顺序号
            orders.add(Integer.parseInt(orderNo + ""));
            Collections.sort(orders);
            if (innercode != null && innercode.length() == 5) {
                orderNos.add(Integer.parseInt(orderNo + ""));
            }
            StringBuilder sb = new StringBuilder();
            for (String columnName : checkFields) {
                //String columnName = checkFields.get(j);
                Object value = getValue(singleRecord, columnName, isPhyscTable);
                if (value instanceof Number) {
                    value = new Double(((Number) value).doubleValue());
                }
                //String codeValue = columnName + "=" + value;
                sb.append(columnName).append('=').append(value).append(',');
            }
            if (sb.length()==0) {
                continue;
            }
            String tmpKey = "[" + sb.substring(0, sb.length() - 1) + "]";
            List<Object> ids = resultMap.get(tmpKey);
            if (ids == null) {
                ids = new ArrayList<Object>();
                resultMap.put(tmpKey, ids);
            }
            if (isPhyscTable) {
                String id =  getValue(singleRecord, "id", isPhyscTable);
                if (id != null) {
                    ids.add(id);
                }
            } else {
                ids.add(singleRecord);
            }
        }
       // returnData.put("matchData", resultMap);
        returnData.setMatchData(resultMap);
        /*Collections.sort(orderNos);
        returnData.put("orderNos", orderNos);
        returnData.put("repeatOrderNo", repeatOrderNo);
        returnData.put("parentIden", parentIden);*/
        returnData.setRepeatOrderNo(repeatOrderNo);
        returnData.setParentIden( parentIden);
        return returnData;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
	private <T> T getValue(Object source, String field, boolean isPhyscTable) {
    	Object t;
    	if(isPhyscTable) {
    		Map  m = (Map) source;
    		t = m.get(field);
    	}
    	else {
    		IDataObject ds = (IDataObject) source;
    		t = ds.get(field);
    	}
        return (T)t;
    }

    private List<String> getInvalidData(Map<String, Set<Object>> queryParams,ParamsVo vo, Set<String> excelIds) {
        
        // 如果重复检查条件只有检查id，就不需要判断是否有id重复了。
        if (excelIds.size() < 1 || (queryParams.size() == 1 && queryParams.containsKey("id"))) {
            return Collections.emptyList();
        }
        // 查询条件
        Map<String, Object> conditionMap = new HashMap<String, Object>();
        conditionMap.put("ids", excelIds); 
        String whereStr ;{//= new StringBuilder(" id in(:ids) and ");{
        	StringBuilder where =new StringBuilder( "(");
	        for (String key : queryParams.keySet()) {
	            if (key.equals("id")) {
	                continue;
	            }
	            Set<Object> valueList = queryParams.get(key);
	            if (valueList.size() > 0) {
	            	where.append(key).append(" not in(:").append(key).append(") or ");
	                conditionMap.put(key, queryParams.get(key));
	            }
	        }
	        if(where.length()==1) {//.equals("(")
	        	whereStr=" id in(:ids)";
	        }
	        else {
	        	whereStr =  " id in(:ids) and " + where.substring(0, where.length() - 3) + ")";
	        }
        }
    
        List<String> existIds = new ArrayList<String>();
        boolean isPhyscTable = vo.isPhyscTable();
        List<?> existIdDatas = null ;
        if (!isPhyscTable) {// 实体
        	IDataView dv = vo.getSourceDataView();
            existIdDatas  = dv.select(whereStr, conditionMap);
        } else {// 表
            IDAS das = getVDS().getDas();
            IDataView dataView = das.find("select * from " + vo.getTableName() + " where " + whereStr, conditionMap);
            if (dataView != null) {
                existIdDatas = dataView.getDatas();
            }
        }
        if(existIdDatas == null) {
        	existIdDatas = Collections.emptyList();
        }
        for (Object object:existIdDatas) {
            String id = getValue (object,"id" ,isPhyscTable);
            if (id != null && id.length()>0) {
                existIds.add(id);
            }
        }
        //existIdDatas.clear();
        //conditionMap.clear();
        return existIds;
    }

    
    /**
     * 根据重复字段以及来源的字段值，获取与表/实体重复的数据
     *
     * @param tableName      表名/实体名，仅是物理表时候使用该变量
     * @param isPhyscTable   是否物理表
     * @param queryParams    查询的来源值，key 字段，value 来源数据
     * @param sourceDataView 实体对象
     * @return 重复的数据，key 字段+字段值，value 对应重复的记录的id
     */
    //private Map<String, Object> getRepeatData(Map<String, Set<Object>> queryParams, IDataView sourceDataView1, Set<String> excelIds, String tableName, boolean isPhyscTable, List<String> checkFields) {
    private RepeatVo getRepeatData(Map<String, Set<Object>> queryParams, ParamsVo vo, Set<String> excelIds) {
        //Map<String, Object> returnData = new HashMap<String, Object>();
    	boolean physcTable = vo.isPhyscTable();
        if (physcTable) { // 物理表
            int dbDataCount = getDataCountSize(vo.getTableName(), null, null); // 获取数据库的记录数
            if (dbDataCount == 0) { // 数据库没记录不去查
                //returnData.put(D_ExistId, new ArrayList<String>());
                //returnData.put("matchData", new HashMap<String, List<Object>>());
                return new RepeatVo();
            }
        }
        // 保存表/实体有的id，但是又不符合重复条件的id
        List<String> existIds = getInvalidData(queryParams, vo, excelIds);
        RepeatVo result = new RepeatVo();
        result.setExistIds(existIds);
        
        // 保存查询条件 仅在目标是表时候用到该变量
        StringBuilder condSqlSb = null;
        //Set<String> queryParamNames = queryParams.keySet();
        Map<String, Object> findMap = new HashMap<String, Object>();
        for (Entry<String, Set<Object>> e : queryParams.entrySet()) {
            //Set<Object> valueList = queryParams.get(parName);
            if (e.getValue().size() > 0) {
            	String parName = e.getKey();
                if (condSqlSb != null) {
                    condSqlSb.append(" and ");
                } else {
                    condSqlSb = new StringBuilder();
                }
                condSqlSb.append(parName).append(" in(:").append(parName).append(")");
                findMap.put(parName, queryParams.get(parName));
            }
        }
        if (condSqlSb == null) {
            condSqlSb = new StringBuilder("1=1");
        }
        // 保存筛选结果 key对应字段 value对应有哪些是在数据库中重复的。
        List<?> matchDatas;
        IDataView dataView ;
        if (physcTable) {
            IDAS das = getVDS().getDas();
            dataView = das.find("select * from " + vo.getTableName() + " where " + condSqlSb.toString(), findMap);
            matchDatas  =dataView.getDatas(); 
        } else {
        	IDataView dv = vo.getSourceDataView();
        	matchDatas= dv.select(condSqlSb.toString(), findMap);
            // vo.getSourceDataView().select(condSqlSb.toString(), findMap) ;
            dataView = dv;//sourceDataView;
        } 
        
        List<String> checkFields = vo.getCheckItems();
        Map<String, List<Object>> resultMap = new HashMap<String, List<Object>>();
        
        for (Object singleRecord:matchDatas) { 
            StringBuilder tmpKey = new StringBuilder();
            for (String  columnName : checkFields) {
                //String columnName = checkFields.get(j);
                Object value = getValue(singleRecord, columnName, physcTable);
                if (value instanceof Number) {
                    value = new Double(((Number) value).doubleValue());
                } 
                tmpKey.append(columnName).append('=').append(value).append(',');
            }
            if (tmpKey.length() ==0) {
                continue;
            }
            String key =  "[" + tmpKey.substring(0, tmpKey.length() - 1) + "]";
            List<Object> ids = resultMap.get(key);
            if (ids == null) {
                ids = new ArrayList<Object>();
                resultMap.put(key, ids);
            }
            if (physcTable) {
            	Map rd = (Map) singleRecord;
                String id = (String)rd.get("id");
                if (id != null) {
                    ids.add(id);
                }
            } else {
                ids.add(singleRecord);
            }
        }
        result.setMatchData( resultMap);
        return result;
    }

 
    /**
     * 取来源实体
     *
     * @param context    规则上下文
     * @param sourceName 来源名称
     * @param sourceType 来源类型
     * @return 
    private IDataView getDataViewWithType(IRuleContext context, String sourceName, String sourceType) {
        IDataView sourceDV = null;
        // 活动集输入变量 ruleSetInput
        // 活动集输出变量 ruleSetOutput
        // 活动集上下文变量 ruleSetVar
        if ("ruleSetInput".equalsIgnoreCase(sourceType) || "ruleSetOutput".equalsIgnoreCase(sourceType) || "ruleSetVar".equalsIgnoreCase(sourceType)) {
            ContextVariableType instanceType = ContextVariableType.getInstanceType(sourceType);
            sourceDV = (IDataView) context.getVObject().getContextObject(sourceName, instanceType);
        } else if ("table".equalsIgnoreCase(sourceType)) {   // 窗体实体
            sourceDV = getVDS().getDas().createDataViewByName(sourceName);
        } else {
            throw new ConfigException("导入Excel规则 : 不支持类型[" + sourceType + "]的变量值设置.");
        }
        return sourceDV;
    }*/

	
    /**
     * 判断是否配与导入ID
     */
    private boolean isExistImportId(List<Map<String, Object>> mapping) {
        for (int i = 0; i < mapping.size(); i++) {
            Map<String, Object> map = mapping.get(i);
            if (map.get("fieldCode").equals("id")) {
                return true;
            }
        }
        return false;
    }



    /**
     * 获取数据量
     *
     * @param tableName    表名
     * @param sqlCondition 查询条件
     * @return
     */
    private int getDataCountSize(String tableName, String sqlCondition, Map<String, Object> paramMap) {
        IDAS das = getVDS().getDas();
        StringBuilder sb = new StringBuilder("select count(id) amount from ").append(tableName);
        IDataView countDataView = null;
        if (sqlCondition != null) {
            sb.append(" where ").append(sqlCondition);
            countDataView = das.find(sb.toString(), paramMap);
        } else {
            countDataView = das.findWithNoFilter(sb.toString(), Collections.emptyMap());
        }
        Object amouts = countDataView.select().get(0).get("amount");
        int amount;
        if(amouts == null ) {
        	amount =0;
        }
        else if(amouts instanceof Number) {
        	amount = ((Number) amouts).intValue();
        }
        else {
        	amount = Integer.valueOf(amouts.toString());
        }
        return amount;
    }

    private IVDS getVDS() {
        return VDS.getIntance();
    }
    
    
    private class RepeatVo{
    	private List<String> existIds;
    	private Map<String, List<Object>> matchData; 
    	public RepeatVo() {
    		this(Collections.emptyList(),Collections.emptyMap());
    	}
    	public RepeatVo(List<String> existIds,Map<String, List<Object>> matchData) {
    		this.existIds = existIds;
    		this.matchData = matchData;
    	}
		public List<String> getExistIds() {
			return existIds;
		}
		public void setExistIds(List<String> existIds) {
			this.existIds = existIds;
		}
		public Map<String, List<Object>> getMatchData() {
			return matchData;
		}
		public void setMatchData(Map<String, List<Object>> matchData) {
			this.matchData = matchData;
		}
    }
    private class RepeatTreeVo extends RepeatVo{ 
    	private Map<String, List<Integer>> repeatOrderNo;
    	private String parentIden;
    	public RepeatTreeVo() {
    	}

		public Map<String, List<Integer>> getRepeatOrderNo() {
			return repeatOrderNo;
		}

		public void setRepeatOrderNo(Map<String, List<Integer>> repeatOrderNo) {
			this.repeatOrderNo = repeatOrderNo;
		}

		public String getParentIden() {
			return parentIden;
		}

		public void setParentIden(String parentIden) {
			this.parentIden = parentIden;
		} 
    }
}
