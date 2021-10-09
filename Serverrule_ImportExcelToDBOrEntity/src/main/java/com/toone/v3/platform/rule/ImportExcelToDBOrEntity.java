package com.toone.v3.platform.rule;

import com.toone.v3.platform.rule.model.*;
import com.toone.v3.platform.rule.util.FPOExcel;
import com.toone.v3.platform.rule.util.ObjPropertyUtil;
import com.yindangu.commons.CaseInsensitiveLinkedMap;
import com.yindangu.v3.business.VDS;
import com.yindangu.v3.business.file.api.model.IAppFileInfo;
import com.yindangu.v3.business.jdbc.api.model.ColumnType;
import com.yindangu.v3.business.jdbc.api.model.DataState;
import com.yindangu.v3.business.jdbc.api.model.IDataSetMetaData;
import com.yindangu.v3.business.metadata.api.IDAS;
import com.yindangu.v3.business.metadata.api.IDataObject;
import com.yindangu.v3.business.metadata.api.IDataView;
import com.yindangu.v3.business.plugin.business.api.rule.ContextVariableType;
import com.yindangu.v3.business.plugin.business.api.rule.IRule;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleContext;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleOutputVo;
import com.yindangu.v3.business.plugin.execptions.BusinessException;
import com.yindangu.v3.business.plugin.execptions.ConfigException;
import com.yindangu.v3.business.vds.IVDS;
import com.yindangu.v3.platform.plugin.util.VdsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.math.BigDecimal;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Author xugang
 * @Date 2021/7/23 16:19
 */
public class ImportExcelToDBOrEntity implements IRule {

    private static final String IMPORTBY = "importBy";
    private static final String BIZCODEFIELD = "filiationField";
    private static final String BIZCODEFORMAT = "filiationFormat";
    private static final String BIZCODECONFIG = "filiationConfig";
    private static final Logger logger = LoggerFactory.getLogger(ImportExcelToDBOrEntity.class);
    // 约定的树型编码每级的长度
    private int innerCodeLength = 5;

    @Override
    public IRuleOutputVo evaluate(IRuleContext context) {
        long starTime = System.currentTimeMillis();
        String fileId = getVDS().getFormulaEngine().eval(context.getPlatformInput("fileSource").toString()); // 文件标识
        if (fileId != null && !fileId.equals("")) {

            InputStream inputStream = null;
            List<Map<String, Object>> itemsList = (List<Map<String, Object>>) context.getPlatformInput("items");
            try {
                for (Map<String, Object> singleItem : itemsList) {
                    IAppFileInfo appFileInfo = getVDS().getFileOperate().getFileInfo(fileId);
                    if (appFileInfo == null) {
                        throw new ConfigException("后台导入Excel规则，获取文件标识为【" + fileId + "】文件对象，请检查");
                    }
                    inputStream = appFileInfo.getDataStream();
                    importToDB(context, singleItem, inputStream);
                }
            } catch (Exception e) {
                throw new ConfigException("后台导入Excel规则，执行失败！\n" + e.getMessage(), e);
            } finally {
                try {
                    if (inputStream != null) {
                        inputStream.close();
                    }
                } catch (Exception e) {
                    logger.warn("后台导入Excel规则，关闭流异常");
                }
            }
        } else {
            throw new ConfigException("后台导入Excel规则 ，文件标识不能为空，请检查");
        }
        logger.info("后台导入Excel数据,总时长：" + (System.currentTimeMillis() - starTime) + "毫秒");

        IRuleOutputVo outputVo = context.newOutputVo();
        outputVo.put(null);
        return outputVo;
    }

    // 20170423 最新
    public IDataView importToDB(IRuleContext context, Map<String, Object> cfgMap, InputStream inputStream) {

        List<Map<String, Object>> mappings = (List<Map<String, Object>>) cfgMap.get("mapping");// 配置映射
        String tableName = (String) cfgMap.get("target"); // 导入的目标表名
        String targetType = (String) cfgMap.get("targetType"); // 目标类型
        boolean isPhyscTable = targetType.equals("table"); // 是否物理表
        int startRow = Integer.valueOf((String) cfgMap.get("dataStartRow")); //开始行号

        // 是否树(当)
        List<Map> treeStruct = (List<Map>) cfgMap.get("treeStruct");
        Map treeStructMap = (treeStruct == null || treeStruct.isEmpty()) ? null : treeStruct.get(0);//
        boolean isTree = treeStructMap != null && !TreeType.BizCode.equals(treeStructMap.get("type"));

        // 重复处理方式，目前仅是替换
        String repeatOperation = (String) cfgMap.get("repeatOperation");
        if (cfgMap.get("repeatOperation") == null || repeatOperation.equals("")) {
            repeatOperation = "replace";
        }

        Map<String, Object> varMap = new HashMap<String, Object>(); // 配置了表达式的值
        boolean isImportId = getOtherData(mappings, varMap); // 是否存在id导入
        List<String> checkItems = (List<String>) cfgMap.get("checkItems"); // 获取检查重复的字段
        // 如果不导入id，就不检查id，其他情况都检查id
        if (checkItems == null || checkItems.size() == 0) {
            checkItems = new ArrayList<String>();
        }
        if (isImportId && !checkItems.contains("id")) {
            checkItems.add("id");
        }

        String cfgStr = VdsUtils.json.toJson(cfgMap);
        ExcelImportDataCfg dCfg = VdsUtils.json.fromJson(cfgStr, ExcelImportDataCfg.class);
        IDataView sourceDataView = null;
        if (!isPhyscTable) {
            sourceDataView = getDataViewWithType(context, tableName, targetType);// 用于获取字段类型
        }

        FModelMapper mapper = getModelMapperByExcelImportCfg(dCfg, isPhyscTable, sourceDataView);
        IDataView dataView = null;
        if (!isPhyscTable) {
            dataView = getDataViewWithType(context, tableName, targetType);// 内存表
        } else {
            dataView = getVDS().getDas().createDataViewByName(tableName);// 物理表
        }
        IDataSetMetaData dataSetMetaData = dataView.getMetadata();
        List<Map<String, Object>> result = getExcelData(cfgMap.get("sheetNum"), dCfg, mapper, inputStream, dataSetMetaData); // excel数据
        //long start = System.currentTimeMillis();

        //过滤树字段
        String filterTreeFied = "";
        if (isTree) {
            filterTreeFied = (String) treeStructMap.get(TreeColumn.BusiFilterField.columnName());
        }
        Map<String, List<Map<String, Object>>> groupResult = new HashMap<String, List<Map<String, Object>>>();
        if (filterTreeFied.equalsIgnoreCase("")) { //没有过滤树字段
            groupResult.put("nogroup", result);
        } else {

            //将Excel数据按照过滤树字段进行分组
            for (Map<String, Object> map : result) {
                String fiterValue = (String) map.get(filterTreeFied) == null ? ((String) varMap.get(filterTreeFied)) : ((String) map.get(filterTreeFied));//过滤字段值
                if (fiterValue == null || fiterValue.trim().equals("")) { //过滤字段值为空
                    throw new BusinessException("过滤树字段值为空");
                }

                if (groupResult.get(fiterValue) == null) {
                    List<Map<String, Object>> fiterList = new ArrayList<Map<String, Object>>();
                    fiterList.add(map);
                    groupResult.put(fiterValue, fiterList);
                } else {
                    groupResult.get(fiterValue).add(map);

                }
            }
        }
        int groupNum = 0; //分组序号
        for (String key : groupResult.keySet()) {
            result = groupResult.get(key);

            Set<String> excelIds = new HashSet<String>(); // 保存Excel的全部id值，如果不导入id，则无数据。用于判断是否有重复id导入。
            Map<String, Set<Object>> sourceFieldData = new LinkedHashMap<String, Set<Object>>(); // 获取检查重复字段在Excel中的数据
            for (String item : checkItems) {
                sourceFieldData.put(item, new HashSet());
            }

            for (Map<String, Object> map : result) {
                String rowId = (String) map.get("id");
                if (rowId != null) {
                    if (excelIds.contains(rowId)) {
                        throw new BusinessException("后台导入Excel规则 ，Excel数据存在id重复，请检查");
                    }
                    excelIds.add(rowId);
                }

                // 把其他表达式的值集合加到数据集里
                if (varMap != null && !varMap.isEmpty()) {
                    map.putAll(varMap);
                }

                for (String item : checkItems) {
                    Object value = null;
                    if ("id".equals(item)) {
                        value = rowId;
                    } else {
                        value = map.get(item);
                    }
                    if (value != null) {
                        sourceFieldData.get(item).add(value);
                    }
                }
            }
            // 获取重复记录数据
            Map<String, Object> repeatData = null;
            // 最后结果
            if (!isTree) {// 树结构为空，则按照普通表/实体导入
                // 查询重复数据
                repeatData = getRepeatData(sourceFieldData, sourceDataView, excelIds, tableName, isPhyscTable, checkItems);

                try {
                    // 处理数据
                    // 完善异常，报错的行号为--startRow+excel中的行号
                    dataView = handleCommonData(repeatData, result, dataView, checkItems, isPhyscTable);
                } catch (RuntimeException ex) {
                    Matcher matcher = Pattern.compile(".*第(\\d+)行.*").matcher(ex.getMessage());
                    if (matcher.matches()) {
                        int beforeRow = Integer.valueOf(matcher.group(1));
                        int row = beforeRow + startRow;
                        String str = ex.getMessage().replaceFirst("第" + beforeRow + "行", "第" + row + "行");
                        throw new RuntimeException(str);
                    } else {
                        throw new RuntimeException(ex);
                    }
                }

                // 更新数据
                handleResultDataView(dataView, tableName, targetType, context);
                return null;
            }

            // 是否导入业务编码
            String importBy = (String) cfgMap.get(IMPORTBY);
            // 业务编码配置
            boolean isBizCodeTree = false;
            if (importBy != null && importBy.equals("filiation")) {
                isBizCodeTree = true;
            }
            String selectId = "";
            // 得到导入树的层级码每级长度
            String treeCodeLength = "00000";
            if (isNotEmpty(treeCodeLength)) {
                innerCodeLength = treeCodeLength.length();
            }
            String orderColumn = (String) treeStructMap.get(TreeColumn.OrderField.columnName());
            String innerCodeColumn = (String) treeStructMap.get(TreeColumn.TreeCodeField.columnName());
            repeatData = getRepeatDataTree(tableName, sourceDataView, sourceFieldData, excelIds, orderColumn, innerCodeColumn, isPhyscTable);
            if (isPhyscTable) {
                dataView = getVDS().getDas().createDataViewByName(tableName);
            } else {
                if (groupNum == 0) { //每个分组都用同一个内存表
                    dataView = getDataViewWithType(context, tableName, targetType);
                }
                groupNum++;
            }
            handelEntityOrTableTree(result, treeStructMap, dataView, selectId, isImportId, isBizCodeTree, cfgMap, tableName, isPhyscTable, repeatData, checkItems);

            handleResultDataView(dataView, tableName, targetType, context);
        }
        return null;
    }

    private void handleResultDataView(IDataView dataView, String tableName, String targetType, IRuleContext context) {
        if (dataView != null) {
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
            }
        }
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
    private IDataView handleCommonData(Map<String, Object> repeatDataMap, List<Map<String, Object>> excelData, IDataView dataView, List<String> checkItems, boolean isPhyscTable) {
        try {

            // 在表中存在，却不符合检查条件的id列表
            List<String> existId = (List<String>) repeatDataMap.get("existId");
            // 符合重复条件的id列表
            Map<String, List<Object>> repeatData = (Map<String, List<Object>>) repeatDataMap.get("matchData");
            if (repeatData.size() == 0) { // 全新增批量处理
                dataView.insertDataObject(excelData);
                return dataView;
            }
            for (Map<String, Object> map : excelData) {
                String rowId = (String) map.get("id");
                // 判断记录是否为重复记录，tmpKey有值表示是重复记录。
                String tmpKey = checkDataRepeatForKey(map, repeatData, checkItems);
                if (tmpKey != null && !isPhyscTable) {// 当前记录是属于重复记录,更新实体对应的记录即可
                    List<IDataObject> matchDatas = ((List<IDataObject>) (Object) repeatData.get(tmpKey));
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
                if (tmpKey != null && isPhyscTable) {// 校验当前记录是否属于重复记录
                    List<String> ids = ((List<String>) (Object) repeatData.get(tmpKey));
                    if (ids != null) {
                        for (int i = 0; i < ids.size(); i++) {
                            dataObject.setStates(DataState.Modified, ids.get(i));
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new ConfigException("无法更新实体表数据，请检查数据类型与字段类型是否一致.", e);
        }
        return dataView;
    }

    public boolean isNumeric(String str) {
        Pattern pattern = Pattern.compile("[0-9]*");
        Matcher isNum = pattern.matcher(str);
        if (!isNum.matches()) {
            return false;
        }
        return true;
    }

    public static String displayWithComma(String str) {
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
        Boolean num = true;
        if (tmpCode == null || tmpCode.equals("")) {
            throw new BusinessException(nullTip);
        } else {
            String pattern = "^\\d+(\\" + sep + "\\d+)?";
            num = Pattern.matches(pattern, tmpCode);
        }
        // 层级码或父子关系字段
        final String fCode = fieldCode;
        // 分隔符
        final String SEPARTOR = sep + "";
        // 是否是数字code
        final boolean numCode = num;

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
        if (str != null && !str.equals("")) {
            return true;
        }

        return false;
    }

    private int getLastOrderNoTableOrEntity(String tableName, String innerCodeField, String orderNoField, IDataView sourceDataView, boolean isPhyscTable, String parentCode) {
        int orderNo = 1;
        IDataView dataView = null;
        Map<String, Object> queryParams = new HashMap<String, Object>();
        queryParams.put("innerCodeParam", parentCode + "_____");
        String wherestr = innerCodeField + " like :innerCodeParam order by " + innerCodeField + " desc";
        if (isPhyscTable) {
            IDAS das = getVDS().getDas();
            String sql = "select * from " + tableName + " where " + wherestr;
            dataView = das.find(sql, queryParams);
            if (dataView != null) {
                List<Map<String, Object>> orderNoMaxData = dataView.getDatas();
                if (orderNoMaxData.size() > 0) {
                    Map<String, Object> singleRecord = orderNoMaxData.get(0);
                    orderNo = (Integer) singleRecord.get(orderNoField);
                    return orderNo;
                }
            }
        } else {
            List<IDataObject> orderNoMaxData = sourceDataView.select(wherestr, queryParams);
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
    private boolean handelEntityOrTableTree(List<Map<String, Object>> result, Map<String, Object> treeStruts, IDataView dataView, String selectId, boolean isImportId, boolean isBizCodeTree,
                                            Map<String, Object> cfgMap, String tableName, boolean isPhyscTable, Map<String, Object> repeatDataMap, List<String> checkItems) {
        // 重复的数据
        Map<String, List<Object>> repeatData = (Map<String, List<Object>>) repeatDataMap.get("matchData");
        List<String> existId = (List<String>) repeatDataMap.get("existId");
        // 数据库中根节点的顺序号
        Map<String, List<Integer>> repeatOrderNo = (Map<String, List<Integer>>) repeatDataMap.get("repeatOrderNo");

        String parentColumn = (String) treeStruts.get(TreeColumn.ParentField.columnName());
        String treeCodeColumn = (String) treeStruts.get(TreeColumn.TreeCodeField.columnName());
        String isLeafColumn = (String) treeStruts.get(TreeColumn.IsLeafField.columnName());
        String orderColumn = (String) treeStruts.get(TreeColumn.OrderField.columnName());

        String bizCodeField = isBizCodeTree ? (String) cfgMap.get(BIZCODEFIELD) : treeCodeColumn;
        String bizCodeFormat = (String) cfgMap.get(BIZCODEFORMAT);
        Object bizCodeConfig = null;
        if (isBizCodeTree)
            bizCodeConfig = getVDS().getFormulaEngine().eval((String) cfgMap.get(BIZCODECONFIG));

        // 根据父子关系字段排序
        result = bizCodeConfig != null ? getSortData(bizCodeConfig, isBizCodeTree, result, bizCodeField) : sortData(result, bizCodeField, isBizCodeTree);

        // 临时缓存,用于设置叶子节点,key=code,value=AbstractTreeDataObject
        Map<String, Object> idMap = new LinkedHashMap<String, Object>();

        // 保存某个编码是否属于新增
        Map<String, Boolean> isInsert = new LinkedHashMap<String, Boolean>();

        // 过滤树内置字段
        Set<String> filterFieldSet = getFilterFieldForTree(treeStruts, isImportId);

        // 保存每个编码下有多少个子节点
        Map<String, Integer> codeOrder = new LinkedHashMap<String, Integer>();

        Map<String, String> codeInnerCode = new LinkedHashMap<String, String>();
        // 根节点临时标识
        String rootCode = (String) repeatDataMap.get("parentIden");
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
            String repeatKey = checkDataRepeatForKey(singleMap, repeatData, checkItems);
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
                pId = selectId;
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
                        maxOrderNo = getLastOrderNoTableOrEntity(tableName, treeCodeColumn, orderColumn, dataView, isPhyscTable, parentNodeInnerCode);
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
            if (isPhyscTable) {
                dataObject = dataView.insertDataObject();
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
                    List<String> ids = (List<String>) (Object) repeatData.get(repeatKey);
                    for (int j = 0; j < ids.size(); j++) {
                        // 如果不检查id的话，更新的时候不更新id
                        if (!checkItems.contains("id"))
                            dataObject.setId(ids.get(j));
                        dataObject.setStates(DataState.Modified, ids.get(j));
                    }
                    isInsert.put(innerCode, false);
                } else {
                    isInsert.put(innerCode, true);// 记录当前节点为新增
                }
            } else {
                if (repeatKey != null) {
                    List<IDataObject> ids = (List<IDataObject>) (Object) repeatData.get(repeatKey);
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
                    dataObject = dataView.insertDataObject();
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
    private Map<String, Object> getRepeatDataTree(String tableName, IDataView sourceDataView, Map<String, Set<Object>> queryParams, Set<String> excelIds, String orderNoCode, String innerCode,
                                                  boolean isPhyscTable) {
        Map<String, Object> returnData = new LinkedHashMap<String, Object>();
        IDataView dataView = null;
        // 保存表/实体有的id，但是又不符合重复条件的id
        List<String> existIds = getInvalidData(queryParams, sourceDataView, tableName, excelIds, isPhyscTable);
        returnData.put("existId", existIds);
        // 保存筛选结果 key对应字段 value对应有哪些是在数据库中重复的。
        Map<String, List<Object>> resultMap = new LinkedHashMap<String, List<Object>>();
        // 顺序号，作为更新根节点时用的。
        List<Integer> orderNos = new ArrayList<Integer>();
        String whereStr = " 1 = 1 and ";
        // 保存哪些key是用作重复判断的
        List<String> checkFields = new ArrayList<String>();
        // 保存查询条件 仅在目标是表时候用到该变量
        Map<String, Object> findMap = new LinkedHashMap<String, Object>();
        for (String key : queryParams.keySet()) {
            Set<Object> valueList = queryParams.get(key);
            if (valueList.size() > 0) {
                whereStr = whereStr + key + " in(:" + key + ") and ";
                findMap.put(key, queryParams.get(key));
            }
            checkFields.add(key);
        }
        whereStr = whereStr.substring(0, whereStr.length() - 4);
        // 根节点的标识
        String parentIden = VdsUtils.uuid.generate();
        // 保存每条重复的数据中的顺序号，用父节点做key,如果是根节点，则用一个随机编码标识
        Map<String, List<Integer>> repeatOrderNo = new LinkedHashMap<String, List<Integer>>();

        List<Object> matchDatas = null;
        if (isPhyscTable) {
            IDAS das = getVDS().getDas();
            dataView = das.find("select * from " + tableName + " where " + whereStr, findMap);
            matchDatas = (List<Object>) ((Object) dataView.getDatas());
        } else {
            matchDatas = (List<Object>) ((Object) sourceDataView.select(whereStr, findMap));
            dataView = sourceDataView;
        }
        for (int i = 0; i < matchDatas.size(); i++) {
            Object singleRecord = matchDatas.get(i);
            String innercode = (String) getValue(singleRecord, innerCode, isPhyscTable);// 层级码
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
            String tmpKey = "";
            for (int j = 0; j < checkFields.size(); j++) {
                String columnName = checkFields.get(j);
                Object value = getValue(singleRecord, columnName, isPhyscTable);
                if (value instanceof BigDecimal)
                    value = ((BigDecimal) value).doubleValue();
                else if (value instanceof Integer)
                    value = Double.parseDouble(value.toString());
                String codeValue = columnName + "=" + value;
                tmpKey = tmpKey + codeValue + ",";
            }
            if (tmpKey.equals(""))
                continue;
            tmpKey = "[" + tmpKey.substring(0, tmpKey.length() - 1) + "]";
            List<Object> ids = resultMap.get(tmpKey);
            if (ids == null) {
                ids = new ArrayList<Object>();
                resultMap.put(tmpKey, ids);
            }
            if (isPhyscTable) {
                String id = (String) getValue(singleRecord, "id", isPhyscTable);
                if (id != null)
                    ids.add(id);
            } else {
                ids.add(singleRecord);
            }
        }
        returnData.put("matchData", resultMap);
        Collections.sort(orderNos);
        returnData.put("orderNos", orderNos);
        returnData.put("repeatOrderNo", repeatOrderNo);
        returnData.put("parentIden", parentIden);
        return returnData;
    }

    private Object getValue(Object source, String field, boolean isPhyscTable) {
        return isPhyscTable ? ((Map<String, Object>) source).get(field) : ((IDataObject) source).get(field);
    }

    private List<String> getInvalidData(Map<String, Set<Object>> queryParams, IDataView sourceDataView, String tableName, Set<String> excelIds, boolean isPhyscTable) {
        List<String> existIds = new ArrayList<String>();
        // 如果重复检查条件只有检查id，就不需要判断是否有id重复了。
        if (excelIds.size() < 1 || (queryParams.size() == 1 && queryParams.containsKey("id")))
            return existIds;
        // 查询条件
        Map<String, Object> conditionMap = new HashMap<String, Object>();
        conditionMap.put("ids", excelIds);
        String whereStr = " id in(:ids) and ";
        String tmp_where = "(";
        for (String key : queryParams.keySet()) {
            if (key.equals("id"))
                continue;
            Set<Object> valueList = queryParams.get(key);
            if (valueList.size() > 0) {
                tmp_where = tmp_where + key + " not in(:" + key + ") or ";
                conditionMap.put(key, queryParams.get(key));
            }
        }
        whereStr = tmp_where.equals("(") ? whereStr.substring(0, whereStr.length() - 4) : whereStr + tmp_where.substring(0, tmp_where.length() - 3) + ")";
        List<Object> existIdDatas = new ArrayList<Object>();
        if (!isPhyscTable) {// 实体
            existIdDatas = (List<Object>) ((Object) sourceDataView.select(whereStr, conditionMap));
        } else {// 表
            IDAS das = getVDS().getDas();
            IDataView dataView = das.find("select * from " + tableName + " where " + whereStr, conditionMap);
            if (dataView != null && dataView.getDatas() != null) {
                existIdDatas = (List<Object>) ((Object) dataView.getDatas());
            }
        }
        for (int i = 0; i < existIdDatas.size(); i++) {
            Object object = existIdDatas.get(i);
            String id = (String) (isPhyscTable ? ((Map<String, Object>) object).get("id") : ((IDataObject) object).get("id"));
            if (id != null && !id.equals("")) {
                existIds.add(id);
            }
        }
        existIdDatas.clear();
        conditionMap.clear();
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
    private Map<String, Object> getRepeatData(Map<String, Set<Object>> queryParams, IDataView sourceDataView, Set<String> excelIds, String tableName, boolean isPhyscTable, List<String> checkFields) {
        Map<String, Object> returnData = new HashMap<String, Object>();
        if (isPhyscTable) { // 物理表
            int dbDataCount = getDataCountSize(tableName, null, null); // 获取数据库的记录数
            if (dbDataCount == 0) { // 数据库没记录不去查
                returnData.put("existId", new ArrayList<String>());
                returnData.put("matchData", new HashMap<String, List<Object>>());
                return returnData;
            }
        }
        IDataView dataView = null;
        // 保存表/实体有的id，但是又不符合重复条件的id
        List<String> existIds = getInvalidData(queryParams, sourceDataView, tableName, excelIds, isPhyscTable);
        returnData.put("existId", existIds);
        // 保存查询条件 仅在目标是表时候用到该变量
        StringBuilder condSqlSb = null;
        Set<String> queryParamNames = queryParams.keySet();
        Map<String, Object> findMap = new HashMap<String, Object>();
        for (String parName : queryParamNames) {
            Set<Object> valueList = queryParams.get(parName);
            if (valueList.size() > 0) {
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
        Map<String, List<Object>> resultMap = new HashMap<String, List<Object>>();
        List<Object> matchDatas = null;
        if (isPhyscTable) {
            IDAS das = getVDS().getDas();
            dataView = das.find("select * from " + tableName + " where " + condSqlSb.toString(), findMap);
            matchDatas = (List<Object>) ((Object) dataView.getDatas());
        } else {
            matchDatas = (List<Object>) ((Object) sourceDataView.select(condSqlSb.toString(), findMap));
            dataView = sourceDataView;
        }
        for (int i = 0; i < matchDatas.size(); i++) {
            Object singleRecord = matchDatas.get(i);
            String tmpKey = "";
            for (int j = 0; j < checkFields.size(); j++) {
                String columnName = checkFields.get(j);
                Object value = getValue(singleRecord, columnName, isPhyscTable);
                if (value instanceof BigDecimal)
                    value = ((BigDecimal) value).doubleValue();
                else if (value instanceof Integer)
                    value = Double.parseDouble(value.toString());
                String codeValue = columnName + "=" + value;
                tmpKey = tmpKey + codeValue + ",";
            }
            if (tmpKey.equals(""))
                continue;
            tmpKey = "[" + tmpKey.substring(0, tmpKey.length() - 1) + "]";
            List<Object> ids = resultMap.get(tmpKey);
            if (ids == null) {
                ids = new ArrayList<Object>();
                resultMap.put(tmpKey, ids);
            }
            if (isPhyscTable) {
                String id = (String) ((Map<String, Object>) singleRecord).get("id");
                if (id != null) {
                    ids.add(id);
                }
            } else {
                ids.add(singleRecord);
            }
        }
        returnData.put("matchData", resultMap);
        return returnData;
    }

    /**
     * 获取excel的数据
     *
     * @param sheetNum    sheet数量（表达式）
     * @param dCfg        导入配置模型
     * @param mapper      模型映射关系
     * @param inputStream 文件流
     * @return
     */
    private List<Map<String, Object>> getExcelData(Object sheetNum, ExcelImportDataCfg dCfg, FModelMapper mapper, InputStream inputStream, IDataSetMetaData dataSetMetaData) {
        int sheetno = 0;
        try {
            sheetNum = getVDS().getFormulaEngine().eval(sheetNum.toString());
            sheetno = Integer.parseInt(sheetNum.toString());
        } catch (Exception e) {
            throw new ConfigException("获取配置的sheet序号失败", e);
        }
        List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
        // 读取Excel的数据
        result = new FPOExcel().readExcel(inputStream, mapper, sheetno, dataSetMetaData);
        return result;
    }

    private FModelMapper getModelMapperByExcelImportCfg(ExcelImportDataCfg dCfg, boolean isPhyscTable, IDataView sourceDataView) {
        FModelMapper mapper = new FModelMapper();
        mapper.setStartRow(dCfg.getDataStartRow() - 1);
        if (mapper.getStartRow() <= 0) {
            throw new ConfigException("Excel导入配置的数据起始行必须大于1");
        }
        List<ColumnImportCfg> list = dCfg.getMapping();
        Map<String, FProperty> properties = new CaseInsensitiveLinkedMap();
        List<String> fieldNameList = ObjPropertyUtil.getPropertyList(list, "fieldCode");
        Set<String> fieldNameSet = new LinkedHashSet<String>(fieldNameList);
        if (fieldNameSet.size() < fieldNameList.size()) {
            throw new ConfigException("Excel导入配置中列名有重复");
        }
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
                ColumnType type = null;
                if (isPhyscTable)
                    type = getVDS().getMdo().getTable(dCfg.getTarget()).getColumn(columnName).getColumnType();
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

    /**
     * 取来源实体
     *
     * @param context    规则上下文
     * @param sourceName 来源名称
     * @param sourceType 来源类型
     * @return
     */
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
    }

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
            countDataView = das.findWithNoFilter(sb.toString(), new HashMap());
        }
        int amount = Integer.valueOf(countDataView.select().get(0).get("amount").toString());
        return amount;
    }

    private IVDS getVDS() {
        return VDS.getIntance();
    }
}
