package com.toone.v3.platform.rule;

import com.yindangu.v3.business.VDS;
import com.yindangu.v3.business.file.api.model.IAppFileInfo;
import com.yindangu.v3.business.metadata.api.IDAS;
import com.yindangu.v3.business.metadata.api.IDataObject;
import com.yindangu.v3.business.metadata.api.IDataView;
import com.yindangu.v3.business.plugin.business.api.rule.ContextVariableType;
import com.yindangu.v3.business.plugin.business.api.rule.IRule;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleContext;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleOutputVo;
import com.yindangu.v3.business.plugin.execptions.ConfigException;
import com.yindangu.v3.business.project.microsoft.api.IProjectService;
import com.yindangu.v3.business.project.microsoft.model.ProjectData;
import com.yindangu.v3.business.project.microsoft.model.ProjectField;
import com.yindangu.v3.business.vds.IVDS;
import com.yindangu.v3.platform.plugin.util.VdsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.InputStream;
import java.text.DecimalFormat;
import java.util.*;

/**
 * @Author xugang
 * @Date 2021/7/23 10:04
 */
public class ImportProjectToDBOrEntity implements IRule {

    private static final Logger logger = LoggerFactory.getLogger(ImportProjectToDBOrEntity.class);
    /* 目标类型：表 */
    private String targetType_table = "table";
    /* 目标类型：活动集输入变量实体 */
    private String targetType_ruleSetInput = "ruleSetInput";
    /* 目标类型：活动集输出变量实体 */
    private String targetType_ruleSetOutput = "ruleSetOutput";
    /* 目标类型：活动集上下文实体 */
    private String targetType_ruleSetVar = "ruleSetVar";
    /* 重复数据处理方式：更新 */
    private String repeatOperation_replace = "replace";
    /* 映射字段来源类型：属性 */
    private String mapping_sourcetype_property = "projectProperty";
    /* 约定的树型编码每级的编码格式，长度为5 */
    private String pattern = "00000";
    /* 系统树形编码树 */
    private int tc = 1;

    @Override
    public IRuleOutputVo evaluate(IRuleContext context) {
        long starTime = System.currentTimeMillis();
//        Map<String, Object> ruleCfgParams = (Map<String, Object>) context.getRuleConfig().getConfigParams();
//        Map<String, Object> runtimeParams = context.getInputParams();

        String tagetType = (String) context.getPlatformInput("targetType"); // 目标类型
        String targetName = (String) context.getPlatformInput("target"); // 目标名称
        IDataView dataView = null;
        if (targetType_table.equals(tagetType)) {
            if (!getVDS().getMdo().hasTable(targetName)) {
                throw new ConfigException("Project导入到数据库表或实体规则，表【" + targetName + "】不存在，请检查！");
            }
        } else {
            if (targetType_ruleSetInput.equals(tagetType) || targetType_ruleSetOutput.equals(tagetType) || targetType_ruleSetVar.equals(tagetType)) {
                ContextVariableType instanceType = ContextVariableType.getInstanceType(tagetType);
                dataView = (IDataView) context.getVObject().getContextObject(targetName, instanceType);
            } else {
                throw new ConfigException("Project导入到数据库表或实体规则，不支持实体类型【" + tagetType + "】，请检查！");
            }
        }

        List<Map<String, Object>> mapping = (List<Map<String, Object>>) context.getPlatformInput("mapping"); // 映射关系
        String repeatOperation = (String) context.getPlatformInput("repeatOperation"); // 重复数据处理方式
        List<String> checkItems = (List<String>) context.getPlatformInput("checkItems"); // 重复数据校验项
        List<Map<String, String>> treeStructList = context.getPlatformInput("treeStruct") != null ? (List<Map<String, String>>) context.getPlatformInput("treeStruct") : null; // 树形结构
        Map<String, String> treeStructMap = isEmpty(treeStructList) ? null : treeStructList.get(0);

        // 获取文件信息
        String fileId = getVDS().getFormulaEngine().eval(context.getPlatformInput("fileSource").toString());
        if (isEmpty(fileId)) {
            throw new ConfigException("Project导入到数据库表或实体规则，文件id为空，请检查！");
        }
        IAppFileInfo appFileInfo = getVDS().getFileOperate().getFileInfo(fileId);
        if (appFileInfo == null) {
            throw new ConfigException("Project导入到数据库表或实体规则，获取不到文件id为【" + fileId + "】的文件信息，请检查！");
        }
        String fileName = appFileInfo.getOldFileName();
        InputStream inputStream = appFileInfo.getDataStream();

        // 解析Project文件
        try {
            Map<Integer, ProjectData> projectDataMap = getVDS().getProjectService().readInputStream(inputStream, fileName);
            if (targetType_table.equals(tagetType)) {
                this.covertDataSaveToDB(projectDataMap, targetName, mapping, repeatOperation, checkItems, treeStructMap);
            } else {
                this.covertDataSaveToDV(projectDataMap, dataView, mapping, repeatOperation, checkItems, treeStructMap);
            }
        } catch (Exception e) {
            throw new ConfigException("Project导入到数据库表或实体规则， 导入失败！信息如下：\n" + e.getMessage(), e);
        } finally {
            quiteClose(inputStream);
        }
        logger.info("后台导入Project数据总时长：" + (System.currentTimeMillis() - starTime) + "毫秒");

        IRuleOutputVo outputVo = context.newOutputVo();
        outputVo.put(null);
        return outputVo;
    }

    /**
     * 将Project数据存到实体中
     *
     * @param projectDataMap
     * @param dataview
     * @param mapping
     * @param repeatOperation
     * @param checkItems
     * @param treeStructMap
     */
    private void covertDataSaveToDV(Map<Integer, ProjectData> projectDataMap, IDataView dataview, List<Map<String, Object>> mapping, String repeatOperation, List<String> checkItems,
                                    Map<String, String> treeStructMap) {

        // 树形结构处理
        boolean isTree = (treeStructMap != null && (tc + "").equals(treeStructMap.get("type")));
        String pidField = null;
        String treeCodeField = null;
        String orderField = null;
        String isLeafField = null;
        if (isTree) {
            pidField = (String) treeStructMap.get("pidField");
            treeCodeField = (String) treeStructMap.get("treeCodeField");
            orderField = (String) treeStructMap.get("orderField");
            isLeafField = (String) treeStructMap.get("isLeafField");
        }

        // 映射处理
        int fieldIndex = 0;
        int mappingSize = mapping.size();
        String[] columns = new String[mappingSize]; // 表字段
        ProjectField[] projectFields = new ProjectField[mappingSize]; // Project任务属性
        Map<String, ProjectField> destMap = new HashMap<String, ProjectField>(mapping.size());
        for (Map<String, Object> map : mapping) {
            String fieldCode = (String) map.get("fieldCode");
            String sourceType = (String) map.get("sourceType");
            String sourceValue = null;
            if (mapping_sourcetype_property.equals(sourceType)) {
                sourceValue = (String) map.get("sourceValue");
                ProjectField projectField = ProjectField.getInstance(sourceValue);
                if (projectField == null) {
                    throw new ConfigException("映射关系中Project属性【" + sourceValue + "】暂未支持，请检查！");
                }
                destMap.put(fieldCode, projectField);
                projectFields[fieldIndex] = projectField;
            }

            columns[fieldIndex] = fieldCode;
            fieldIndex++;
        }

        // 校验处理
        ProjectField[] checkFields = null;
        Map<String, IDataObject> existedDataMap = null;
        String checkSql = null;
        Map<String, List<Object>> paramMap = null;
        Collection<ProjectData> projectDataList = projectDataMap.values();
        int checkItemSize = 0;
        if (!isEmpty(checkItems)) {
            checkItemSize = checkItems.size();
            checkFields = new ProjectField[checkItemSize];
            int checkItemIndex = 0;
            for (String field : checkItems) {
                ProjectField projectField = destMap.get(field);
                checkFields[checkItemIndex] = projectField;
                checkItemIndex++;
            }

            // 实体中存在的数据
            List<IDataObject> datas = dataview.select();
            if (datas.size() > 0) {
                existedDataMap = new HashMap(datas.size());
                for (IDataObject data : datas) {
                    String value = "";
                    for (String checkField : checkItems) {
                        Object valObj = data.get(checkField);
                        if (valObj == null) {
                            value = value + ";null";
                        } else {
                            value = value + ";" + valObj.toString();
                        }
                    }
                    existedDataMap.put(value, data);
                }
            }
        }

        // 构造树
        Integer[] rootIds = projectDataMap.get(IProjectService.ROOT_ID).getChildrenID();
        int orderNo = 1;
        for (Integer rootId : rootIds) {
            this.putTreeToDV(dataview, projectDataMap, rootId, checkFields, projectFields, columns, existedDataMap, isTree, pidField, treeCodeField, orderField, isLeafField, "", "", orderNo);
            orderNo++;
        }
    }

    /**
     * 将Project数据存到数据中
     *
     * @param projectDataMap
     * @param tableName
     * @param mapping
     * @param repeatOperation
     * @param checkItems
     * @param treeStructMap
     */
    private void covertDataSaveToDB(Map<Integer, ProjectData> projectDataMap, String tableName, List<Map<String, Object>> mapping, String repeatOperation, List<String> checkItems,
                                    Map<String, String> treeStructMap) {

        // 树形结构处理
        boolean isTree = (treeStructMap != null && (tc + "").equals(treeStructMap.get("type")));
        String pidField = null;
        String treeCodeField = null;
        String orderField = null;
        String isLeafField = null;
        if (isTree) {
            pidField = (String) treeStructMap.get("pidField");
            treeCodeField = (String) treeStructMap.get("treeCodeField");
            orderField = (String) treeStructMap.get("orderField");
            isLeafField = (String) treeStructMap.get("isLeafField");
        }

        // 映射处理
        int fieldIndex = 0;
        int mappingSize = mapping.size();
        String[] columns = new String[mappingSize]; // 表字段
        ProjectField[] projectFields = new ProjectField[mappingSize]; // Project任务属性
        Map<String, ProjectField> destMap = new HashMap<String, ProjectField>(mapping.size());
        for (Map<String, Object> map : mapping) {
            String fieldCode = (String) map.get("fieldCode");
            String sourceType = (String) map.get("sourceType");
            String sourceValue = null;
            if (mapping_sourcetype_property.equals(sourceType)) {
                sourceValue = (String) map.get("sourceValue");
                ProjectField projectField = ProjectField.getInstance(sourceValue);
                if (projectField == null) {
                    throw new ConfigException("映射关系中Project属性【" + sourceValue + "】暂未支持，请检查！");
                }
                destMap.put(fieldCode, projectField);
                projectFields[fieldIndex] = projectField;
            }

            columns[fieldIndex] = fieldCode;
            fieldIndex++;
        }

        // 校验处理
        ProjectField[] checkFields = null;
        Map<String, IDataObject> existedDataMap = null;
        String checkSql = null;
        Map<String, Object> paramMap = null;
        Collection<ProjectData> projectDataList = projectDataMap.values();
        IDataView dataView = null;
        IDAS das = getVDS().getDas();
        int checkItemSize = 0;
        if (!isEmpty(checkItems)) {
            checkItemSize = checkItems.size();
            checkFields = new ProjectField[checkItemSize];
            if (checkItemSize == 1) {
                String field = checkItems.get(0);
                checkSql = "select * from " + tableName + " where " + field + " in (:" + field + ") ";
                List<Object> fieldValueList = new ArrayList<Object>();
                ProjectField projectField = destMap.get(field);
                checkFields[0] = projectField;
                for (ProjectData projectData : projectDataList) {
                    if (projectData.getID() != null) {
                        fieldValueList.add(projectData.getFieldValue(projectField));
                    }
                }
                paramMap = new HashMap<String, Object>();
                paramMap.put(field, fieldValueList);
                dataView = das.find(checkSql, paramMap);
                List<IDataObject> datas = dataView.select();
                if (datas.size() > 0) {
                    existedDataMap = new HashMap(datas.size());
                    for (IDataObject data : datas) {
                        String value = "";
                        for (String checkField : checkItems) {
                            Object valObj = data.get(checkField);
                            if (valObj == null) {
                                value = value + ";null";
                            } else {
                                value = value + ";" + valObj.toString();
                            }
                        }
                        existedDataMap.put(value, data);
                    }
                }
            } else if (checkItemSize > 1) {
                String idCheckSql = null;
                int checkItemIndex = 0;
                for (String field : checkItems) {
                    if (idCheckSql == null) {
                        idCheckSql = "select id from " + tableName + " where " + field + "= :" + field + " ";
                    } else {
                        idCheckSql = idCheckSql + " and " + field + "= :" + field;
                    }
                    ProjectField projectField = destMap.get(field);
                    checkFields[checkItemIndex] = projectField;
                    checkItemIndex++;
                }

                List<Object> idList = new ArrayList<Object>();
                Map<String, Object> idCheckParamMap = null;
                for (ProjectData projectData : projectDataList) {
                    if (projectData.getID() != null) {
                        idCheckParamMap = new HashMap<String, Object>(checkItemSize);
                        Object[] projectFieldVals = projectData.getFieldValues(checkFields);
                        for (int itemIndex = 0; itemIndex < checkItemSize; itemIndex++) {
                            String field = checkItems.get(itemIndex);
                            idCheckParamMap.put(field, projectFieldVals[itemIndex]);
                        }
                        IDataView idDataView = das.find(idCheckSql, idCheckParamMap);
                        List<IDataObject> idDataObjects = idDataView.select();
                        if (idDataObjects.size() > 0) {
                            idList.add((idDataObjects.get(0).get("id")));
                        }
                    }
                }
                if (idList.size() > 0) {
                    paramMap = new HashMap<String, Object>(checkItemSize);
                    paramMap.put("id", idList);
                    checkSql = "select * from " + tableName + " where id in (:id) ";
                    dataView = das.find(checkSql, paramMap);
                    List<IDataObject> datas = dataView.select();
                    if (datas.size() > 0) {
                        existedDataMap = new HashMap(datas.size());
                        for (IDataObject data : datas) {
                            String value = "";
                            for (String checkField : checkItems) {
                                Object valObj = data.get(checkField);
                                if (valObj == null) {
                                    value = value + ";null";
                                } else {
                                    value = value + ";" + valObj.toString();
                                }
                            }
                            existedDataMap.put(value, data);
                        }
                    }
                }
            }
        }

        if (dataView == null) {
            dataView = das.createDataViewByName(tableName);
        }

        // 构造树
        Integer[] rootIds = projectDataMap.get(IProjectService.ROOT_ID).getChildrenID();
        int orderNo = 1;
        for (Integer rootId : rootIds) {
            this.putTreeToDV(dataView, projectDataMap, rootId, checkFields, projectFields, columns, existedDataMap, isTree, pidField, treeCodeField, orderField, isLeafField, "", "", orderNo);
            orderNo++;
        }
        dataView.acceptChanges();
    }

    /**
     * 递归构造树
     *
     * @param dataView
     * @param projectDataMap
     * @param dataID
     * @param checkFields
     * @param projectFields
     * @param columns
     * @param existedDataMap
     * @param isTree
     * @param pidField
     * @param treeCodeField
     * @param orderField
     * @param isLeafField
     * @param parentId
     * @param parentCode
     * @param orderNo
     */
    private void putTreeToDV(IDataView dataView, Map<Integer, ProjectData> projectDataMap, Integer dataID, ProjectField[] checkFields, ProjectField[] projectFields, String[] columns,
                             Map<String, IDataObject> existedDataMap, boolean isTree, String pidField, String treeCodeField, String orderField, String isLeafField, String parentId, String parentCode, int orderNo) {
        ProjectData projectData = projectDataMap.get(dataID);
        if (projectData == null) {
            return;
        }

        IDataObject dataObject = null;
        if (checkFields != null && existedDataMap != null) {
            Object[] valueArr = projectData.getFieldValues(checkFields);
            String value = "";
            for (Object valObj : valueArr) {
                if (valObj == null) {
                    value = value + ";null";
                } else {
                    value = value + ";" + valObj.toString();
                }
            }
            if (existedDataMap.containsKey(value)) {
                dataObject = existedDataMap.get(value);
            }
        }

        if (dataObject == null) {
            dataObject = dataView.insertDataObject();
            dataObject.set("id", VdsUtils.uuid.generate());
        }

        Object[] data = projectData.getFieldValues(projectFields);
        for (int columnIndex = 0; columnIndex < columns.length; columnIndex++) {
            dataObject.set(columns[columnIndex], data[columnIndex]);
        }
        String id = dataObject.getId();

        boolean isleaf = false;
        Integer[] childIDs = projectData.getChildrenID();
        if (childIDs == null) {
            isleaf = true;
        }
        String innerCode = parentCode + new DecimalFormat(pattern).format(orderNo);

        if (isTree) {
            dataObject.set(pidField, parentId);
            dataObject.set(treeCodeField, innerCode);
            dataObject.set(orderField, orderNo);
            dataObject.set(isLeafField, isleaf);
        }

        if (!isleaf) {
            int childIndex = 1;
            for (Integer childID : childIDs) {
                this.putTreeToDV(dataView, projectDataMap, childID, checkFields, projectFields, columns, existedDataMap, isTree, pidField, treeCodeField, orderField, isLeafField, id, innerCode,
                        childIndex);
                childIndex++;
            }
        }
    }

    private boolean isEmpty(String str) {
        if (str == null || str.equals("")) {
            return true;
        }

        return false;
    }

    private boolean isEmpty(List<?> list) {
        if (list == null || list.isEmpty()) {
            return true;
        }

        return false;
    }

    private IVDS getVDS() {
        return VDS.getIntance();
    }

    private void quiteClose(Closeable c) {
        if (c != null) {
            try {
                c.close();
            } catch (Exception e) {
                // 忽略
            }
        }
    }
}
