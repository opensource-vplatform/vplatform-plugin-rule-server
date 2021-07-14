package com.toone.v3.platform.rule;

import com.yindangu.v3.business.VDS;
import com.yindangu.v3.business.metadata.api.IDataObject;
import com.yindangu.v3.business.metadata.api.IDataView;
import com.yindangu.v3.business.plugin.business.api.rule.*;
import com.yindangu.v3.business.plugin.execptions.ConfigException;
import com.yindangu.v3.platform.plugin.util.VdsUtils;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 配置数据还原
 *
 * @Author xugang
 * @Date 2021/7/12 10:35
 */
public class ServerRestoreXMLOrJSON implements IRule {

    private static final String PARAM_DATAS = "RestoreDataDetail";
    private static final String DATA_TYPE_XML = "XML";
    private static final String DATA_TYPE_JSON = "JSON";
    private static final String RESTORE_DATA_TYPE = "RestoreDataType";
    private static final String RESTORE_DATA_SRC = "RestoreDataSrc";

    @Override
    public IRuleOutputVo evaluate(IRuleContext context) {
        String restoreDataType = (String) context.getPlatformInput(RESTORE_DATA_TYPE);
        String restoreDataSrc = (String) context.getPlatformInput(RESTORE_DATA_SRC);
        List<Map<String, Object>> dataDetail = (List<Map<String, Object>>) context.getPlatformInput(PARAM_DATAS);
        if (!DATA_TYPE_XML.equals(restoreDataType)&&!DATA_TYPE_JSON.equals(restoreDataType)) {
            throw new ConfigException("[ServerGenerateXMLOrJSON.evaluate]数据来源表达式值不为JSON和XML，无法进行配置数据还原");
        }
        if (isEmpty(restoreDataSrc)) {
            throw new ConfigException("[ServerGenerateXMLOrJSON.evaluate]规则json配置有误,还原数据来源未设置，无法进行配置数据还原");
        }
        if (VdsUtils.collection.isEmpty(dataDetail)) {
            throw new ConfigException("[ServerRestoreXMLOrJSON.evaluate]规则json配置有误,还原数据内容未指定，无法进行配置数据还原");
        }
        //计算表达式的值(配置数据内容值)
        String restoreDataValue = VDS.getIntance().getFormulaEngine().eval(restoreDataSrc.toString());
        if (isEmpty(restoreDataValue)) {
            throw new ConfigException("[ServerGenerateXMLOrJSON.evaluate]" + "执行规则配置的还原数据来源表达式值"+restoreDataSrc.toString()+",返回值不允许为空且必须为字符串");
        }

        List<String> elementNames = new ArrayList<String>();
        //元素名到字段名的映射Map
        Map<String, Object> elementNameToFieldNameMap = new HashMap<String, Object>();
        for (int i = 0; i < dataDetail.size(); i++) {
            Map<String, Object> dataConfig = dataDetail.get(i);
            Object elementNameSrc =  dataConfig.get("ElementNameSrc");
            Object elementValueDestField =  dataConfig.get("ElementValueDestField");
            Object elementValueSrcType =  dataConfig.get("ElementValueDestType");
            if (isEmpty(elementNameSrc.toString())) {
                throw new ConfigException("[ServerGenerateXMLOrJSON.evaluate]" + "规则json配置有误,还原数据内容中，元素名来源表达式串不允许为空且必须为字符串");
            }
            if (isEmpty(elementValueDestField.toString())) {
                throw new ConfigException("[ServerGenerateXMLOrJSON.evaluate]" + "规则json配置有误,还原数据内容中，元素值对应的实体字段未设置");
            }
            if(elementValueDestField.toString().indexOf(".") < 0){
                throw new ConfigException("[ServerGenerateXMLOrJSON.evaluate]" + "规则json配置有误,还原数据内容中，元素值对应的实体字段名格式有误,应为实体名.字段");
            }
            if (isEmpty(elementValueSrcType.toString())) {
                throw new ConfigException("[ServerGenerateXMLOrJSON.evaluate]" + "规则json配置有误,还原数据内容中，元素值对应的实体类型不允许为空");
            }
            String elementNameValue = VDS.getIntance().getFormulaEngine().eval(elementNameSrc.toString());
            if (isEmpty(elementNameValue)) {
                throw new ConfigException("[ServerGenerateXMLOrJSON.evaluate]" + "执行规则配置的元素名来源表达式:" + elementNameSrc.toString() + ",返回值不允许为空且必须为字符串");
            }
            elementNameToFieldNameMap.put(elementNameValue,elementValueDestField.toString());
            elementNameToFieldNameMap.put(elementNameValue+"Type", elementValueSrcType);
            elementNames.add(elementNameValue);
        }

        Map<String, Object> resultDataMap = new HashMap<String, Object>();
        if (DATA_TYPE_XML.equals(restoreDataType)) {
            resultDataMap = parseAndRestoreXmlData(restoreDataValue, elementNames);
        } else if (DATA_TYPE_JSON.equals(restoreDataType)) {
            resultDataMap = parseAndRestoreJsonData(restoreDataValue, elementNames);
        }
        //生成对应的实体字段
        generateEntityWithType(context, elementNameToFieldNameMap, elementNames,resultDataMap);

        IRuleOutputVo outputVo = context.newOutputVo();
        outputVo.put(resultDataMap);
        return outputVo;
    }

    /**
     * 解析并且收集生成json数据内容
     * @param restoreDataValue
     * @param elementNames
     * @return
     */
    private Map<String, Object> parseAndRestoreJsonData(
            String restoreDataValue,
            List<String> elementNames) {

        Map<String, Object> resultDataMap = new HashMap<String, Object>();
        if (VdsUtils.collection.isEmpty(elementNames)) {
            return resultDataMap;
        }

        Map<String, Object> jsonDataMap = null;
        try {
            jsonDataMap = VdsUtils.json.fromJson(restoreDataValue);
        } catch (Exception e) {
            throw new ConfigException("[RestoreXMLOrJSON.parseAndRestoreJsonData]" +"解析待还原JSON数据失败,json=" + restoreDataValue, e);
        }

        String currentElementName = null;
        Object currentElementJsonData = null;
        try {
            for (String elementName : elementNames) {
                if (!jsonDataMap.containsKey(elementName)) {
                    continue;
                }

                currentElementName = elementName;
                Object elementJsonData = jsonDataMap.get(elementName);
                currentElementJsonData = elementJsonData;
                if (elementJsonData == null) {
                    resultDataMap.put(elementName, null);
                } else if (elementJsonData instanceof List) {
                    @SuppressWarnings("rawtypes")
                    List elementJsonDataLst = (List) elementJsonData;
                    List<Object> elementJsonStrLst = new ArrayList<Object>();
                    for (Object elementJsonDataEle : elementJsonDataLst) {
                        if(null == elementJsonDataEle){
                            elementJsonStrLst.add(null);
                        }else{
                            String elementJsonStr = VdsUtils.json.toJson(elementJsonDataEle);
                            elementJsonStr = removeStartEndDoubleQuotes(elementJsonStr);
                            elementJsonStrLst.add(elementJsonStr);
                        }
                    }
                    resultDataMap.put(elementName, elementJsonStrLst);
                } else {
                    String elementJsonStr = VdsUtils.json.toJson(elementJsonData);
                    elementJsonStr = removeStartEndDoubleQuotes(elementJsonStr);
                    resultDataMap.put(elementName, elementJsonStr);
                }

            }
        } catch (Exception e) {
            throw new ConfigException("[RestoreXMLOrJSON.parseAndRestoreJsonData]" +
                    "当前元素名:" + currentElementName + "对应的元素值从json对象转换为字符串失败。" +
                    "json对象=" + currentElementJsonData, e);
        }

        return resultDataMap;
    }

    /**
     * 清除开始结尾的双引号
     * @param str
     * @return
     */
    private String removeStartEndDoubleQuotes(String str) {
        if (str == null || str.equals("")) {
            return str;
        }
        if (str.startsWith("\"")) {
            str = str.substring(1);
        }

        if (str.endsWith("\"")) {
            str = str.substring(0, str.length() - 1);
        }
        return str;
    }

    /**
     * 解析并且收集生成xml数据内容
     * @param restoreDataValue
     * @param elementNames
     * @return
     */
    private Map<String, Object> parseAndRestoreXmlData(
            String restoreDataValue,
            List<String> elementNames) {

        Map<String, Object> resultDataMap = new HashMap<String, Object>();
        if (VdsUtils.collection.isEmpty(elementNames)) {
            return resultDataMap;
        }
        Document doc = null;
        try {
            doc = DocumentHelper.parseText(restoreDataValue);
        } catch (DocumentException e) {
            throw new ConfigException("[RestoreXMLOrJSON.parseAndRestoreXmlData]" +
                    "解析待还原XML数据失败,xml=" + restoreDataValue, e);
        }

        Element root = doc.getRootElement();
        for (String elementName : elementNames) {
            Element elementLstEle = null;
            Element elementEle = null;
            if ((elementLstEle = root.element(elementName + "-list")) != null) {
                @SuppressWarnings("unchecked")
                List<Element> tmpElementEleLst = elementLstEle.elements(elementName);
                List<String> elementDataLst = new ArrayList<String>();
                for (Element tmpElementEle : tmpElementEleLst) {
                    String isNull = tmpElementEle.attributeValue("isNull");
                    if(null != isNull && "true".equals(isNull)){
                        elementDataLst.add(null);
                    }else{
                        String elementData = tmpElementEle.getTextTrim();
                        elementDataLst.add(elementData);
                    }
                }
                resultDataMap.put(elementName, elementDataLst);
            } else if ((elementEle = root.element(elementName)) != null) {
//				resultDataMap.put(elementName, elementEle.getTextTrim());
                String isNull = elementEle.attributeValue("isNull");
                if(null != isNull && "true".equals(isNull)){
                    resultDataMap.put(elementName, null);
                }else{
                    resultDataMap.put(elementName, elementEle.getTextTrim());
                }
            }
        }

        return resultDataMap;
    }


    /** 生成对应的实体字段
     * @param context
     * @param elementNameToFieldNameMap
     * @param elementNames
     * @param resultDataMap
     */
    @SuppressWarnings("unchecked")
    private void generateEntityWithType(IRuleContext context, Map<String, Object> elementNameToFieldNameMap, List<String> elementNames ,Map<String, Object> resultDataMap){
        //元素值对应的实体字段数组
        Map<String, Object> tableToFieldArr = new HashMap<String, Object>();
        for(int i = 0;i < elementNames.size(); i++){
            String fieldName = (String) elementNameToFieldNameMap.get(elementNames.get(i));
            String tableType = (String) elementNameToFieldNameMap.get(elementNames.get(i)+"Type");
            String table = fieldName.substring(0, fieldName.indexOf("."));
            String field = fieldName.substring(fieldName.indexOf(".") + 1);
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> tableArr = (List<Map<String, Object>>) tableToFieldArr.get(table);
            if(VdsUtils.collection.isEmpty(tableArr)){
                tableArr = new ArrayList<Map<String, Object>>();
            }
            Map<String, Object> eleArr = new HashMap<String, Object>();
            eleArr.put("field", field);
            eleArr.put("element", elementNames.get(i));
            tableArr.add(eleArr);
            tableToFieldArr.put(table, tableArr);
            tableToFieldArr.put(table+"Type", tableType);
        }

        // 生成对应的实体数据
        for (String table : tableToFieldArr.keySet()) {
            if(table.indexOf("Type") < 0){
                List<Map<String, Object>> fieldObjArr = (List<Map<String, Object>>) tableToFieldArr.get(table);
                int maxSize = 1;
                for(int i = 0;i < fieldObjArr.size(); i++){
                    Map<String, Object> fieldObj = fieldObjArr.get(i);
                    List ElementValueArr = new ArrayList();
                    Object ElementValue = resultDataMap.get(fieldObj.get("element"));
                    if(ElementValue instanceof List){
                        ElementValueArr = (List) ElementValue ;
                    }else if(ElementValue != null){
                        ElementValueArr.add(ElementValue);
                    }
                    int largestSize = 1 ;
                    if(!VdsUtils.collection.isEmpty(ElementValueArr)){
                        largestSize = ElementValueArr.size();
                    }
                    if( largestSize > maxSize){
                        maxSize = largestSize ;
                    }
                }
                String sourceType = (String) tableToFieldArr.get(table+"Type");
                //获取实体
                IDataView dataView = getDataViewWithType(context, table, sourceType);
                List<IDataObject> emptyRecords = new ArrayList<IDataObject>();
                for(int j = 0; j< maxSize;j++){
                    IDataObject dataObject = dataView.insertDataObject();
                    emptyRecords.add(dataObject);
                }
                for(int index = 0 ; index < fieldObjArr.size();index ++){
                    Map<String, Object> fieldObj = fieldObjArr.get(index);
                    String field = (String) fieldObj.get("field");
                    String element = (String) fieldObj.get("element");
                    Object elementValue = resultDataMap.get(element);
                    List elementValueArr = new ArrayList();
                    if(elementValue instanceof List){
                        elementValueArr = (List) elementValue ;
                    }else if(elementValue != null){
                        elementValueArr.add(elementValue);
                    }
                    if(!VdsUtils.collection.isEmpty(elementValueArr)){
                        for (int subIndex = 0; subIndex < maxSize; subIndex++) {
                            if (subIndex >= elementValueArr.size())
                                break;
                            String elementValue2 = (String) elementValueArr.get(subIndex);
                            emptyRecords.get(subIndex).set(field, elementValue2);
                        }
                    }

                }
                //赋值目标实体
                setDataViewWithType(context,table,sourceType,dataView);
            }
        }
    }

    /**
     * 取来源实体
     * @param context
     * @param sourceName
     * @param sourceType
     * @return
     */
    private IDataView getDataViewWithType(IRuleContext context, String sourceName, String sourceType) {
        IRuleVObject ruleObject = context.getVObject();
        ContextVariableType targetEntityType = ContextVariableType.getInstanceType(sourceType);
        if (targetEntityType == null) {
            throw new ConfigException("[ServerRestoreXMLOrJSON]不支持的变量类型：" + sourceType);
        }
        IDataView dataView = (IDataView) ruleObject.getContextObject(sourceName, targetEntityType);
        return dataView;
    }

    /**
     * 赋值目标实体
     * @param context
     * @param destName
     * @param destType
     * @param destDataView
     */
    private void setDataViewWithType(IRuleContext context, String destName, String destType, IDataView destDataView) {
//        if (!(destName == null || destName.equals("")) && !(destType == null || destType.equals(""))) {
            IRuleVObject ruleVObject = context.getVObject();
            ContextVariableType targetEntityType = ContextVariableType.getInstanceType(destType);
            if (targetEntityType == null) {
                throw new ConfigException("[ServerRestoreXMLOrJSON]不支持的变量类型：" + destType);
            }
            ruleVObject.setContextObject(targetEntityType, destName, destDataView);
//        }
    }

    private boolean isEmpty(String str) {
        if(str == null || str.equals("")) {
            return true;
        }

        return false;
    }
}
