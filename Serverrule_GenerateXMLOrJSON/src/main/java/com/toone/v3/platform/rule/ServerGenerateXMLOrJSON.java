package com.toone.v3.platform.rule;

import com.yindangu.v3.business.VDS;
import com.yindangu.v3.business.metadata.api.IDataView;
import com.yindangu.v3.business.plugin.business.api.rule.*;
import com.yindangu.v3.business.plugin.execptions.ConfigException;
import com.yindangu.v3.platform.plugin.util.VdsUtils;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 服务端规则：配置数据生成
 *
 * @Author xugang
 * @Date 2021/6/23 14:00
 */
public class ServerGenerateXMLOrJSON implements IRule {

    private static final String PARAM_DATA_TYPE = "ResultDataType";
    private static final String PARAM_DATAS = "ResultDataDetail";
    private static final String PARAM_ROOT_NAME = "RootName";
    private static final String PARAM_SAVE_TARGET_TYPE = "SaveTargetType";
    private static final String PARAM_SAVE_TARGET = "SaveTarget";

    private static final String DATA_TYPE_XML = "XML";
    private static final String DATA_TYPE_JSON = "JSON";

    private static final String CFG_ELEMENT_NAME = "elementName";
    private static final String CFG_ELEMENT_VALUE = "elementValue";
    private static final String CFG_ELEMENT_SPLICE_TYPE = "elementSpliceType";

    @Override
    public IRuleOutputVo evaluate(IRuleContext context) {
        String dataType = (String) context.getPlatformInput(PARAM_DATA_TYPE);
        String rootName = (String) context.getPlatformInput(PARAM_ROOT_NAME);
        String destName = (String) context.getPlatformInput(PARAM_SAVE_TARGET);
        String destType = (String) context.getPlatformInput(PARAM_SAVE_TARGET_TYPE);
        List resultDataDetail = (List<Map<String, Object>>) context.getPlatformInput(PARAM_DATAS);
        if (dataType == null || dataType.equals("")) {
            throw new ConfigException("[ServerGenerateXMLOrJSON.evaluate]生成数据格式未进行设置，请检查配置是否正确");
        }
        if (resultDataDetail == null || resultDataDetail.isEmpty()) {
            throw new ConfigException("[ServerGenerateXMLOrJSON.evaluate]生成数据内容未进行设置，请检查配置是否正确");
        }

        // 获取数据
        List<Map<String, Object>> datas = generateDatas(resultDataDetail, context);
        String generateStr = "";
        if (DATA_TYPE_XML.equals(dataType.toUpperCase())) {
            generateStr = generateXML(rootName, datas);
        } else if (DATA_TYPE_JSON.equals(dataType.toUpperCase())) {
            generateStr = generateJSON(datas);
        } else {
            throw new ConfigException("[ServerGenerateXMLOrJSON]当前只支持生成xml或者json格式的数据，不支持的数据类型:" + dataType);
        }

        // 设置规则输出值
        setRuleOutput(context, destName, destType, generateStr);

        IRuleOutputVo outputVo = context.newOutputVo();
        outputVo.put(generateStr);
        return outputVo;
    }

    private void setRuleOutput(IRuleContext context, String destName, String destType, String generateStr) {
        if (!(destName == null || destName.equals("")) && !(destType == null || destType.equals(""))) {
            IRuleVObject ruleVObject = context.getVObject();
            ContextVariableType targetEntityType = ContextVariableType.getInstanceType(destType);
            if (targetEntityType == null) {
                throw new ConfigException("[ServerGenerateXMLOrJSON]不支持的变量类型：" + destType);
            }
            ruleVObject.setContextObject(targetEntityType, destName, generateStr);
        }
    }

    /**
     * 生成XML或JSON需要的数据格式
     *
     * @param dataDetail
     * @param context
     * @return
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private List<Map<String, Object>> generateDatas(List<Map<String, Object>> dataDetail, IRuleContext context) {
        List<Map<String, Object>> datas = new ArrayList();
        for (int i = 0; i < dataDetail.size(); i++) {
            Map<String, Object> dataConfig = dataDetail.get(i);
            Object elementNameSrc = dataConfig.get("ElementNameSrc");
            Object elementValueSrcType = dataConfig.get("ElementValueSrcType");
            Object elementValue = dataConfig.get("ElementValue");
            Object scope = dataConfig.get("Scope");
            Object spliceType = dataConfig.get("SpliceType");

            Map<String, Object> data = new LinkedHashMap<String, Object>();
            //elementNameSrc 计算表达式的值
            String elementName = VDS.getIntance().getFormulaEngine().eval(elementNameSrc.toString());
            data.put("elementName", elementName);
            data.put("elementScope", scope);
            data.put("elementSpliceType", spliceType);
            //表达式
            if (elementValueSrcType.equals("0")) {
                Object elementValueStr = VDS.getIntance().getFormulaEngine().eval(elementValue.toString());
                data.put("elementValue", elementValueStr);
                //方法输入实体  方法输出实体  方法变量实体
            } else {
                List elementValues = getElementValueFromTableColumn(context, elementValueSrcType.toString(), elementValue.toString(), scope.toString());
                data.put("elementValue", elementValues);
            }
            datas.add(data);
        }
        return datas;
    }

    /**
     * 获取实体字段值
     *
     * @param context
     * @param sourceType
     * @param tableColumn
     * @param scope
     * @return
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private List getElementValueFromTableColumn(IRuleContext context, String sourceType, String tableColumn, String scope) {
        if (tableColumn.indexOf(".") < 0) {
            throw new ConfigException("[ServerGenerateXMLOrJSON.getElementValueFromTableColumn]来源实体字段" + tableColumn + "格式不正确，应为实体名.字段");
        }
        // 按照取值返回获取的元素值，可能是单值，也可能是列表(返回数组类型)
        List elementValue = new ArrayList();
        String[] arr = tableColumn.split("\\.");
        if (arr.length > 1) {
            String tableName = arr[0];
            String columnName = arr[1];
            //获取实体值
            IRuleVObject ruleVObject = context.getVObject();
            ContextVariableType targetEntityType = ContextVariableType.getInstanceType(sourceType);
            if (targetEntityType == null) {
                throw new ConfigException("[ServerGenerateXMLOrJSON]不支持的变量类型：" + sourceType);
            }
            IDataView dataView = (IDataView) ruleVObject.getContextObject(tableName, targetEntityType);

            List mapList = dataView.getDatas();
            //首行
            if (scope.equals("firstRow")) {
                if (mapList != null && !mapList.isEmpty()) {
                    elementValue.add(((Map<String, Object>) mapList.get(0)).get(columnName).toString());
                }
                //全部行
            } else if (scope.equals("2")) {
                if (mapList != null && !mapList.isEmpty()) {
                    for (Object map : mapList) {
                        if (map instanceof Map) {
                            Map recordMap = (Map) map;
                            Object value = recordMap.get(columnName);
                            if (value == null) {
                                value = "";
                                elementValue.add(null);
                            } else {
                                elementValue.add(value.toString());
                            }

                        }
                    }
                }
            } else {
                throw new ConfigException("[ServerGenerateXMLOrJSON.getElementValueFromTableColumn]元素值来源范围不正确" + scope);
            }
        }

        return elementValue;
    }

    enum VariableType {
        // 活动集输出变量
        RuleSetOutput("ruleSetOutput"),
        // 活动集输入变量
        RuleSetInput("ruleSetInput"),
        // 活动集上下文变量
        RuleSetVar("ruleSetVar");

        private String type;

        private VariableType(String type) {
            this.type = type;
        }

        public static VariableType getInstanceType(String key) {
            VariableType ret = null;
            for (VariableType type : VariableType.values()) {
                if (key.equals(type.type)) {
                    ret = type;
                }
            }
            return ret;
        }
    }

    /**
     * 生成xml字符串
     *
     * @param rootName
     * @param datas
     * @return
     */
    @SuppressWarnings("unchecked")
    private String generateXML(String rootName, List<Map<String, Object>> datas) {
        Document doc = DocumentHelper.createDocument();
        if (rootName == null || rootName.equals("")) {
            rootName = "root";
        }
        Element root = doc.addElement(rootName);
        // 循环配置，生成xml节点信息
        for (Map<String, Object> dataConfig : datas) {
            Object elementNameObj = dataConfig.get(CFG_ELEMENT_NAME);
            if (!(elementNameObj instanceof String)) {
                throw new RuntimeException("[GenerateXMLOrJSON.generateXML]元素名类型不正确，该数据类型不支持:" + elementNameObj.getClass().getName());
            }
            String elementName = (String) dataConfig.get(CFG_ELEMENT_NAME);
            Object elementValue = dataConfig.get(CFG_ELEMENT_VALUE);
            String spliceType = (String) dataConfig.get(CFG_ELEMENT_SPLICE_TYPE);

            // 拼接类型为1，标识为增加结构
            boolean isStructure = "1".equals(spliceType);

            if (elementValue instanceof List) {
                List<Object> values = (List<Object>) elementValue;
                Element listEle = root.addElement(elementName + "-list");
                for (Object elementValueStr : values) {
                    String subElementValue = null;
                    if (elementValueStr != null) {
                        if (elementValueStr instanceof Double || elementValueStr instanceof Integer || elementValueStr instanceof Boolean) {
                            subElementValue = elementValueStr.toString();
                        } else {
                            subElementValue = (String) elementValueStr;
                        }
                    }
                    if (isStructure) {
                        // 子结构XML添加
                        Document subDoc = null;
                        try {
                            subDoc = parse(subElementValue);
                        } catch (Exception e) {
                            throw new RuntimeException("[GenerateXMLOrJSON.generateXML]转换子结构XML发生错误:" + subElementValue);
                        }

                        Element subRootEle = subDoc.getRootElement();
                        String subRootName = subRootEle.getName();

                        if (elementName.equals(subRootName)) {
                            listEle.add(subRootEle);
                        } else {
                            listEle.addElement(elementName).add(subRootEle);
                        }
                        // ele.add(subRootEle);
                    } else {
                        // 元素值列表添加
                        Element tmp = listEle.addElement(elementName).addCDATA(subElementValue);
                        if (null == subElementValue) {
                            tmp.addAttribute("isNull", "true");
                        }
                    }
                }
            } else {// else if (elementValue instanceof String || elementValue == null) {
                // null与字符串相同处理方式
                String elementValueStr = null;
                if (elementValue != null) {
                    if (elementValue instanceof Double || elementValue instanceof Integer || elementValue instanceof Boolean) {
                        elementValueStr = elementValue.toString();
                    } else {
                        elementValueStr = (String) elementValue;
                    }
                }
                if (isStructure) {
                    // 子结构XML添加
                    Document subDoc = null;
                    try {
                        subDoc = parse(elementValueStr);
                    } catch (Exception e) {
                        throw new RuntimeException("[GenerateXMLOrJSON.generateXML]生成节点【" + elementName + "】子结构XML发生错误:" + elementValueStr);
                    }

                    Element subRootEle = subDoc.getRootElement();
                    String subRootName = subRootEle.getName();
                    // liangmf 2013-05-11
                    // 增加子结构时，如果子结构的根节点名称跟当前节点名称一致，就往当前的父节点上加，保证不会出现两层一样名字的情况
                    if (elementName.equals(subRootName)) {
                        root.add(subRootEle);
                    } else {
                        root.addElement(elementName).add(subRootEle);
                    }
                } else {
                    Element tmp = root.addElement(elementName).addCDATA(elementValueStr);
                    if (null == elementValueStr) {
                        tmp.addAttribute("isNull", "true");
                    }
                }
//			} else {
//				throw new RuntimeException("[GenerateXMLOrJSON.generateXML]元素值类型不正确，该数据类型不支持:" + elementValue.getClass().getName());
            }

        }
        return doc.asXML();
    }

    private Document parse(String xmldocumentstr) throws DocumentException {
        SAXReader reader = new SAXReader();
        reader.setEncoding("utf-8");
        StringReader str_reader = new StringReader(xmldocumentstr);
        Document document = reader.read(str_reader);
        str_reader.close();
        return document;
    }

    /**
     * 生成json字符串
     *
     * @param datas
     * @return
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private String generateJSON(List<Map<String, Object>> datas) {
        Map<String, Object> jsonObj = new LinkedHashMap<String, Object>();
        // 循环配置，生成json节点信息
        for (Map<String, Object> dataConfig : datas) {
            Object elementNameObj = dataConfig.get(CFG_ELEMENT_NAME);
            if (!(elementNameObj instanceof String)) {
                throw new RuntimeException("[GenerateXMLOrJSON.generateJSON]元素名类型不正确，该数据类型不支持:" + elementNameObj.getClass().getName());
            }
            String elementName = (String) dataConfig.get(CFG_ELEMENT_NAME);
            Object elementValue = dataConfig.get(CFG_ELEMENT_VALUE);
            String spliceType = (String) dataConfig.get(CFG_ELEMENT_SPLICE_TYPE);

            // 拼接类型为1，标识为增加结构
            boolean isStructure = "1".equals(spliceType);

            if (elementValue instanceof List) {
                List subJsonArray = new ArrayList();
                List<Object> values = (List<Object>) elementValue;
                if (isStructure) {
                    for (Object subJsonStr : values) {
                        Map<String, Object> subJson = null;
                        try {
                            subJson = VdsUtils.json.fromJson(subJsonStr.toString());
                        } catch (Exception e) {
                            throw new RuntimeException("[GenerateXMLOrJSON.generateJSON]转换子结构JSON发生错误:" + subJsonStr);
                        }
                        subJsonArray.add(subJson);
                    }
                    jsonObj.put(elementName, subJsonArray);
                } else {
                    for (Object value : values) {
                        subJsonArray.add(value);
                    }
                    jsonObj.put(elementName, subJsonArray);
                }
            } else {
                if (elementValue != null && !(elementValue instanceof String)) {
                    elementValue = elementValue.toString();
                }
                if (isStructure) {
                    String subJsonStr = (String) elementValue;
                    Map<String, Object> subJson = null;
                    List<Map<String, Object>> subJsonList = null;
                    // 处理2次，一个是按Map处理，一次是按List处理，那个可以正常解析就按那个执行
                    try {
                        subJson = VdsUtils.json.fromJson(subJsonStr);
                    } catch (Exception e) {

                    }

                    if (null == subJson) {
                        try {
                            subJsonList = VdsUtils.json.fromJsonList(subJsonStr);
                        } catch (Exception e) {

                        }
                    }

                    if (null == subJson && null == subJsonList) {
                        // 暂时不抛异常信息
                    }

                    // 如果subJson不为空，则直接使用
                    if (null != subJson) {
                        jsonObj.put(elementName, subJson);
                    }

                    if (null != subJsonList) {
                        jsonObj.put(elementName, subJsonList);
                    }
                } else {
                    jsonObj.put(elementName, elementValue);
                }
            }
        }
        return VdsUtils.json.toJson(jsonObj);
    }
}
