package com.toone.v3.platform.rule;

import com.yindangu.v3.business.VDS;
import com.yindangu.v3.business.jdbc.api.model.ColumnType;
import com.yindangu.v3.business.jdbc.api.model.IColumn;
import com.yindangu.v3.business.jdbc.api.model.IDataSetMetaData;
import com.yindangu.v3.business.metadata.api.IDataObject;
import com.yindangu.v3.business.metadata.api.IDataView;
import com.yindangu.v3.business.plugin.business.api.rule.ContextVariableType;
import com.yindangu.v3.business.plugin.business.api.rule.IRule;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleContext;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleOutputVo;
import com.yindangu.v3.business.plugin.execptions.ConfigException;
import com.yindangu.v3.business.ruleset.api.context.IRuleSetRuntimeContext;
import com.yindangu.v3.business.ruleset.api.model.IRuleSet;
import com.yindangu.v3.business.ruleset.api.model.IRuleSetVariable;
import com.yindangu.v3.business.systemvariable.api.model.SystemVariableModel;
import com.yindangu.v3.business.vds.IVDS;
import com.yindangu.v3.business.ws.api.IWSServiceCall;
import com.yindangu.v3.platform.plugin.util.VdsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;

/**
 * 服务端规则：调用WebService
 *
 * @Author xugang
 * @Date 2021/7/14 16:37
 */
public class CallWebService implements IRule {

    private enum paramTypeEnum {
        CHAR, TEXT, NUMBER, BOOLEAN, DATE, LONGDATE, INTEGER, ENTITY
    }
    private enum ruleSetVariableTypeMetaEnum {
        varName, varType, varScope
    }
    private enum ruleSetVariableTypeEnum {
        OutputVariable, ContextVariable, InputVariable
    }
    private enum destTypeEnum {
        ruleSetOutput, ruleSetVariant, systemVariant
    }

    private static final Logger logger = LoggerFactory.getLogger(CallWebService.class);

    @Override
    public IRuleOutputVo evaluate(IRuleContext context) {
//        Map<String, Object> paramMap = (Map<String, Object>) context.getRuleConfig().getConfigParams();
        //	获取wbdl地址
        String wsdl = (String)context.getPlatformInput("webServiceSite");
        //	wsdl前期处理
        wsdl = dealWithWsdl(wsdl);
        //	获取输入参数
        @SuppressWarnings({ "rawtypes", "unchecked" })
        List<Map> inParams = (List<Map>) context.getPlatformInput("inputParams");
        //	拿到符合输入参数格式要求的参数集合
        @SuppressWarnings("rawtypes")
        List<Map> operParams = getOperParams(inParams, context);
        // 获取服务提供方
        String serviceProvider = (String) context.getPlatformInput("serviceProvider");
        //	获取方法编码
        String methodCode = (String) context.getPlatformInput("methodCode");
        //	目标类型
        String targetType = (String) context.getPlatformInput("targetType");
        //	目标
        String target = (String) context.getPlatformInput("target");
        //	获取输出参数
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> returnMappings = (List<Map<String, Object>>) context.getPlatformInput("returnMapping");

        //	这种情况下都不为文件格式
        Boolean isFile = false;

        IWSServiceCall wsServiceCall = getVDS().getWSServiceCall();
        Object jsonValue = null;
        try {
            jsonValue = wsServiceCall.invoke(isFile, wsdl, methodCode, operParams);
        } catch (Exception e) {
            throw new ConfigException("执行webService服务失败," + "原因:" + e.getMessage(), e);
        }

        if ("VWebService".equals(serviceProvider)) {
            //	V规范
            handleVOutputParams(context, returnMappings, (String)jsonValue);
        } else if ("OtherWebService".equals(serviceProvider)) {
            //	第三方
            handleOOutputParams(context, targetType, target, jsonValue);
        } else {
            throw new ConfigException("执行webService失败，该服务提供方为" + serviceProvider + "，目前只支持V规范和第三方");
        }

        IRuleOutputVo outputVo = context.newOutputVo();
        outputVo.put(null);
        return outputVo;
    }

    /**
     * 获取输入参数
     * @param inParams
     * @param context
     * @return
     */
    @SuppressWarnings("rawtypes")
    private List<Map> getOperParams(List<Map> inParams, IRuleContext context) {
        List<Map> operParams = new ArrayList<Map>();
        if (isEmpty(inParams)) {
            return operParams;
        }
        IRuleSetRuntimeContext runtimeContext = (IRuleSetRuntimeContext) context.getVObject().getContextObject(null, ContextVariableType.RuleChainRuntimeContext);
//        RuleSetRuntimeContext runtimeContext = RuleSetVariableUtil.getRuntimeContext(context);
        IRuleSet ruleSet = runtimeContext.getRuleSet();
        for (Map inParam : inParams) {
            String paramName = (String) inParam.get("paramName");
            String paramType = (String) inParam.get("paramType");
            String srcType = (String) inParam.get("srcType");
            Object paramValue = null;
            if (paramTypeEnum.ENTITY.name().equalsIgnoreCase(paramType)) {
                //	获取来源实体
                String sourceName = (String) inParam.get("src");
                IDataView sourceDataView = null;
                if ("ruleSetInput".equalsIgnoreCase(srcType)) {
                    ContextVariableType ruleSetInput = ContextVariableType.getInstanceType("ruleSetInput");
                    sourceDataView = (IDataView) context.getVObject().getContextObject(sourceName, ruleSetInput);
                } else if ("ruleSetVar".equalsIgnoreCase(srcType)) {
                    ContextVariableType ruleSetVar = ContextVariableType.getInstanceType("ruleSetVar");
                    sourceDataView = (IDataView) context.getVObject().getContextObject(sourceName, ruleSetVar);
                } else {
                    throw new ConfigException("未能对构件" + ruleSet.getComponentCode() + "的后台活动集" + ruleSet.getMetaCode() + "的实体变量" + paramName + "赋值，该实体变量来源类型" + srcType
                            + "不正确，目前只支持ruleSetInput或ruleSetVar，请检查活动集是否已经部署");
                }
                //	获取来源实体字段
                Map<String, IColumn> sourceColumnMap = getSourceColumnMap(sourceDataView);
                //	创建输入实体对应的dataView
                IDataView dataView = getVDS().getDas().createDataViewByMetadata();
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> fieldMappings = (List<Map<String, Object>>) inParam.get("paramFieldMapping");
                if (null != fieldMappings) {
                    // 初始化实体变量DataView的字段信息
                    boolean hasID = false;
                    for (Map<String, Object> fieldMapping : fieldMappings) {
                        String paramEntityField = (String) fieldMapping.get("paramEntityField");
                        String fieldValueType = (String) fieldMapping.get("fieldValueType");
                        String fieldValue = (String) fieldMapping.get("fieldValue");

                        //	字段来源为表达式或实体字段
                        if ("expression".equals(fieldValueType)) {
                            Object expression = getVDS().getFormulaEngine().eval(fieldValue);
                            ColumnType columnType = getColumnType(expression);
                            dataView.addColumn(paramEntityField, columnType);
                            if (!hasID && "id".equalsIgnoreCase(paramEntityField)) {
                                hasID = true;
                            }
                        } else if ("entityField".equals(fieldValueType) || "field".equals(fieldValueType)) {
                            IColumn vColumn = sourceColumnMap.get(fieldValue);
                            ColumnType columnType = vColumn.getColumnType();
                            int columnLength = vColumn.getLength();
                            int precision = -1;
                            if (columnType == ColumnType.Number) {
                                precision = vColumn.getPrecision();
                            }
                            if ((columnLength >= 0)
                                    && ((columnType == ColumnType.Text) || (columnType == ColumnType.LongText) || (columnType == ColumnType.Integer))) {
                                String columnChineseName = vColumn.getChineseName();
                                dataView.addColumn(paramEntityField, columnChineseName, columnType, precision, columnLength);
                            } else if ((!(columnLength <= 0 && precision <= 0)) && (columnType == ColumnType.Number)) {
                                String columnChineseName = vColumn.getChineseName();
                                dataView.addColumn(paramEntityField, columnChineseName, columnType, precision, columnLength);
                            } else {
                                dataView.addColumn(paramEntityField, columnType);
                            }
                            if (!hasID && "id".equalsIgnoreCase(paramEntityField)) {
                                hasID = true;
                            }
                        } else {
                            throw new ConfigException("未能识别构件" + ruleSet.getComponentCode() + "的后台活动集" + ruleSet.getMetaCode() + "的变量" + paramName + "的字段来源类型:" + fieldValueType
                                    + ",目前只允许expression或entityField类型");
                        }
                    }
                    if (!hasID) {
                        // 补充ID字段信息
                        dataView.addColumn("id", ColumnType.Text);
                    }
                }
                insertDataViewRecord(sourceDataView, dataView, inParam, ruleSet, paramName);
                paramValue = dataView.toJson();
            } else if ("expression".equalsIgnoreCase(srcType)) {
                String expression = (String) inParam.get("src");
                if (!isEmpty(expression)) {
                    paramValue = getVDS().getFormulaEngine().eval(expression);
                    //	若值为空，需要赋默认值
                    paramValue = setDefaultValue(paramValue, paramType);
                }
            } else {
                throw new ConfigException("未能识别构件" + ruleSet.getComponentCode() + "的后台活动集" + ruleSet.getMetaCode() + "的变量" + paramName + "的参数类型:" + paramType + ",目前只允许expression或entity类型");
            }

            Map <String, Object> operParamMap = new HashMap<String, Object>();
            operParamMap.put("paramName", paramName);
            operParamMap.put("paramType", paramType);
            operParamMap.put("paramValue", paramValue);
            operParams.add(operParamMap);
        }
        return operParams;
    }

    /**
     * 当输入参数为Boolean、Integer、Number类型，并且值为null时，赋初始值
     * @param paramValue
     * @param paramType
     * @return
     */
    private Object setDefaultValue(Object paramValue, String paramType) {
        if (null != paramValue) {
            return paramValue;
        } else {
            if (ColumnType.Boolean.name().equalsIgnoreCase(paramType)) {
                paramValue = false;
                return paramValue;
            }
            if (ColumnType.Integer.name().equalsIgnoreCase(paramType)) {
                paramValue = 0;
                return paramValue;
            }
            if (ColumnType.Number.name().equalsIgnoreCase(paramType)) {
                paramValue = 0.0;
                return paramValue;
            }
        }
        return paramValue;
    }

    /**
     * 获取表达式可能的类型
     * @param expression
     * @return
     */
    private ColumnType getColumnType(Object expression) {
        if (expression instanceof String) {
            return ColumnType.Text;
        }
        if (expression instanceof Integer) {
            return ColumnType.Integer;
        }
        if (expression instanceof Double) {
            return ColumnType.Number;
        }
        if (expression instanceof Boolean) {
            return ColumnType.Boolean;
        }
        return ColumnType.Text;
    }

    /**
     * 将来源实体的记录插入到输入实体中
     * @param sourceDataView
     * @param dataView
     * @param inParam
     * @param ruleSet
     * @param paramName
     */
    @SuppressWarnings("rawtypes")
    private void insertDataViewRecord(IDataView sourceDataView, IDataView dataView, Map inParam, IRuleSet ruleSet, String paramName) {
        if (isEmpty(inParam) || null == ruleSet) {
            return;
        }
        // 初始化实体变量DataView的数据信息
        if (null != sourceDataView && sourceDataView.size() > 0) {
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> fieldMappings = (List<Map<String, Object>>) inParam.get("paramFieldMapping");
            if (!isEmpty(fieldMappings)) {
                List<IDataObject> sourceRecords = sourceDataView.select();
                // 循环来源dataView,并对目标dataViwe生成记录
                for (IDataObject sourceRecord : sourceRecords) {
                    IDataObject record = dataView.insertDataObject();
                    // 循环字段配置信息
                    for (Map<String, Object> fieldMapping : fieldMappings) {
                        String source = (String) fieldMapping.get("fieldValue");
                        String sourceType = (String) fieldMapping.get("fieldValueType");
                        String targetField = (String) fieldMapping.get("paramEntityField");
                        Object targetValue = null;
                        // 字段来源为表达式或实体字段
                        if ("expression".equals(sourceType)) {
                            targetValue = getVDS().getFormulaEngine().eval(source);
                        } else if ("entityField".equals(sourceType) || "field".equals(sourceType)) {
                            targetValue = sourceRecord.get(source);
                        } else {
                            throw new ConfigException("未能识别构件" + ruleSet.getComponentCode() + "的后台活动集" + ruleSet.getMetaCode() + "的变量" + paramName + "的字段来源类型:" + sourceType
                                    + ",目前只允许expression或entityField类型");
                        }
                        record.set(targetField, targetValue);
                    }
                }
            }
        }
    }

    /**
     * 传进来的wsdl地址前后有双引号，在后面调用invoke会出错，在此去除前后双引号
     * @param wsdl
     * @return
     */
    private String dealWithWsdl(String wsdl) {
        if (isEmpty(wsdl)) {
            throw new ConfigException("解析wsdl异常，wsdl不能为空！");
        }
        try {
            wsdl = getVDS().getFormulaEngine().eval(wsdl);
        } catch(Exception e) {
            throw new ConfigException("解析表达式异常 ,wbdl地址 ：" +wsdl, e);
        }
        return wsdl;
    }

    /**
     * 处理V规范返回数据
     * @param context
     * @param returnMappings
     * @param jsonString
     */
    private void handleVOutputParams(IRuleContext context, List<Map<String, Object>> returnMappings, String jsonString) {
        if (null == context) {
            throw new ConfigException("执行webService的处理V规范出现异常，所传入的上下文对象不能为空！");
        }
        //	返回值为空，不处理，直接返回
        if (isEmpty(returnMappings)) {
            return;
        }
        //	webService输出所有数据字段名对应的类型
        Map<String, Object> srcColumnMaps = new HashMap<String, Object>();
        //	将要返回的数据用map封装
        Map<String, Object> resultMap = getResultMap(jsonString, srcColumnMaps);
        //	先拿到该规则所有数据,包含系统变量
        List<Map<String, String>> rulesetSystemVariables = getRuleSetSystemVariables(context);
        //	遍历每个输入变量
        for (Map<String, Object> returnMapping : returnMappings) {
            String targetType = (String) returnMapping.get("destType");
            String targetName = (String) returnMapping.get("dest");
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> fieldMappings = (List<Map<String, Object>>) returnMapping.get("destFieldMapping");
            boolean isDataViewSource = (null != fieldMappings && !isEmpty(fieldMappings));
            if (isDataViewSource) {
                //	获取当前活动集的实体变量
                IDataView dataView = null;
                if ("ruleSetVariant".equals(targetType)) {
                    ContextVariableType ruleSetVar = ContextVariableType.getInstanceType("ruleSetVar");
                    dataView = (IDataView) context.getVObject().getContextObject(targetName, ruleSetVar);
                } else if ("ruleSetOutput".equals(targetType)) {
                    ContextVariableType ruleSetOutput = ContextVariableType.getInstanceType("ruleSetOutput");
                    dataView = (IDataView) context.getVObject().getContextObject(targetName, ruleSetOutput);
                } else {
                    throw new ConfigException("目标实体变量赋值失败，返回目标类型" + targetType + "不正确，目前只支持类型ruleSetVariant及ruleSetOutput");
                }

                boolean isCleanTarget = toBooleanObj(returnMapping.get("isCleanDestEntityData"));
                if (isCleanTarget) {
                    //	清空dataView数据
                    List<IDataObject> dataObjects = dataView.select();
                    for (IDataObject dataObject : dataObjects) {
                        dataObject.remove();
                    }
                }

                //	获取规定返回的实体名
                String returnName = (String) returnMapping.get("return");
                //	若是map中无相关key，说明webService输出变量名与手动写进的变量名不一致,对该返回值不进行处理
                if (!resultMap.containsKey(returnName)) {
                    logger.warn("webService输出变量中没有名称为" + returnName + "变量,请检查手动写进的该变量名");
                    continue;
                }
                //	获取要返回的数据
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> returnValues = (List<Map<String, Object>>) resultMap.get(returnName);

                //	获取实体间字段的映射关系
                Map<String, String> destScrMap = new HashMap<String, String>();
                Map<String, Object> destValueMap = new HashMap<String, Object>();
                for (Map<String, Object> fieldMapping : fieldMappings) {
                    String destField = (String) fieldMapping.get("destField");
                    String srcValueType = (String) fieldMapping.get("srcValueType");
                    String srcValue = (String) fieldMapping.get("srcValue");
                    if ("field".equals(srcValueType)) {
//						srcValue = FormulaEngineFactory.getFormulaEngine().eval(srcValue);
                        destScrMap.put(destField, srcValue);
                    }
                    if ("expression".equals(srcValueType)) {
                        destValueMap.put(destField, srcValue);
                    }
                }
                //	该目标实体字段和类型对应映射
                Map<String, String> targetColumnMap = getTragetColumnMap(dataView);
                @SuppressWarnings("unchecked")
                Map<String, String> srcColumnMap = (Map<String, String>) srcColumnMaps.get(returnName);
                //	检验webService输出的实体字段和要输出的目标实体字段是否匹配
                checkEntityColumnType(srcColumnMap, targetColumnMap, destScrMap, returnName, targetName);

                //	调用的webService没有返回实体数据,直接返回，到此处才判断是为了先检查字段类型是否匹配
                if (isEmpty(returnValues)) {
                    continue;
                }

                //	获取更新方式
                String updateMode = (String) returnMapping.get("updateDestEntityMethod");
                List<Map<String, Object>> insertDataViewList = new ArrayList<Map<String,Object>>();
                Set<String> destKeys = destScrMap.keySet();
                Set<String> destValueKeys = destValueMap.keySet();
                //	更新记录的情况下，只取来源的第一条记录，赋值到目标的第一条记录上
                if ("updateRecord".equals(updateMode)) {
                    IDataObject targetRecordDataObject = null;
                    if (dataView.size() > 0) {
                        targetRecordDataObject = dataView.select().get(0);
                    } else {
                        targetRecordDataObject = dataView.insertDataObject();
                    }
                    Map<String, Object> firstReturnValueMap = returnValues.get(0);
                    //	字段处理
                    for (String destKey : destKeys) {
                        String srcValue = destScrMap.get(destKey);
                        Object destValue = firstReturnValueMap.get(srcValue);
                        targetRecordDataObject.set(destKey, destValue);
                    }
                    //	表达式处理
                    for (String destValueKey : destValueKeys) {
                        Object destValue = destValueMap.get(destValueKey);
                        String deestValueString = String.valueOf(destValue);
                        targetRecordDataObject.set(destValueKey, getVDS().getFormulaEngine().eval(deestValueString));
                    }

                } else {
                    for (Map<String, Object> returnValueMap : returnValues) {
                        Map<String, Object> dataViewMap = new HashMap<String, Object>();
                        //	字段处理
                        for (String destKey : destKeys) {
                            String srcValue = destScrMap.get(destKey);
                            Object destValue = returnValueMap.get(srcValue);
                            dataViewMap.put(destKey, destValue);
                        }
                        // 表达式处理
                        for (String destValueKey : destValueKeys) {
                            Object destValue = destValueMap.get(destValueKey);
                            String deestValueString = String.valueOf(destValue);
                            dataViewMap.put(destValueKey, getVDS().getFormulaEngine().eval(deestValueString));
                        }

                        //	倘若输出目标实体字段中没有id，需要为其添加
                        if (!dataViewMap.containsKey("id")) {
                            dataViewMap.put("id", returnValueMap.get("id"));
                        }
                        insertDataViewList.add(dataViewMap);
                    }
                    //	根据字段id去更新
                    List<IDataObject> dataObjects = dataView.select();
                    List<Map<String, Object>> updateDataViewList = new ArrayList<Map<String,Object>>();
                    for (IDataObject dataObject : dataObjects) {
                        String id = dataObject.getId();
                        for (Map<String, Object>  insertDataViewMap : insertDataViewList) {
                            Object insertId = insertDataViewMap.get("id");
                            if (id.equals(insertId)) {
                                Set<String> columns = insertDataViewMap.keySet();
                                for (String column : columns) {
                                    dataObject.set(column, insertDataViewMap.get(column));
                                }
                                updateDataViewList.add(insertDataViewMap);
                            }
                        }
                    }
                    insertDataViewList.removeAll(updateDataViewList);
                    dataView.insertDataObject(insertDataViewList);
                }
            } else {
                //	获取来源值
                String returnName = (String) returnMapping.get("return");
                String returnType = (String) returnMapping.get("returnType");

                //	类型判断
                checkVarColumnType(returnName, returnType, targetName, targetType, srcColumnMaps, rulesetSystemVariables);

                Object targetValue = null;
                if ("returnValue".equals(returnType)) {
                    targetValue = resultMap.get(returnName);
                } else if ("expression".equals(returnType)) {
                    //	从表达式获取
//					returnName = String.valueOf(FormulaEngineFactory.getFormulaEngine().eval(returnName));
//					targetValue = resultMap.get(returnName);
                    targetValue = getVDS().getFormulaEngine().eval(returnName);
                } else {
                    // TODO 暂不抛异常
                }
                // 设置目标变量
                if ("ruleSetVariant".equals(targetType)) {
                    ContextVariableType ruleSetVarType = ContextVariableType.getInstanceType("ruleSetVar");
                    context.getVObject().setContextObject(ruleSetVarType, targetName, targetValue);
                } else if ("ruleSetOutput".equals(targetType)) {
                    ContextVariableType ruleSetOutputType = ContextVariableType.getInstanceType("ruleSetOutput");
                    context.getVObject().setContextObject(ruleSetOutputType, targetName, targetValue);
                } else if ("systemVariant".equals(targetType)) {
                    ContextVariableType systemVariantType = ContextVariableType.getInstanceType("systemVariant");
                    context.getVObject().setContextObject(systemVariantType, targetName, targetValue);
                } else {
                    throw new ConfigException("未能识别构件的返回目标" + targetName + "的类型:" + targetType);
                }
            }

        }

    }

    /**
     * 校验webService输出的非实体类型和目标输出类型是否一致
     * @param returnName
     * @param returnType
     * @param targetName
     * @param targetType
     * @param srcColumnMaps
     * @param rulesetSystemVariables
     */
    private void checkVarColumnType(String returnName, String returnType, String targetName, String targetType,
                                    Map<String, Object> srcColumnMaps, List<Map<String, String>> rulesetSystemVariables) {
        //	此处要注意可能returnNmae = "null"
        if (null == returnName || "".equals(returnName) || isEmpty(returnType) || isEmpty(targetType)
                || isEmpty(targetName) || null == srcColumnMaps || null == rulesetSystemVariables) {
            throw new ConfigException("检验webService输出值与目标值类型是否匹配时出现异常，所传入的参数不能为空！");
        }
        //	若类型是表达式，需要解析,解析出来有可能不为String类型，需要转化换
        if ("expression".equals(returnType)) {
        	Object s = getVDS().getFormulaEngine().eval(returnName);
            returnName =  String.valueOf(s);
        }
        //	判断webService返回值与手动输入的变量是否一致，不一致直接返回
        if (!srcColumnMaps.containsKey(returnName)) {
            logger.warn("webService输出变量中没有名称为" + returnName + "变量,请检查手动写进的该变量名");
            return;
        }
        //	判断webService返回值类型与目标输出值类型是否匹配
        String varType = "";
        for (Map<String, String> rulesetSystemVariableMap : rulesetSystemVariables) {
            String varName = rulesetSystemVariableMap.get(ruleSetVariableTypeMetaEnum.varName.name());
            String varScope = rulesetSystemVariableMap.get(ruleSetVariableTypeMetaEnum.varScope.name());
            if (varName.equals(targetName) && varScope.equals(targetType)) {
                varType = rulesetSystemVariableMap.get(ruleSetVariableTypeMetaEnum.varType.name());
                break;
            }
        }
        Object object = srcColumnMaps.get(returnName);
        if (object instanceof Map) {
            throw new ConfigException("执行webService异常，webService服务返回参数" + returnName + "为entity类型，"
                    + "而目标输出参数" + targetName + "的类型为" + varType + "，两者类型匹配不上");
        } else {
            String srcType = (String) object;
            if (!varType.equals(srcType)) {
                throw new ConfigException("执行webService异常，webService服务返回参数" + returnName + "为" + srcType + "类型，"
                        + "而目标输出参数" + targetName + "的类型为" + varType + "，两者类型匹配不上");
            }
        }
    }

    /**
     * 校验webService输出的实体字段和目标实体字段类型是否一致
     * @param srcColumnMap
     * @param targetColumnMap
     * @param destScrMap
     * @param returnName
     * @param targetName
     */
    private void checkEntityColumnType(Map<String, String> srcColumnMap,
                                       Map<String, String> targetColumnMap, Map<String, String> destScrMap, String returnName, String targetName) {
        if (isEmpty(srcColumnMap) || isEmpty(targetColumnMap)) {
            throw new ConfigException("检验webService输出值与目标值类型是否匹配时出现异常，"
                    + "所传入的参数不能为空！");
        }
        //	目标实体字段与来源实体字段无映射关系，此时应该是来源字段都为表达式，无需比较字段差异
        if (isEmpty(destScrMap)) {
            return;
        }
        //	要输出的目标实体字段名集合
        Set<String> targetKeys = destScrMap.keySet();
        Map<String, String> errorTypeMap = new HashMap<String, String>();
        for (String targetKey : targetKeys) {
            //	来源实体字段名集合
            String srcKey = destScrMap.get(targetKey);
            if (!srcColumnMap.containsKey(srcKey)) {
                logger.warn("webService输出变量中变量名为" + returnName + "的实体中没有字段名为" + srcKey + "的变量,请检查手动写进的变量名");
                continue;
            }
            //	webService输出实体的字段变量类型
            String srcType = srcColumnMap.get(srcKey);
            //	要输出实体的字段的变量的类型
            String tarType = targetColumnMap.get(targetKey);
            if (!srcType.equals(tarType)) {
                errorTypeMap.put(targetKey, srcKey);
            }
        }
        if (!isEmpty(errorTypeMap)) {
            Set<String> errorTargetKeys = errorTypeMap.keySet();
            StringBuffer buffer =  new StringBuffer("输出的目标实体" + targetName + "的字段类型与webService返回的实体字段" + returnName + "类型匹配不上：");
            for (String errorTargetKey : errorTargetKeys) {
                String errorSrcKey = errorTypeMap.get(errorTargetKey);
                buffer.append("webService返回字段" + errorSrcKey + "类型为" + srcColumnMap.get(errorSrcKey)
                        + ",而输出字段" + errorTargetKey + "类型为" + targetColumnMap.get(errorTargetKey) + " ");
            }
            throw new ConfigException(buffer.toString());
        }
    }

    /**
     * 拿到该方法的有关的所有数据,包含系统变量
     * @param context
     * @return
     */
    private List<Map<String, String>> getRuleSetSystemVariables(IRuleContext context) {
        if (null == context) {
            throw new ConfigException("获取规则所有参数变量异常，所出传入的上下文对象不能为空！");
        }
        List<Map<String, String>> RuleSetSystemVariables = new ArrayList<Map<String,String>>();
        IRuleSetRuntimeContext runtimeContext = (IRuleSetRuntimeContext) context.getVObject().getContextObject(null, ContextVariableType.RuleChainRuntimeContext);
//        RuleSetRuntimeContext runtimeContext = RuleSetVariableUtil.getRuntimeContext(context);
        IRuleSet ruleSet = runtimeContext.getRuleSet();
        List<IRuleSetVariable> ruleSetVariables = ruleSet.getRuleSetVariables();
        for (IRuleSetVariable ruleSetVariable : ruleSetVariables) {
            String varName = ruleSetVariable.getCode();
            String varType = ruleSetVariable.getType().getCode();
            String scopeType = ruleSetVariable.getScopeType().name();
            if (ruleSetVariableTypeEnum.OutputVariable.name().equals(scopeType)) {
                Map<String, String> ruleSetVariableMap = new HashMap<String, String>();
                ruleSetVariableMap.put(ruleSetVariableTypeMetaEnum.varName.name(), varName);
                ruleSetVariableMap.put(ruleSetVariableTypeMetaEnum.varType.name(), varType);
                ruleSetVariableMap.put(ruleSetVariableTypeMetaEnum.varScope.name(), destTypeEnum.ruleSetOutput.name());
                RuleSetSystemVariables.add(ruleSetVariableMap);
                continue;
            }
            if (ruleSetVariableTypeEnum.ContextVariable.name().equals(scopeType)) {
                Map<String, String> ruleSetVariableMap = new HashMap<String, String>();
                ruleSetVariableMap.put(ruleSetVariableTypeMetaEnum.varName.name(), varName);
                ruleSetVariableMap.put(ruleSetVariableTypeMetaEnum.varType.name(), varType);
                ruleSetVariableMap.put(ruleSetVariableTypeMetaEnum.varScope.name(), destTypeEnum.ruleSetVariant.name());
                RuleSetSystemVariables.add(ruleSetVariableMap);
                continue;
            }
        }
        List<SystemVariableModel> systemVariables = getVDS().getSystemVariableManager().getSystemVariables();
        for (SystemVariableModel systemVariable : systemVariables) {
            Map<String, String> systemVariableMap = new HashMap<String, String>();
            String varName = systemVariable.getCode();
            String varType = systemVariable.getType().toString();
            systemVariableMap.put(ruleSetVariableTypeMetaEnum.varName.name(), varName);
            systemVariableMap.put(ruleSetVariableTypeMetaEnum.varType.name(), varType);
            systemVariableMap.put(ruleSetVariableTypeMetaEnum.varScope.name(), destTypeEnum.systemVariant.name());
            RuleSetSystemVariables.add(systemVariableMap);
        }
        return RuleSetSystemVariables;
    }

    /**
     * 处理第三方返回数据
     * @param context
     * @param targetType
     * @param targetName
     * @param jsonValue
     */
    private void handleOOutputParams(IRuleContext context, String targetType, String targetName, Object jsonValue) {
        if (isEmpty(targetName) || isEmpty(targetType)) {
            throw new ConfigException("处理返回数据异常，调用第三方webService时目标类型targetType和目标target不能为空！");
        }
        if (null == jsonValue) {
            return;
        }
        jsonValue = VdsUtils.json.toJson(jsonValue);
        // 设置目标变量
        if ("ruleSetVariant".equals(targetType)) {
            ContextVariableType ruleSetVarType = ContextVariableType.getInstanceType("ruleSetVar");
            context.getVObject().setContextObject(ruleSetVarType, targetName, jsonValue);
        } else if ("ruleSetOutput".equals(targetType)) {
            ContextVariableType ruleSetOutputType = ContextVariableType.getInstanceType("ruleSetOutput");
            context.getVObject().setContextObject(ruleSetOutputType, targetName, jsonValue);
        } else if ("systemVariant".equals(targetType)) {
            ContextVariableType systemVariantType = ContextVariableType.getInstanceType("systemVariant");
            context.getVObject().setContextObject(systemVariantType, targetName, jsonValue);
        } else {
            throw new ConfigException("未能识别构件的返回目标" + targetName + "的类型:" + targetType);
        }
    }

    /**
     * 解析json数据，返回执行webSerivce返回的结果的键值映射关系
     *
     * @param jsonString
     * @param srcColumnMap
     * @return
     */
    private Map<String, Object> getResultMap(String jsonString, Map<String, Object> srcColumnMap) {
        Map<String, Object> resultMap = new HashMap<String, Object>();

        //	直接返回。不处理,这种情况有可能是webService服务输入输出都为空
        if (isEmpty(jsonString)) {
            return resultMap;
        }
        //	解析json数据
        Map<String, Object> invokes = VdsUtils.json.fromJson(jsonString);
        //	获取处理后的要返回的数据
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> ruleSetResults = (List<Map<String, Object>>) invokes.get("ruleSetResult");
        //	webService输出值为空，直接返回，不处理
        if (null == ruleSetResults) {
            return resultMap;
        }
        for (Map<String, Object> ruleSetResultMap : ruleSetResults) {
            String itemType = (String) ruleSetResultMap.get("itemType");
            String itemCode = (String) ruleSetResultMap.get("itemCode");
            if (paramTypeEnum.ENTITY.name().equalsIgnoreCase(itemType)) {
                String itemValueJson = (String) ruleSetResultMap.get("itemValue");
                Map<Object, Object> itemValueMap = VdsUtils.json.fromJson(itemValueJson);
                @SuppressWarnings("unchecked")
                Map<String, Object> dataMap = (Map<String, Object>) itemValueMap.get("datas");
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> values = (List<Map<String, Object>>) dataMap.get("values");
                resultMap.put(itemCode, values);

                //	获取webService输出的实体的字段的名称和类型
                @SuppressWarnings("unchecked")
                Map<String, Object> metadataMap = (Map<String, Object>) itemValueMap.get("metadata");
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> models = (List<Map<String, Object>>) metadataMap.get("model");
                Map<String, Object> model = (Map<String, Object>) models.get(0);
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> fields = (List<Map<String, Object>>) model.get("fields");
                Map<String, String> fieldColumnNameTypeMap = new HashMap<String, String>();
                for (Map<String, Object> fieldMap : fields) {
                    String columnName = (String) fieldMap.get("code");
                    String columnType = (String) fieldMap.get("type");
                    fieldColumnNameTypeMap.put(columnName, columnType);
                }
                srcColumnMap.put(itemCode, fieldColumnNameTypeMap);
            } else {
                Object itemValue = ruleSetResultMap.get("itemValue");
                resultMap.put(itemCode, itemValue);
                srcColumnMap.put(itemCode, itemType);
            }
        }
        return resultMap;
    }

    /** H2状态字段 */
    private static String	H2_STATE_FIELD	= "H_2_S_T_A_T_E";
    /** H2保存旧id的字段 */
    private static String	H2_ORGID_FIELD	= "H_2_O_R_G_I_D";
    /** H2保存来源SQL的字段,来源SQL的hashcode值 */
    private static String	H2_SOURCE_FIELD	= "H_2_S_O_U_R_C_E";
    /** H2保存来源pk的字段,来源pk的hashcode值 */
    private static String	H2_PK_FIELD		= "H_2_P_K";
    /** 租户字段 */
    private static String H2_TenantID_Field="V3_TenantID";

    private Collection<IColumn> getColumns(IDataSetMetaData datasetMetaData) {
    	//封装了新接口
    	 Collection<IColumn> columns = datasetMetaData.getColumns();
         return columns;
        /*try { 
            Set<String> columnNames = datasetMetaData.getColumnNames();
            List<IColumn> columns = new ArrayList<>();
            for(String columnName : columnNames) {
                if(!columnName.equalsIgnoreCase(H2_STATE_FIELD)
                        && !columnName.equalsIgnoreCase(H2_ORGID_FIELD)
                        && !columnName.equalsIgnoreCase(H2_SOURCE_FIELD)
                        && !columnName.equalsIgnoreCase(H2_PK_FIELD)
                        && !columnName.equalsIgnoreCase(H2_TenantID_Field)) {

                    columns.add(datasetMetaData.getColumn(columnName));
                }
            }
        	
        } catch(SQLException e) {
            throw new RuntimeException("获取字段失败", e);
        }*/
       
    }

    /**
     * 拿到目标实体的字段
     * @param targetDataView
     * @return
     */
    private Map<String, String> getTragetColumnMap(IDataView targetDataView) {
        if (null == targetDataView) {
            throw new ConfigException("获取实体字段和类型异常，所传入的dataView参数为空");
        }
        IDataSetMetaData datasetMetaData = ((IDataSetMetaData)targetDataView.getMetadata());
        Collection<IColumn> columns = getColumns(datasetMetaData);
        Map<String, String> tragetColumnMap = new HashMap<String, String>();
        for (IColumn vColumn : columns) {
            String columnName = vColumn.getColumnName();
            String columnType = vColumn.getColumnType().toString();
            tragetColumnMap.put(columnName, columnType);
        }
        return tragetColumnMap;
    }

    /**
     * 拿到来源实体的字段
     * @param sourceDataView
     * @return
     */
    private Map<String, IColumn> getSourceColumnMap(IDataView sourceDataView) {
        if (null == sourceDataView) {
            throw new ConfigException("获取实体字段和类型异常，所传入的dataView参数为空");
        }
        IDataSetMetaData datasetMetaData = ((IDataSetMetaData)sourceDataView.getMetadata());
        Collection<IColumn> columns = getColumns(datasetMetaData);
        Map<String, IColumn> tragetColumnMap = new HashMap<String, IColumn>();
        for (IColumn vColumn : columns) {
            String columnName = vColumn.getColumnName();
            tragetColumnMap.put(columnName, vColumn);
        }
        return tragetColumnMap;
    }

    private Boolean toBooleanObj(Object obj) {
        if (obj == null) {
            return false;
        } else if (obj instanceof Boolean) {
            return (Boolean) obj;
        } else if (obj instanceof Number) {// 所有的数字型全部这样判断
            return ((Number) obj).intValue() > 0 ? true : false;
        } else if (obj instanceof String) {
            return Boolean.parseBoolean(obj.toString());
        } else {
            throw new RuntimeException("转换Boolean类型错误！目前只支持Boolean,Number,String类型");
        }
    }

    private boolean isEmpty(Map<?, ?> map) {
        if(map == null || map.isEmpty()) {
            return true;
        }

        return false;
    }

    private boolean isEmpty(Collection<?> collection) {
        if(collection == null || collection.isEmpty()) {
            return true;
        }

        return false;
    }

    private boolean isEmpty(String str) {
        if(str == null || str.equals("")) {
            return true;
        }

        return false;
    }

    private IVDS getVDS() {
        return VDS.getIntance();
    }
}
