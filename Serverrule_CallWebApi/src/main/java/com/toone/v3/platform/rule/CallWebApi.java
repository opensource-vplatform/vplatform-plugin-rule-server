package com.toone.v3.platform.rule;

import com.yindangu.v3.business.VDS;
import com.yindangu.v3.business.http.model.HttpRequest;
import com.yindangu.v3.business.http.model.HttpRequestResult;
import com.yindangu.v3.business.metadata.api.IDataObject;
import com.yindangu.v3.business.metadata.api.IDataView;
import com.yindangu.v3.business.plugin.business.api.rule.*;
import com.yindangu.v3.business.plugin.execptions.ConfigException;
import com.yindangu.v3.business.vds.IVDS;
import com.yindangu.v3.platform.plugin.util.VdsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.util.*;

/**
 * 后台规则：调用WebAPI
 *
 * @Author xugang
 * @Date 2021/7/12 15:29
 */
public class CallWebApi implements IRule {

    // 用于标识参数是实体类型，否则还原时会因为没有类型信息导致解析不准确
    private static final String Mark_Entity = "@EntityVariable";
    private static final String tenantKey = "tenantKeyTooneV3";
    private static final Logger log = LoggerFactory.getLogger(CallWebApi.class);

    @Override
    public IRuleOutputVo evaluate(IRuleContext context) {
//        Map<String, Object> ruleCfgParams = (Map<String, Object>) context.getRuleConfig().getConfigParams();
        String webAPISiteExp = context.getPlatformInput("webAPISite").toString();
        List<Map<String, Object>> inputParams = (List<Map<String, Object>>) context.getPlatformInput("inputParams");
        List<Map<String, Object>> returnMappings = (List<Map<String, Object>>) context.getPlatformInput("returnMapping");
        // 服务提供方[V规范V平台WebAPI(WebAPI)/第三方API(API)]
        String serviceProvider = (String) context.getPlatformInput("serviceProvider");
        // *请求方式[POST(POST)/GET(GET)],当服务提供方为"V规范V平台WebAPI"时此处为空
        String requestType = (String) context.getPlatformInput("requestType");
        // *Header设置,当服务提供方为"V规范V平台WebAPI"时此处为空
        List<Map<String, Object>> headerParams = (List<Map<String, Object>>) context.getPlatformInput("headerParams");
        //taoyz,租户模式下，增加传递租户标识
        String tenantCode = "";
        String tenantCodeExpStr = "";
        //优先取规则配置项传入的值
        Object tenantCodeExp = context.getPlatformInput("tenantCode");
        if (tenantCodeExp != null) {
            tenantCodeExpStr = tenantCodeExp.toString();
        }
        if (tenantCodeExpStr != null && !"".equals(tenantCodeExpStr)) {
            tenantCode = getVDS().getFormulaEngine().eval(tenantCodeExpStr);
        }
        //规则配置项没有传入值，就判断当前系统是否为租户模式，若是就取当前租户编码
        if (tenantCode == null || "".equals(tenantCode)) {
            if (getVDS().getTenantService().isTenantModel()) {
                tenantCode = getVDS().getTenantService().getTenantCode();
            }
        }
        if (tenantCode != null && !"".equals(tenantCode)) {
            if (headerParams == null) {
                headerParams = new ArrayList<Map<String, Object>>();
            }
            //加密
            try {
                tenantCode = encryptAES(tenantCode, tenantKey);
            } catch (Exception e) {
                log.warn("tenantCode加密出错", e);
            }
            Map<String, Object> tenantHeaderParam = new HashMap<String, Object>();
            tenantHeaderParam.put("paramCode", "tenantCode");
            tenantHeaderParam.put("paramType", "");
            tenantHeaderParam.put("paramValue", tenantCode);
            headerParams.add(tenantHeaderParam);
        }

        // *入参设置[手动输入],当服务提供方为"V规范V平台WebAPI"或者为"第三方API"且请求方式为"GET"时此处为空
        String paramText = (String) context.getPlatformInput("paramText");
        // 当服务提供方为"V规范V平台WebAPI"时,"invokeTargetType/invokeTarget/respondTargetType/respondTarget"这些参数为空
        // *调用结果存储的目标类型[方法输出(ruleSetOutput)/方法变量(ruleSetVariant)/构件变量(systemVariant)]
        String invokeTargetType = (String) context.getPlatformInput("invokeTargetType");
        // *调用结果存储的目标,只允许选择"布尔"类型的变量
        String invokeTarget = (String) context.getPlatformInput("invokeTarget");
        // *响应结果存储的目标类型[方法输出(ruleSetOutput)/方法变量(ruleSetVariant)/构件变量(systemVariant)]
        String respondTargetType = (String) context.getPlatformInput("respondTargetType");
        // *响应结果存储的目标,只允许选择"文本/长文本"类型的变量
        String respondTarget = (String) context.getPlatformInput("respondTarget");
        // *传参方式[1(Key/Value)/2(字符串入参)],当服务提供方为"V规范V平台WebAPI"时或"第三方API"且请求方式为"GET"时此处为空
        String transParamType = (String) context.getPlatformInput("transParamType");
        String isAsyn = (String) context.getPlatformInput("isParallelism");
        //请求超时
        int timeout = Integer.parseInt(isEmpty((String) context.getPlatformInput("timeOut")) ? "3" : (String) context.getPlatformInput("timeOut"));
        boolean isGet = !isEmpty(requestType) && requestType.toUpperCase().equals("GET");
        if (isEmpty(serviceProvider)) {
            serviceProvider = "WebAPI";
            isGet = false;
        }

        String webAPISite = getVDS().getFormulaEngine().eval(webAPISiteExp);
        //是否异步
        if (!isEmpty(isAsyn) && isAsyn.equals("True")) {
            final String webAPISitef = webAPISite;
            final IRuleContext contextf = context;
            final List<Map<String, Object>> inputParamsf = inputParams;
            final boolean isGetf = isGet;
            final String paramTextf = paramText;
            final List<Map<String, Object>> headerParamsf = headerParams;
            final String invokeTargetTypef = invokeTargetType;
            final String invokeTargetf = invokeTarget;
            final String respondTargetTypef = respondTargetType;
            final String respondTargetf = respondTarget;
            final String serviceProviderf = serviceProvider;
            final String transParamTypef = transParamType;
            final List<Map<String, Object>> returnMappingsf = returnMappings;
            Map<String, Object> invokeParams = initRuleSetInputParams(inputParamsf, context);
            final String serialInputParamsString = serializeInputParams(invokeParams);
            final int timeoutf = timeout;
//            ThreadExecuteLogic excuteLogic = new ThreadExecuteLogic() {
//                @Override
//                public void execute() {
//                    Map<Object, Object> returnData = requestHttpAsyn(webAPISitef, contextf, inputParamsf, serialInputParamsString, isGetf, paramTextf, headerParamsf, invokeTargetTypef, invokeTargetf, respondTargetTypef, respondTargetf, serviceProviderf, transParamTypef, timeoutf);
//                    if (returnMappingsf != null && returnData != null && serviceProviderf.equals("WebAPI")) {
//                        setReturnValue(returnMappingsf, contextf, returnData);
//                    }
//                }
//            };
//            ThreadExecutor.runThread("CallWebApi_Async_Execute_Thread_" + VdsUtils.uuid.generate(), excuteLogic);
            // 平台线程池尚未封装，这里直接new线程执行
            new Thread(new Runnable() {
                @Override
                public void run() {
                    Map<Object, Object> returnData = requestHttpAsyn(webAPISitef, contextf, inputParamsf, serialInputParamsString, isGetf, paramTextf, headerParamsf, invokeTargetTypef, invokeTargetf, respondTargetTypef, respondTargetf, serviceProviderf, transParamTypef, timeoutf);
                    if (returnMappingsf != null && returnData != null && serviceProviderf.equals("WebAPI")) {
                        setReturnValue(returnMappingsf, contextf, returnData);
                    }
                }
            }, "CallWebApi_Async_Execute_Thread_" + VdsUtils.uuid.generate()).start();
        } else {
            //请求webapi 地址，返回数据
            Map<Object, Object> returnData = requestHttp(webAPISite, context, inputParams, isGet, paramText, headerParams, invokeTargetType, invokeTarget, respondTargetType, respondTarget, serviceProvider, transParamType, timeout);
            //把返回数据赋值到变量
            if (returnMappings != null && returnData != null && serviceProvider.equals("WebAPI")) {
                setReturnValue(returnMappings, context, returnData);
            }
        }

        IRuleOutputVo outputVo = context.newOutputVo();
        outputVo.put(null);
        return outputVo;
    }

    public void setReturnValue(List<Map<String, Object>> returnMappings, IRuleContext context, Map<Object, Object> returnData) {

        for (Map<String, Object> returnMapping : returnMappings) {
            String targetType = (String) returnMapping.get("destType");
            String targetName = (String) returnMapping.get("dest");

            List<Map<String, Object>> fieldMappings = (List<Map<String, Object>>) returnMapping.get("destFieldMapping");
            boolean isDataViewSource = (null != fieldMappings && !(fieldMappings == null || fieldMappings.isEmpty()));

            // 是否DataView的输出
            if (isDataViewSource) {
                ContextVariableType targetEntityType = null;
                if ("ruleSetVariant".equals(targetType)) {
                    targetEntityType = ContextVariableType.getInstanceType("ruleSetVar");
                } else if ("ruleSetOutput".equals(targetType)) {
                    targetEntityType = ContextVariableType.getInstanceType("ruleSetOutput");
                } else if ("systemVariant".equals(targetType)) {
                    targetEntityType = ContextVariableType.getInstanceType("systemVariant");
                } else {
                    throw new ConfigException("执行活动集目标实体变量赋值失败，返回目标类型" + targetType + "不正确，目前只支持类型ruleSetVariant及ruleSetOutput");
                }
                // 获取当前活动集的实体变量
                IDataView dataView = (IDataView) context.getVObject().getContextObject(targetName, targetEntityType);

                boolean isCleanTarget = toBooleanObj(returnMapping.get("isCleanDestEntityData"));
                if (isCleanTarget && dataView != null) {
                    // 清空dataView数据
                    dataView.removeAll();
                }

                List<Map<String, Object>> sourceRecords = (List<Map<String, Object>>) returnData.get((String) returnMapping.get("return"));
                String updateMode = (String) returnMapping.get("updateDestEntityMethod");
                if ("updateRecord".equals(updateMode)) {
                    // 更新记录的情况下，只取来源的第一条记录，赋值到目标的第一条记录上
                    // 目标DataView没有记录则创建一条
                    Map<String, Object> sourceRecord = sourceRecords.get(0);
                    IDataObject targetRecord = null;
                    if (dataView.size() > 0) {
                        targetRecord = dataView.select().get(0);
                    } else {
                        targetRecord = dataView.insertDataObject();
                    }

                    // 循环字段配置信息，对目标记录赋值
                    for (Map<String, Object> fieldMapping : fieldMappings) {
                        String source = (String) fieldMapping.get("srcValue");
                        String sourceType = (String) fieldMapping.get("srcValueType");
                        String targetField = (String) fieldMapping.get("destField");
                        Object targetValue = null;
                        if ("expression".equals(sourceType)) {
                            targetValue = getVDS().getFormulaEngine().eval(source);
                        } else {
                            targetValue = sourceRecord.get(source);
                        }

                        targetRecord.set(targetField, targetValue);
                    }
                } else {
                    // 缓存表达式的值，只取一次
//					Map<String, Object> targetExpMapping = null;
                    // 默认都按照insertOrUpdateBySameId执行
                    Map<String, Map<String, Object>> id2Record = new LinkedHashMap<String, Map<String, Object>>();
                    List<Map<String, Object>> noIdsRecords = new ArrayList<Map<String, Object>>();
                    if (sourceRecords != null) {
                        for (Map<String, Object> sourceRecord : sourceRecords) {
                            if (isEmpty((String) sourceRecord.get("id"))) {
                                noIdsRecords.add(sourceRecord);
                            } else {
                                id2Record.put((String) sourceRecord.get("id"), sourceRecord);
                            }
                        }
                    }
                    // 修改记录处理
                    Set<String> ids = id2Record.keySet();
                    Map<String, Object> idParams = new HashMap<String, Object>();
                    idParams.put("id", ids);
                    List<IDataObject> updateRecords = dataView.select(" id in (:id) ", idParams);
                    for (IDataObject targetRecord : updateRecords) {
                        String id = targetRecord.getId();
                        Map<String, Object> sourceRecord = id2Record.get(id);
                        // 循环字段配置信息，对目标记录赋值
                        for (Map<String, Object> fieldMapping : fieldMappings) {
                            String source = (String) fieldMapping.get("srcValue");
                            String sourceType = (String) fieldMapping.get("srcValueType");
                            String targetField = (String) fieldMapping.get("destField");
                            Object targetValue = null;
                            if ("expression".equals(sourceType)) {
                                targetValue = getVDS().getFormulaEngine().eval(source);
                            } else {
                                targetValue = sourceRecord.get(source);
                            }
                            targetRecord.set(targetField, targetValue);
                        }
                        ids.remove(id);
                    }

                    // 新增记录处理
                    List<Map<String, Object>> datas = new ArrayList<Map<String, Object>>();
                    for (String insertId : ids) {
                        Map<String, Object> sourceRecord = id2Record.get(insertId);
                        Map<String, Object> data = new HashMap<String, Object>();
                        // 循环字段配置信息，对目标记录赋值
                        for (Map<String, Object> fieldMapping : fieldMappings) {
                            String source = (String) fieldMapping.get("srcValue");
                            String sourceType = (String) fieldMapping.get("srcValueType");
                            String targetField = (String) fieldMapping.get("destField");
                            Object targetValue = null;
                            if ("expression".equals(sourceType)) {
                                targetValue = getVDS().getFormulaEngine().eval(source);
                            } else {
                                targetValue = sourceRecord.get(source);
                            }
                            data.put(targetField, targetValue);
                        }
                        datas.add(data);
                    }
                    for (Map<String, Object> sourceRecord : noIdsRecords) {
                        Map<String, Object> data = new HashMap<String, Object>();
                        // 循环字段配置信息，对目标记录赋值
                        for (Map<String, Object> fieldMapping : fieldMappings) {
                            String source = (String) fieldMapping.get("srcValue");
                            String sourceType = (String) fieldMapping.get("srcValueType");
                            String targetField = (String) fieldMapping.get("destField");
                            Object targetValue = null;
                            if ("expression".equals(sourceType)) {
                                targetValue = getVDS().getFormulaEngine().eval(source);
                            } else {
                                targetValue = sourceRecord.get(source);
                            }
                            data.put(targetField, targetValue);
                        }
                        datas.add(data);
                    }
                    dataView.insertDataObject(datas);
                }
            } else {
                // 获取来源值
                String sourceType = (String) returnMapping.get("returnType");
                String returnValue = (String) returnMapping.get("return");
                Object targetValue = null;
                if ("returnValue".equals(sourceType)) {
                    //2016-12-06 liangzc：若调用的方法没有对应的方法输出，则报错。此处为其他类型的变量

                    // 从活动集返回值获取
                    targetValue = returnData.get(returnValue);
                } else if ("expression".equals(sourceType)) {
                    // 从表达式获取
                    targetValue = getVDS().getFormulaEngine().eval(returnValue);
                } else {
                    // TODO 暂不抛异常
                }

                // 枚举字符串不一致的，先进行转换
                ContextVariableType targetEntityType = null;
                if ("ruleSetVariant".equals(targetType)) {
                    targetEntityType = ContextVariableType.getInstanceType("ruleSetVar");
                } else if ("ruleSetOutput".equals(targetType)) {
                    targetEntityType = ContextVariableType.getInstanceType("ruleSetOutput");
                } else if ("systemVariant".equals(targetType)) {
                    targetEntityType = ContextVariableType.getInstanceType("systemVariant");
                }

                // 设置目标变量
                if (targetEntityType != null) {
                    context.getVObject().setContextObject(targetEntityType, targetName, targetValue);
                }
            }
        }
    }

    public Map<Object, Object> requestHttp(String webAPISite, IRuleContext context, List<Map<String, Object>> inputParams,
                                           boolean isGet, String paramText, List<Map<String, Object>> headerParams, String invokeTargetType,
                                           String invokeTarget, String respondTargetType, String respondTarget, String serviceProvider, String transParamType, int timeout) {
        if (isEmpty(webAPISite)) {
            return null;
        }
        // 创建参数队列
        Map<String, Object> paramsMap = new HashMap<String, Object>();
        //2019-07-02 taoyz 增加传递参数为null值的处理
        String nullParamNames = "";
        if (inputParams != null) {
            for (int i = 0; i < inputParams.size(); i++) {
                Map<String, Object> map = inputParams.get(i);
                String key = (String) map.get("paramName");
                String src = (String) map.get("src");
                String srcType = (String) map.get("srcType");
                //      		ruleSetVar
                if (!map.get("paramType").equals("entity")) {
                    Object value = getVDS().getFormulaEngine().eval(src);
                    if (value == null) {
                        value = "";
                        nullParamNames = nullParamNames + key + ",";
                    }
                    paramsMap.put(key, value.toString());
                } else {
                    IDataView dv = null;
                    if ("ruleSetVar".equals(srcType)) {
                        dv = (IDataView) context.getVObject().getContextObject(src, ContextVariableType.getInstanceType(srcType));
                    } else if ("ruleSetInput".equals(srcType)) {
                        dv = (IDataView) context.getVObject().getContextObject(src, ContextVariableType.getInstanceType(srcType));
                    }
                    if (dv != null) {
                        List<Map<String, Object>> newdatas = new ArrayList<Map<String, Object>>();
                        List<Map<String, Object>> datas = dv.getDatas();
                        List<Map<String, Object>> paramFieldMapping = (List<Map<String, Object>>) map.get("paramFieldMapping");

                        for (int k = 0; k < datas.size(); k++) {
                            Map<String, Object> data = datas.get(k);
                            Map<String, Object> newdata = new HashMap<String, Object>();

                            for (int j = 0; j < paramFieldMapping.size(); j++) {
                                Map<String, Object> field = paramFieldMapping.get(j);
                                Object fieldValue = field.get("fieldValue");
                                String fieldValueType = (String) field.get("fieldValueType");
                                String paramEntityField = (String) field.get("paramEntityField");
                                if ("expression".equals(fieldValueType)) {
                                    fieldValue = getVDS().getFormulaEngine().eval((String) fieldValue);
                                    newdata.put(paramEntityField, fieldValue);
                                } else {
                                    newdata.put(paramEntityField, data.get(fieldValue));
                                }
                            }
                            newdatas.add(newdata);

                        }

                        String value = VdsUtils.json.toJson(newdatas);
                        paramsMap.put(key, value);
                    }
                }
            }
        }
        if (!isEmpty(nullParamNames)) {
            //2019-07-02 taoyz 增加传递参数为null值的处理
            String nullParamKey = "_null_params_";
            if (nullParamNames.endsWith(",")) {
                //去掉最后一个逗号
                nullParamNames = nullParamNames.substring(0, nullParamNames.length() - 1);
            }
            paramsMap.put(nullParamKey, nullParamNames);
        }

        HttpRequest httpRequest = getVDS().getWebApiInvoker().newHttpRequest(webAPISite);
//        HttpRequest httpRequest = new HttpRequest(webAPISite);
        httpRequest.setMethod(isGet ? HttpRequest.HttpMethod.GET : HttpRequest.HttpMethod.POST);
        if (!isGet) {
            if (serviceProvider.equals("API")) {
                if ("string".equals(transParamType)) {
                    paramText = getVDS().getFormulaEngine().eval(paramText);
                    httpRequest.setBody(paramText);
                }
                httpRequest.setTransmissionMode(transParamType);
            }
        }

        // 设置header
        Map<String, Object> headerMap = new HashMap<String, Object>();
        if (headerParams != null && headerParams.size() > 0) {
            for (int i = 0; i < headerParams.size(); i++) {
                Map<String, Object> header = headerParams.get(i);
                String paramCode = (String) header.get("paramCode");
                String paramType = (String) header.get("paramType");
                String paramValue = (String) header.get("paramValue");
                if ("expression".equals(paramType)) {
                    paramValue = getVDS().getFormulaEngine().eval(paramValue);
                }
                headerMap.put(paramCode, paramValue);
            }
        }
        httpRequest.setTimeout(timeout);
        httpRequest.setHeaders(headerMap);
        httpRequest.setParams(paramsMap);

        boolean targetValue = false;
        HttpRequestResult result;
        if (serviceProvider.equals("API")) {
            result = getVDS().getWebApiInvoker().callThirdHttpRequest(httpRequest);
        } else {
            result = getVDS().getWebApiInvoker().callWebApi(httpRequest);
        }

        String responseValue = "";
        try {
            if (result.exceptionCaught() == null) {
                // 请求成功
                targetValue = true;
                log.info("请求状态码 200");
                log.info("请求返回值 " + result.getResult());
                if (serviceProvider.equals("API")) {
                    //第三方api
                    Object responseContent = result.getResult().get("responseContent");
                    responseValue = responseContent == null ? "" : responseContent.toString();
                } else {
                    //平台的webapi
                    //2020-11-07 taoyz 增加对result.getResult()里面的内容解释，因为调用平台的webapi出错时，返回也是200，data里面有causedby的话就是出错的webapi，出错时就抛出异常
                    Map<Object, Object> resultMap = result.getResult();
                    Map<Object, Object> causedbyMap = (Map<Object, Object>) resultMap.get("causedby");
                    if (causedbyMap != null) {
                        String errorCode = (String) causedbyMap.get("errorCode");
                        if (errorCode != null && !"".equals(errorCode)) {
                            String errorMsg = (String) causedbyMap.get("errorMsg");
                            if (errorMsg == null || "".equals(errorMsg)) {
                                errorMsg = (String) resultMap.get("detail");
                            }
                            throw new Exception(errorMsg);
                        }
                    }
                    return resultMap;
                }
            } else {
                log.info("请求状态码 " + result.exceptionCaught().getRespCode());
                // throw new Exception("请求CallWebApi异常", result.exceptionCaught());
                throw new ConfigException("请求WebApi异常", result.exceptionCaught());
            }

            if (!isEmpty(invokeTarget)) {
                ContextVariableType invokeTargetTypeEnum = null;
                if ("ruleSetVariant".equals(invokeTargetType)) {
                    invokeTargetTypeEnum = ContextVariableType.getInstanceType("ruleSetVar");
                } else if ("ruleSetOutput".equals(invokeTargetType)) {
                    invokeTargetTypeEnum = ContextVariableType.getInstanceType("ruleSetOutput");
                } else if ("systemVariant".equals(invokeTargetType)) {
                    invokeTargetTypeEnum = ContextVariableType.getInstanceType("systemVariant");
                }
                IRuleVObject ruleVObject = context.getVObject();
                if (invokeTargetTypeEnum != null) {
                    ruleVObject.setContextObject(invokeTargetTypeEnum, invokeTarget, targetValue);
                }
            }

            if (!isEmpty(respondTargetType)) {

                ContextVariableType respondTargetTypeEnum = null;
                if ("ruleSetVariant".equals(respondTargetType)) {
                    respondTargetTypeEnum = ContextVariableType.getInstanceType("ruleSetVar");
                } else if ("ruleSetOutput".equals(respondTargetType)) {
                    respondTargetTypeEnum = ContextVariableType.getInstanceType("ruleSetOutput");
                } else if ("systemVariant".equals(respondTargetType)) {
                    respondTargetTypeEnum = ContextVariableType.getInstanceType("systemVariant");
                }
                if (respondTargetTypeEnum != null) {
                    context.getVObject().setContextObject(respondTargetTypeEnum, respondTarget, responseValue);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("请求WebApi异常", e);
        }

        return null;
    }


    private Map<String, Object> initRuleSetInputParams(List<Map<String, Object>> invokeParams, IRuleContext context) {
        Map<String, Object> webApiInputParams = new HashMap<String, Object>();
        if (invokeParams != null) {
            for (int i = 0; i < invokeParams.size(); i++) {
                Map<String, Object> map = invokeParams.get(i);
                String key = (String) map.get("paramName");
                String src = (String) map.get("src");
                String srcType = (String) map.get("srcType");
                //      		ruleSetVar
                if (!map.get("paramType").equals("entity")) {
                    Object value = getVDS().getFormulaEngine().eval(src);
                    if (value == null) value = "";
                    webApiInputParams.put(key, value);
                } else {
                    IDataView dv = null;
                    if ("ruleSetVar".equals(srcType)) {
                        ContextVariableType ruleSetVar = ContextVariableType.getInstanceType("ruleSetVar");
                        dv = (IDataView) context.getVObject().getContextObject(src, ruleSetVar);
                    } else if ("ruleSetInput".equals(srcType)) {
                        ContextVariableType ruleSetInput = ContextVariableType.getInstanceType("ruleSetInput");
                        dv = (IDataView) context.getVObject().getContextObject(src, ruleSetInput);
                    }
                    webApiInputParams.put(key, dv);
                }
            }
        }

        return webApiInputParams;

    }

    public Map<Object, Object> requestHttpAsyn(String webAPISite, IRuleContext context, List<Map<String, Object>> inputParams,
                                               String serialInputParamsString, boolean isGet, String paramText, List<Map<String, Object>> headerParams, String invokeTargetType,
                                               String invokeTarget, String respondTargetType, String respondTarget, String serviceProvider, String transParamType, int timeout) {

        if (isEmpty(webAPISite)) {
            return null;
        }

        Map<String, Object> copyedRuntimeParams = null;
        if (!isEmpty(serialInputParamsString)) {
            copyedRuntimeParams = unserializeInputParams(serialInputParamsString);
        }
        // 创建参数队列
        Map<String, Object> paramsMap = new HashMap<String, Object>();
        if (inputParams != null) {
            for (int i = 0; i < inputParams.size(); i++) {
                Map<String, Object> map = inputParams.get(i);
                String key = (String) map.get("paramName");
                if (!map.get("paramType").equals("entity")) {
                    paramsMap.put(key, copyedRuntimeParams.get(key).toString());
                } else {
                    IDataView dv = null;
                    dv = (IDataView) copyedRuntimeParams.get(key);
                    if (dv != null) {
                        List<Map<String, Object>> newdatas = new ArrayList<Map<String, Object>>();
                        List<Map<String, Object>> datas = dv.getDatas();
                        List<Map<String, Object>> paramFieldMapping = (List<Map<String, Object>>) map.get("paramFieldMapping");

                        for (int k = 0; k < datas.size(); k++) {
                            Map<String, Object> data = datas.get(k);
                            Map<String, Object> newdata = new HashMap<String, Object>();

                            for (int j = 0; j < paramFieldMapping.size(); j++) {
                                Map<String, Object> field = paramFieldMapping.get(j);
                                Object fieldValue = field.get("fieldValue");
                                String fieldValueType = (String) field.get("fieldValueType");
                                String paramEntityField = (String) field.get("paramEntityField");
                                if ("expression".equals(fieldValueType)) {
                                    fieldValue = getVDS().getFormulaEngine().eval((String) fieldValue);
                                    newdata.put(paramEntityField, fieldValue);
                                } else {
                                    newdata.put(paramEntityField, data.get(fieldValue));
                                }
                            }
                            newdatas.add(newdata);
                        }

                        String value = VdsUtils.json.toJson(newdatas);
                        // formparams1.add(new BasicNameValuePair(key, value));
                        paramsMap.put(key, value);
                    }
                }
            }
        }

        HttpRequest httpRequest = getVDS().getWebApiInvoker().newHttpRequest(webAPISite);
        httpRequest.setMethod(isGet ? HttpRequest.HttpMethod.GET : HttpRequest.HttpMethod.POST);
        if (!isGet) {
            if (serviceProvider.equals("API")) {
                if ("string".equals(transParamType)) {
                    paramText = getVDS().getFormulaEngine().eval(paramText);
                    httpRequest.setBody(paramText);
                }
                httpRequest.setTransmissionMode(transParamType);
            }
        }

        // 设置header
        Map<String, Object> headerMap = new HashMap<String, Object>();
        if (headerParams != null && headerParams.size() > 0) {
            for (int i = 0; i < headerParams.size(); i++) {
                Map<String, Object> header = headerParams.get(i);
                String paramCode = (String) header.get("paramCode");
                String paramType = (String) header.get("paramType");
                String paramValue = (String) header.get("paramValue");
                if ("expression".equals(paramType)) {
                    paramValue = getVDS().getFormulaEngine().eval(paramValue);
                }
                headerMap.put(paramCode, paramValue);
            }
        }
        httpRequest.setTimeout(timeout);
        httpRequest.setHeaders(headerMap);
        httpRequest.setParams(paramsMap);

        boolean targetValue = false;
        HttpRequestResult result;
        if (serviceProvider.equals("API")) {
            result = getVDS().getWebApiInvoker().callThirdHttpRequest(httpRequest);
        } else {
            result = getVDS().getWebApiInvoker().callWebApi(httpRequest);
        }

        String responseValue = "";
        try {
            if (result.exceptionCaught() == null) {
                // 请求成功
                targetValue = true;
                log.info("请求状态码 200");
                log.info("请求返回值 " + result.getResult());
                if (serviceProvider.equals("API")) {
                    Object responseContent = result.getResult().get("responseContent");
                    responseValue = responseContent == null ? "" : responseContent.toString();
                } else {
                    return result.getResult();
                }
            } else {
                log.warn("请求状态码 " + result.exceptionCaught().getRespCode());
                throw new ConfigException("请求WebApi异常", result.exceptionCaught());
            }

            if (!isEmpty(invokeTarget)) {
                if ("ruleSetVariant".equals(invokeTargetType)) {
                    ContextVariableType ruleSetVar = ContextVariableType.getInstanceType("ruleSetVar");
                    context.getVObject().setContextObject(ruleSetVar, invokeTarget, targetValue);
                } else if ("ruleSetOutput".equals(invokeTargetType)) {
                    ContextVariableType ruleSetVar = ContextVariableType.getInstanceType("ruleSetOutput");
                    context.getVObject().setContextObject(ruleSetVar, invokeTarget, targetValue);
                } else if ("systemVariant".equals(invokeTargetType)) {
                    ContextVariableType ruleSetVar = ContextVariableType.getInstanceType("systemVariant");
                    context.getVObject().setContextObject(ruleSetVar, invokeTarget, targetValue);
                }
            }
            if (!isEmpty(respondTargetType)) {
                if ("ruleSetVariant".equals(respondTargetType)) {
                    ContextVariableType ruleSetVar = ContextVariableType.getInstanceType("ruleSetVar");
                    context.getVObject().setContextObject(ruleSetVar, respondTarget, responseValue);
                } else if ("ruleSetOutput".equals(respondTargetType)) {
                    ContextVariableType ruleSetVar = ContextVariableType.getInstanceType("ruleSetOutput");
                    context.getVObject().setContextObject(ruleSetVar, respondTarget, responseValue);
                } else if ("systemVariant".equals(respondTargetType)) {
                    ContextVariableType ruleSetVar = ContextVariableType.getInstanceType("systemVariant");
                    context.getVObject().setContextObject(ruleSetVar, respondTarget, responseValue);
                }
            }
        } catch (Exception e) {
            throw new ConfigException("请求WebApi异常", e);
        }

        return null;
    }

    /**
     * 活动集入参序列化
     *
     * @param inputParams
     * @return
     */
    public static String serializeInputParams(Map<String, Object> inputParams) {
        if (inputParams == null || inputParams.isEmpty()) {
            return null;
        }
        Map<String, Object> _temp = new HashMap<String, Object>();
        for (Map.Entry<String, Object> entry : inputParams.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (null != value) {
                // 如果是dataView对象，则进行dataView的序列化
                String dbSerialize = null;
                if (value instanceof IDataView) {
                    String newKey = key + Mark_Entity;
                    dbSerialize = ((IDataView) value).toJson();
                    _temp.put(newKey, dbSerialize);
                    continue;
                } else if ((value instanceof String)
                        || (value instanceof Boolean)
                        || (value instanceof Number)
                        || (value instanceof Integer)) {
                    // 只有基础数据类型
                    _temp.put(key, value);
                }
            } else {
                // 空值直接放null进去
                _temp.put(key, null);
            }
        }
        String serialize = VdsUtils.json.toJson(_temp);
        return serialize;
    }

    /**
     * 活动集入参反序列化
     *
     * @param serialize
     * @return
     */
    private Map<String, Object> unserializeInputParams(String serialize) {
        if (isEmpty(serialize)) {
            return null;
        }
        Map<String, Object> _temp = VdsUtils.json.fromJson(serialize);
        Map<String, Object> inputParams = new HashMap<String, Object>();
        for (Map.Entry<String, Object> entry : _temp.entrySet()) {
            String key = entry.getKey();
            // 参数key包含实体标识，则对其字符串反序列化成DataView
            if (key.endsWith(Mark_Entity)) {
                String dbSerialize = (String) entry.getValue();
                String oriKey = key.substring(0, key.length() - Mark_Entity.length());
                IDataView dataView = null;
                if (!isEmpty(dbSerialize)) {
                    dataView = getVDS().getDas().createDataViewByMetadata().fromJson(dbSerialize);
                }
                inputParams.put(oriKey, dataView);
                continue;
            }
            Object value = entry.getValue();
            inputParams.put(key, value);
        }
        return inputParams;
    }

    private String encryptAES(String value, String privateKey) throws Exception {
        try {
            byte[] raw = privateKey.getBytes();
            SecretKeySpec skeySpec = new SecretKeySpec(raw, "AES");
            Cipher cipher = Cipher.getInstance("AES");
            cipher.init(Cipher.ENCRYPT_MODE, skeySpec);
            return byteToHexString(cipher.doFinal(value.getBytes()));
        } catch (Exception ex) {
            throw new Exception(ex);
        }
    }

    private String byteToHexString(byte[] bytes) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < bytes.length; ++i) {
            int v = bytes[i];
            if (v < 0) {
                v += 256;
            }
            String n = Integer.toHexString(v);
            if (n.length() == 1)
                n = "0" + n;
            builder.append(n);
        }

        return builder.toString();
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

    private boolean isEmpty(String str) {
        if (str == null || str.equals("")) {
            return true;
        }

        return false;
    }

    private IVDS getVDS() {
        IVDS intance = VDS.getIntance();
        return intance;
    }
}
