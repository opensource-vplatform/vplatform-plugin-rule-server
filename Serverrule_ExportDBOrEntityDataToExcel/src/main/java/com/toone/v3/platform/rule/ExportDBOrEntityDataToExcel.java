package com.toone.v3.platform.rule;

import com.toone.v3.platform.rule.model.DataEXportCfg;
import com.toone.v3.platform.rule.model.DataType;
import com.toone.v3.platform.rule.model.EXportDataCfg;
import com.toone.v3.platform.rule.model.FileInputStreamWrapper;
import com.toone.v3.platform.rule.util.CurrencyUtil;
import com.toone.v3.platform.rule.util.ExcelExportData;
import com.yindangu.v3.business.VDS;
import com.yindangu.v3.business.file.api.IFileOperate;
import com.yindangu.v3.business.file.api.model.IAppFileInfo;
import com.yindangu.v3.business.jdbc.api.model.ColumnType;
import com.yindangu.v3.business.jdbc.api.model.IQueryParamsVo;
import com.yindangu.v3.business.jdbc.api.model.ITable;
import com.yindangu.v3.business.metadata.api.IDAS;
import com.yindangu.v3.business.metadata.api.IDataView;
import com.yindangu.v3.business.plugin.business.api.rule.ContextVariableType;
import com.yindangu.v3.business.plugin.business.api.rule.IRule;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleContext;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleOutputVo;
import com.yindangu.v3.business.plugin.execptions.BusinessException;
import com.yindangu.v3.business.rule.api.parse.IConditionParse;
import com.yindangu.v3.business.rule.api.parse.ISQLBuf;
import com.yindangu.v3.business.ruleset.api.context.IRuleSetRuntimeContext;
import com.yindangu.v3.business.vds.IVDS;
import com.yindangu.v3.business.vsql.apiserver.IVSQLConditions;
import com.yindangu.v3.business.vsql.apiserver.IVSQLOrderBy;
import com.yindangu.v3.business.vsql.apiserver.IVSQLQuery;
import com.yindangu.v3.business.vsql.apiserver.VSQLConditionLogic;
import com.yindangu.v3.platform.plugin.util.VdsUtils;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

/**
 * @Author xugang
 * @Date 2021/7/27 9:30
 */
public class ExportDBOrEntityDataToExcel implements IRule {

    @Override
    public IRuleOutputVo evaluate(IRuleContext context) {
        EXportDataCfg singleConf = null;
        Map<String, Object> configData = new LinkedHashMap<String, Object>();
        String version = isEmpty((String) context.getPlatformInput("fileType")) ? "xls" : (String) context.getPlatformInput("fileType");
        try {
            String title = getVDS().getFormulaEngine().eval(context.getPlatformInput("defaultFileName").toString());
            List items = (List) context.getPlatformInput("items");
            for (Object object : items) {
                Map sourceMap = (Map) object;
                Map scMap = new HashMap();
                String sheetName = getVDS().getFormulaEngine().eval(sourceMap.get("sheetName").toString());
                String iscu = sourceMap.get("columnHeaderFormat").toString();
                if (iscu.equals("columnNameFieldCode")) {
                    scMap.put("IsCU", true);
                } else {
                    scMap.put("IsCU", false);
                }
                Map<String, Object> param = HandleParam(sourceMap);//获取过滤条件和查询参数。
                String datasource = (String) sourceMap.get("dataSource");
                if (isEmpty(title)) title = datasource;
                EXportDataCfg conf = parse(sourceMap, title, param);
                IDataView exportData = queryDatas(sourceMap, context, param);
                if (singleConf == null) singleConf = conf;//用于选择导出工具

                scMap.put("EXportDataCfg", conf);
                scMap.put("DataView", exportData);
                if (configData.containsKey(sheetName)) throw new BusinessException("后台规则-导出Excel：配置的sheet名称有重复");
                configData.put(sheetName, scMap);
            }

            //获取临时文件
            File tmpFile = exportTmpFile(configData, singleConf, version);

            // 临时文件留到输入流关闭时清除
            InputStream in = new FileInputStreamWrapper(tmpFile);
            IAppFileInfo fileInfo = getVDS().newAppFileInfo();
            fileInfo.setId(VdsUtils.uuid.generate());
            if (singleConf != null) {
                fileInfo.setFileType(singleConf.getExportFileExtension());
                fileInfo.setOldFileName(singleConf.getDefaultFileName());
            }
            fileInfo.setFileSize(tmpFile.length());
            fileInfo.setDataStream(in);
            IFileOperate fileOperate = getVDS().getFileOperate();
            fileOperate.saveFileInfo(fileInfo);
            String result = fileInfo.getId();
            List sourceList = (List) context.getPlatformInput("fileIdSave");
            //保存文件ID到变量
            SetResult(result, sourceList, context);

            IRuleOutputVo outputVo = context.newOutputVo();
            outputVo.put(result);
            return outputVo;
        } catch (Exception e) {
            LoggerFactory.getLogger(getClass()).warn("导出EXCEL失败！", e.getMessage());
            throw new BusinessException("规则-导出Excel：执行失败！" + e.getMessage());
        }
    }

    /**
     * 将数据导出为临时文件
     * @param config
     * @param singleCfg
     * @param version
     * @return
     * @throws Exception    如果导出失败，则将临时文件删除，并抛出异常
     */
    protected File exportTmpFile(Map<String, Object> config, DataEXportCfg singleCfg, String version) throws Exception {
        File tmpFile = null;
        OutputStream out = null;

        ExcelExportData export = new ExcelExportData();
//        IExportData export = IExportDataManagerFactory.getService().getExportData(singleCfg);
        // 导出成功的标志， 导出失败的话，需要清理临时文件
        boolean exportSuccess = false;
        try {
            tmpFile = File.createTempFile("export", ".xls");
            out = new BufferedOutputStream(new FileOutputStream(tmpFile));
            export.exportData(config, out, version);
            exportSuccess = true;
            return tmpFile;
        } finally {
            quietClose(out);
            // 如果导出失败，则清理临时文件，否则在文件下载完成后清理临时文件
            if (!exportSuccess) {
                if (tmpFile != null)
                    tmpFile.delete();
            }
        }
    }

    public EXportDataCfg parse(Map sourceMap, String title, Map<String, Object> param) throws Exception {
//		Map<String, Object> param = HandleParam(sourceMap);
        String oriDataEXportCfgJson = VdsUtils.json.toJson(sourceMap);
        // 规则Id
        EXportDataCfg cfg = VdsUtils.json.fromJson(oriDataEXportCfgJson, EXportDataCfg.class);
        // 标题(表达式计算后的结果)
        cfg.setTitle(title);

        // 查询条件(查询条件计算后的结果)
        cfg.setCondSql(param.get("condSql").toString());

        // 查询条件(参数值)
        Map condParams = (Map) param.get("condParams");

        // 查询参数(计算后的结果)
        if (condParams != null && !condParams.isEmpty()) {
            // 如果是查询的话，查询参数有可能为null，这时候需要设置默认查询参数
            if (DataType.QUERY.name().equalsIgnoreCase(cfg.getDataSourceType())) {
                cfg.getParamObject().putAll(fixQueryParamsIfNull(cfg.getDataSource(), condParams));
            } else {
                cfg.getParamObject().putAll(condParams);
            }
        }

        return cfg;
    }

    /**
     * 查询数据
     *
     * @param sourceMap
     * @param context
     * @param param
     * @return
     * @throws Exception
     */
    protected IDataView queryDatas(Map sourceMap, IRuleContext context, Map<String, Object> param) throws Exception {
        IDataView dataView = null;
        String dataSourceName = sourceMap.get("dataSource").toString();
        String whereCond = param.get("condSql").toString();
        String dataType = sourceMap.get("dataSourceType").toString();
        String orderBy = getOrderBy(sourceMap);
        Map<String, Object> queryParams = (Map<String, Object>) param.get("condParams");
        if (dataType.equalsIgnoreCase("table")) {//数据库表
            dataView = findFromTable(dataSourceName, null, whereCond, orderBy, queryParams);
        } else if (dataType.equalsIgnoreCase("query")) {//查询
            dataView = findFromQuery(dataSourceName, whereCond, queryParams, orderBy);
        } else {//方法变量
            dataView = CurrencyUtil.getVariableDataView(context, dataType, dataSourceName);
            if (dataView == null) {
                throw new BusinessException(CurrencyUtil.ERRORMESSAGE);
            }
            //暂时没有接口对活动集变量进行排序。此操作必影响性能问题。
            dataView = CurrencyUtil.orderByForDataView(dataView, whereCond, orderBy, queryParams);//对DataView进行重新排序
            return dataView;
        }
        return dataView;
    }

    /**
     * 将数据导出为临时文件
     * @param config
     * @param singleCfg
     * @param version
     * @return
     * @throws Exception 如果导出失败，则将临时文件删除，并抛出异常
     */
    protected File exportTmpFile(Map<String, Object> config, EXportDataCfg singleCfg, String version) throws Exception {
        File tmpFile = null;
        OutputStream out = null;
        ExcelExportData export = new ExcelExportData();
//        IExportData export = IExportDataManagerFactory.getService().getExportData(singleCfg);
        // 导出成功的标志， 导出失败的话，需要清理临时文件
        boolean exportSuccess = false;
        try {
            tmpFile = File.createTempFile("export", ".xls");
            out = new BufferedOutputStream(new FileOutputStream(tmpFile));
            export.exportDataBack(config, out, version);
            exportSuccess = true;
            return tmpFile;
        } finally {
            quietClose(out);
            // 如果导出失败，则清理临时文件，否则在文件下载完成后清理临时文件
            if (!exportSuccess) {
                if (tmpFile != null)
                    tmpFile.delete();
            }
        }
    }

    /**
     * 获取排序字段
     *
     * @param sourceMap
     * @return
     */
    private String getOrderBy(Map sourceMap) {
        if (sourceMap == null) return "";
        String orderBy = "";
        List items = (List) sourceMap.get("mapping");
        if (items != null)
            for (int i = 0; i < items.size(); i++) {
                Map map = (Map) items.get(i);
                Object orderType = map.get("orderType");
                if (orderType != null && !orderType.equals("")) {
                    orderBy = orderBy + map.get("fieldCode") + " " + orderType + ",";
                }
            }
        if (orderBy != "") orderBy = orderBy.substring(0, orderBy.length() - 1);
        return orderBy;
    }

    /**
     * 处理查询参数为null的情况--->取defaultParamValue
     *
     * @param queryName
     * @param params
     * @return
     */
    protected Map<String, Object> fixQueryParamsIfNull(String queryName, Map params) {
        if (params == null) {
            return null;
        }

        ITable queryParamsTable = getVDS().getMdo().getTable(queryName);
        if (queryParamsTable == null) {
            throw new BusinessException("不存在表名为" + queryName + "的查询，请检查配置");
        }

        Map<String, IQueryParamsVo> initParams = new HashMap<String, IQueryParamsVo>();
        List<IQueryParamsVo> queryParams = queryParamsTable.getQueryParams();
        for (IQueryParamsVo initParam : queryParams) {
            initParams.put(initParam.getParamName(), initParam);
        }

        Map<String, Object> result = new HashMap<String, Object>(params.size());
        for (Map.Entry entry : (Set<Map.Entry>) params.entrySet()) {
            String key = (String) entry.getKey();
            Object value = entry.getValue();

            if (value == null && initParams.containsKey(key)) {
                IQueryParamsVo initParam = initParams.get(key);
                ColumnType v3ValueTypeEnum = ColumnType.getColumnType(initParam.getValueType());
                Object paramValue = convertValue(v3ValueTypeEnum, initParam.getInitValue());
                // 处理""值转换为null值问题
                if (paramValue == null || "".equals(paramValue)) {
                    result.put(key, null);
                } else {
                    result.put(key, paramValue);
                }
            } else {
                result.put(key, value);
            }
        }
        return result;
    }

    /**
     * 根据字段类型，将字符串转为对应的JAVA类型
     *
     * @param strValue 字段值
     * @return
     */
    private Object convertValue(ColumnType type, String strValue) {
        if (type.equals(ColumnType.Text) || type.equals(ColumnType.LongText) || type.equals(ColumnType.File)) {
            return strValue;
        } else if (type.equals(ColumnType.Boolean)) {
            return isEmpty(strValue) ? false : toBooleanObj(strValue);
        } else if (type.equals(ColumnType.Integer)) {
            return isEmpty(strValue) ? new Integer(1) : toIntegerObj(strValue);
        } else if (type.equals(ColumnType.Number)) {
            return isEmpty(strValue) ? new BigDecimal(1) : new BigDecimal(strValue);
        } else {
            return strValue;
        }
    }

    /**
     * 将对象转为布尔值
     *
     * @param obj
     * @return
     */
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

    private Integer toIntegerObj(Object obj) {
        Integer rs = Integer.valueOf(0);
        if (obj == null) {
            return rs;
        }

        if (obj instanceof Integer) {
            rs = (Integer) obj;
        } else if (obj instanceof BigDecimal) {//转Integer溢出 jiqj
            BigDecimal b = (BigDecimal) obj;
            int r = 1;
            try {
                r = b.intValueExact();
            } catch (ArithmeticException e) {
                throw new RuntimeException("[" + b.toString() + "]转Integer溢出");
            }
            rs = Integer.valueOf(r);
        } else if (obj instanceof BigInteger) {//转Integer溢出 jiqj
            BigInteger b = (BigInteger) obj;
            if (b.bitLength() > 31) {
                throw new RuntimeException("[" + b.toString() + "]转Integer溢出");
            }
            int r = b.intValue();
            rs = Integer.valueOf(r);
        } else if (obj instanceof Number) {
            rs = Integer.valueOf(((Number) obj).intValue());
        } else if (obj instanceof String) {
            if (!isEmpty(obj.toString())) {
                rs = new Integer(obj.toString());
            }
        } else if (obj instanceof Boolean) {
            rs = Integer.valueOf(((Boolean) obj) ? 1 : 0);
        } else {
            throw new RuntimeException("转换Integer类型错误！目前只支持Integer,Number,String类型");
        }
        return rs;
    }

    /**
     * 处理参数
     *
     * @param sourceMap
     */
    private Map<String, Object> HandleParam(Map sourceMap) {
        Map<String, Object> result = new LinkedHashMap<String, Object>();
        Object dsWhere = sourceMap.get("filterCondition");
        List<Map<String, Object>> dsQueryParam = (List<Map<String, Object>>) (sourceMap.get("queryParam") != null ? sourceMap.get("queryParam") : null);

        // 初始化查询参数
        Map<String, Object> queryParams = new HashMap<String, Object>();
        if (!isEmpty(dsQueryParam)) {
            for (Map<String, Object> paramConfig : dsQueryParam) {
                String paramKey = (String) paramConfig.get("queryfield");
                Object paramValue = null;
                String paramValueExpr = (String) paramConfig.get("queryfieldValue");
                if (!isEmpty(paramValueExpr)) {
                    paramValue = getVDS().getFormulaEngine().eval(paramValueExpr);
                }
                queryParams.put(paramKey, paramValue);
            }
        }

        List<Map> condSql = new ArrayList<Map>();
        if (dsWhere != null && dsWhere instanceof List)
            condSql = (List<Map>) dsWhere;
        String whereCond = "";
        if (!isEmpty(condSql)) {
            ISQLBuf sb = getVDS().getVSqlParse().parseConditionsJson(condSql);
//            SQLBuf sb = QueryConditionUtil.parseConditionsNotSupportRuleTemplate(condSql);
            whereCond = sb.getSQL();
            queryParams.putAll(sb.getParams());
        }
        if (whereCond.equals("")) whereCond = "1=1";
        result.put("condSql", whereCond);
        result.put("condParams", queryParams);
        return result;
    }

    /**
     * 获取自定义查询数据
     *
     * @param queryName
     * @param extraQueryCondition
     * @param queryParams
     * @param orderBy
     * @return
     */
    private IDataView findFromQuery(String queryName, String extraQueryCondition, Map<String, Object> queryParams, String orderBy) {
        IDataView dataView = null;

        if (null == queryParams) {
            queryParams = new HashMap<String, Object>();
        }

        // 调用时，后台可能会有缺参数的情况，所以这里需要做一下补全
        List<IQueryParamsVo> vQueryParams = getVDS().getMdo().getTable(queryName).getQueryParams();
        if (!isEmpty(vQueryParams)) {
            for (IQueryParamsVo param : vQueryParams) {
                String paramName = param.getParamName();
                if (!queryParams.containsKey(paramName)) {
                    queryParams.put(paramName, null);
                }
            }
        }

        // 查询附加条件组装
        IVSQLConditions extraSqlConditions = null;
        if (!isEmpty(extraQueryCondition)) {
            extraSqlConditions = getVDS().getVSQLQuery().getVSQLConditions(extraQueryCondition);
//            extraSqlConditions = IVSQLConditionsFactory.getService().init();
            extraSqlConditions.setSqlConStr(extraQueryCondition);
            extraSqlConditions.setLogic(VSQLConditionLogic.AND);
        }

        if (null == orderBy || orderBy.length()==0) {
            dataView = getVDS().getVSQLQuery().loadQueryData(queryName, queryParams, extraSqlConditions, true);
//            dataView = IVSQLQueryFactory.getService().loadQueryData(queryName, queryParams, extraSqlConditions, true);
        } else {
        	IVSQLQuery q = getVDS().getVSQLQuery();
            IVSQLOrderBy ivsql = q.newSQLOrderBy();//initVSQLOrderBy();
//            IVSQLOrderBy ivsql = IVSQLOrderByFactory.getService().getVSQLOrderBy();
            ivsql.setSqlConStr("order by " + orderBy);
            //dataView = getVDS().getVSQLQuery().loadQueryData(queryName, queryParams, extraSqlConditions, ivsql, true);
            dataView = q.loadQueryData(queryName, queryParams, extraSqlConditions, ivsql,0,0, true);
        }

        return dataView;
    }

    /**
     * 获取物理表数据
     *
     * @param tableName
     * @param queryFields
     * @param queryCondition
     * @param queryOrderBy
     * @param queryParams
     * @return
     */
    private IDataView findFromTable(String tableName, List<String> queryFields, String queryCondition,
                                    String queryOrderBy, Map<String, Object> queryParams) {
        String sql = generateSelectExpression(tableName, queryFields, queryCondition, queryOrderBy);

        IDAS das = getVDS().getDas();
        IDataView dataView = null;

        if (null == queryParams) {
            queryParams = new HashMap<String, Object>();
        }
        // 无分页的查询
        dataView = das.find(sql, queryParams);
        return dataView;
    }

    /**
     * 生成查询sql语句
     *
     * @param tableName
     * @param queryFields
     * @param queryCondition
     * @param queryOrderBy
     * @return
     */
    private String generateSelectExpression(String tableName, List<String> queryFields, String queryCondition,
                                            String queryOrderBy) {

        String selectExpression = "";

        // 添加查询列
        StringBuffer selectFields = new StringBuffer("");

        if (isEmpty(queryFields)) {
            selectFields.append(" * ");
        } else {
            int counter = 0;
            for (String fieldName : queryFields) {
                if (counter > 0) {
                    selectFields.append(", ");
                }

                selectFields.append(fieldName);
                counter++;
            }
        }

        // 添加condition
        if (isEmpty(queryCondition)) {
            queryCondition = "1 = 1";
        }

        // 添加orderby列
        StringBuffer orderBy = new StringBuffer("");
        if (!isEmpty(queryOrderBy)) {
            orderBy.append(" order by ").append(queryOrderBy);
        }

        StringBuffer sql = new StringBuffer("");
        sql.append(" select ");
        sql.append(selectFields);
        sql.append(" from ");
        sql.append(tableName);
        sql.append(" where ");
        sql.append(queryCondition);
        sql.append(orderBy);

        selectExpression = sql.toString();
        return selectExpression;
    }

    /**
     * 设置返回的结果
     *
     * @param fileId     文件ID
     * @param sourceList 保存文件ID的配置信息
     */
    public void SetResult(String fileId, List sourceList, IRuleContext context) {
        if (sourceList != null) {
            Map sourceMap = (Map) sourceList.get(0);
            String targetType = sourceMap.get("targetType").toString();
            String name = sourceMap.get("target").toString();
            IRuleSetRuntimeContext runtimeContext = (IRuleSetRuntimeContext) context.getVObject().getContextObject(null, ContextVariableType.RuleChainRuntimeContext);
            if (targetType.equals("methodOutput")) {
//                name = "BR_OUT_PARENT."+name;
//                RuleSetVariableUtil.setVariable(context, name, fileId);
                runtimeContext.setOutputVariable(name, fileId);
            } else if (targetType.equals("methodVariant")) {
//                name = "BR_VAR_PARENT."+name;
//                RuleSetVariableUtil.setVariable(context, name, fileId);
                runtimeContext.setContextVariable(name, fileId);
            } else {
                throw new BusinessException("规则-导出Excel：不支持这种类型【" + targetType + "】变量保存文件ID");
            }
        } else {
            throw new BusinessException("规则-导出Excel：无法读取文件ID保存的配置信息");
        }
    }

    private IVDS getVDS() {
        return VDS.getIntance();
    }

    private void quietClose(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException e) {
            }
        }
    }

    private boolean isEmpty(String str) {
        if (str == null || str.equals("")) {
            return true;
        }

        return false;
    }

    private boolean isEmpty(Collection<?> coll) {
        if (coll == null || coll.isEmpty()) {
            return true;
        }

        return false;
    }
}
