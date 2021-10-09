package com.toone.v3.platform.rule;

import com.yindangu.v3.business.VDS;
import com.yindangu.v3.business.jdbc.api.model.IQueryParamsVo;
import com.yindangu.v3.business.plugin.business.api.rule.IRule;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleContext;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleOutputVo;
import com.yindangu.v3.business.plugin.execptions.ConfigException;
import com.yindangu.v3.business.vds.IVDS;
import com.yindangu.v3.business.vsql.apiserver.IVSQLConditions;
import com.yindangu.v3.business.vsql.apiserver.VSQLConditionLogic;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author xugang
 * @Date 2021/7/23 8:53
 */
public class ExecuteVoidQuery implements IRule {

    private static final String Param_QueryCode = "queryCode";
    private static final String Param_QueryParam = "queryParam";

    @Override
    public IRuleOutputVo evaluate(IRuleContext context) {
        try {
            String quertCode = context.getPlatformInput(Param_QueryCode).toString();
            List<Map<String, Object>> queryParam = (List<Map<String, Object>>) context.getPlatformInput(Param_QueryParam);

            // 初始化查询参数
            Map<String, Object> queryParams = new HashMap<String, Object>();
            if (!isEmpty(queryParam)) {
                for (Map<String, Object> paramConfig : queryParam) {
                    String paramKey = (String) paramConfig.get("queryfield");
                    Object paramValue = null;
                    String paramValueExpr = (String) paramConfig
                            .get("queryfieldValue");
                    if (!isEmpty(paramValueExpr)) {
                        paramValue = getVds().getFormulaEngine().eval(
                                paramValueExpr);
                    }
                    queryParams.put(paramKey, paramValue);
                }
            }
            String whereCond = "";
            findFromQuery(quertCode, whereCond, queryParams);

            IRuleOutputVo outputVo = context.newOutputVo();
            outputVo.put(null);
            return outputVo;
        } catch (Exception e) {
            throw new ConfigException("规则[ExecuteVoidQuery]：执行失败！"+e.getMessage());
        }
    }

    /**
     * 获取自定义查询数据
     *
     * @param queryName
     * @param extraQueryCondition
     * @param queryParams
     * @return
     */
    private void findFromQuery(String queryName, String extraQueryCondition,Map<String, Object> queryParams) {

        if (null == queryParams) {
            queryParams = new HashMap<String, Object>();
        }

        // 调用时，后台可能会有缺参数的情况，所以这里需要做一下补全
        List<IQueryParamsVo> vQueryParams = getVds().getMdo().getTable(queryName).getQueryParams();
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
            extraSqlConditions = getVds().getVSQLQuery().getVSQLConditions(extraQueryCondition);
            extraSqlConditions.setLogic(VSQLConditionLogic.AND);
        }
        //执行
        try {
            getVds().getVSQLQuery().loadQueryData(queryName,queryParams, extraSqlConditions, true);
        } catch (Exception e) {
            throw new ConfigException("规则[ExecuteVoidQuery]：语句执行失败！请检查语句是否有误！queryName="+queryName+"\n"+e.getMessage());
        }

    }

    private IVDS getVds() {
        return VDS.getIntance();
    }

    private boolean isEmpty(String str) {
        if(str == null || str.equals("")) {
            return true;
        }

        return false;
    }

    private boolean isEmpty(List<?> list) {
        if(list == null || list.isEmpty()) {
            return true;
        }

        return false;
    }
}
