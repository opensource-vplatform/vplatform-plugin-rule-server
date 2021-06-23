package com.toone.v3.platform.rule;

import com.yindangu.v3.business.VDS;
import com.yindangu.v3.business.metadata.api.IDataObject;
import com.yindangu.v3.business.metadata.api.IDataView;
import com.yindangu.v3.business.plugin.business.api.rule.*;
import com.yindangu.v3.business.rule.api.parse.IQueryParse;
import com.yindangu.v3.business.rule.api.parse.ISQLBuf;

import java.util.List;
import java.util.Map;

/**
 * 删除实体记录
 *
 * @Author xugang
 * @Date 2021/6/22 11:23
 */
public class EntityConditionRemove implements IRule {

    public static final String Param_dtMaster = "dtMaster";
    public static final String Param_entityName = "entityName";
    public static final String Param_entityType = "entityType";
    public static final String Param_dsWhere = "dsWhere";

    @Override
    public IRuleOutputVo evaluate(IRuleContext context) {
        IRuleOutputVo outputVo = context.newOutputVo();
        List<Map> dtMaster = (List<Map>) context.getPlatformInput(Param_dtMaster);
        if (dtMaster == null || dtMaster.isEmpty()) {
            return outputVo;
        }
        for (Map dtChileMap : dtMaster) {
            String entityName = (String) dtChileMap.get(Param_entityName);
            String entityType = (String) dtChileMap.get(Param_entityType);
            List<Map> conditions = (List<Map>) dtChileMap.get(Param_dsWhere);

            IQueryParse vparse= VDS.getIntance().getVSqlParse();
            ISQLBuf sql = vparse.parseConditionsJson(conditions);

            String condSql = sql.getSQL();
            Map<String, Object> queryParams = sql.getParams();

            IRuleVObject ruleVObject = context.getVObject();

            ContextVariableType targetEntityType = ContextVariableType.getInstanceType(entityType);
            IDataView dataView = (IDataView)ruleVObject.getContextObject(entityName, targetEntityType);
            if (dataView != null && dataView.getDatas().size()>0) {
                List<IDataObject> list = dataView.select(condSql, queryParams);
                for (IDataObject dataObject : list) {
                    // 实体记录清除
                    dataObject.remove();
                }
            }
        }

        outputVo.put(true);
        return outputVo;
    }
}
