package com.toone.itop.formula.rule.inte;


import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yindangu.v3.business.VDS;
import com.yindangu.v3.business.formula.api.IEvalFormulaInterceptor;
import com.yindangu.v3.business.formula.api.IFormulaEngine;
import com.yindangu.v3.business.formula.spi.IEvalFormulaCallback;
import com.yindangu.v3.business.metadata.api.IDataObject;
import com.yindangu.v3.business.plugin.business.api.rule.IRule;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleContext;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleOutputVo;
import com.yindangu.v3.business.plugin.execptions.ConfigException;
import com.yindangu.v3.platform.plugin.util.VdsUtils;

/**
 * 删除数据库中的记录
 * @author jiqj
 *
 */
public class DeleteConditionRelationData implements IRule  { // extends AbstractRule4Tree {
	
		private static final Logger log = LoggerFactory.getLogger(SetLoopVariant.class); 
		public static final String D_RULE_NAME = "删除数据库中的记录";
		public static final String D_RULE_CODE = "DeleteConditionRelationData";
		public static final String D_RULE_DESC = "删除数据库中选中表的记录，支持条件筛选。\r\n" + 
				"方法名：" + D_RULE_CODE;
		private static final String D_dtChileMaps= "dtChileMaps";
		private static final String D_treeStruct = "treeStruct";
			
			
    @SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public IRuleOutputVo evaluate(IRuleContext context) {
        //Map<String, Object> inParams = (Map<String, Object>) context.getRuleConfig().getConfigParams();
        List<Map> dtChileMaps = (List<Map>) context.getPlatformInput(D_dtChileMaps);
        if (VdsUtils.collection.isEmpty(dtChileMaps)) {
            return context.newOutputVo();
        }
        
        //List<Map> treeStructList = inParams.get("treeStruct") != null ? (List<Map>) inParams.get("treeStruct") : null; // 树形结构
        List<Map> treeStructList =  (List<Map>) context.getPlatformInput(D_dtChileMaps);
        boolean isTree = !(VdsUtils.collection.isEmpty(treeStructList));
        if (isTree) {
            DASRuntimeContextFactory.getService().addTreeStructMaps(
                    treeStructList);
        }
        DAS das = IMetaDataFactory.getService().das();
        for (Map dtChileMap : dtChileMaps) {
            String tableName = (String) dtChileMap.get("tableName");
            List<Map> conditions = (List<Map>) dtChileMap.get("dsWhere");
//            if (CollectionUtils.isEmpty(conditions)) {
//                das.executeUpdate("delete from " + tableName);
//                continue;
//            }
            SQLBuf sql = QueryConditionUtil
                    .parseConditionsNotSupportRuleTemplate(conditions);
            String condSql = sql.getSQL();
            Map condParams = sql.getParams();
            // 拼查询语句
            String countFieldAlias = "countField";
            StringBuilder sbSelect = new StringBuilder(" select count(1) as ");
            sbSelect.append(countFieldAlias);
            sbSelect.append(" from ").append(tableName).append(" ");
            String conditionsql = null;
            if (!StringUtils.isBlank(condSql)) {
                conditionsql = "where ";
                if (condSql.startsWith("  and  ")
                        || condSql.startsWith("  or  ")) {
                    conditionsql = conditionsql + " 1 = 1 ";
                }
                conditionsql = conditionsql + condSql;
                sbSelect.append(conditionsql);
            }

            // 查数据
            DataView dataview = das.findWithNoFilter(sbSelect.toString(), condParams);
            List<DataObject> datas = dataview.select();
            long totalValue = 0;
            Object total = datas.get(0).get(countFieldAlias);
            if (total instanceof Number) {
                totalValue = ((Number) total).longValue();
            }
            // 如果有要删的数据，才执行删除
            if (totalValue > 0) {
                if (isTree) {// 树形结构删除
                    deleteTreeDatas(tableName, conditionsql, condParams);
                } else { // 普通表删除
                    deleteDatas(tableName, conditionsql, condParams);
                }
            }
        }
        return true;
    }

    /**
     * 删除数据(树形)
     * 
     * @return
     */
    @SuppressWarnings("rawtypes")
    private void deleteTreeDatas(String tableName, String conditionsql,
            Map paramsMap) {
        IMetaDataFactory.getService().das()
                .delete(tableName, conditionsql, paramsMap);
    }

    /**
     * 删除数据
     * 
     * @param queryTableName
     * @param sql
     * @return
     */
    @SuppressWarnings("rawtypes")
    private void deleteDatas(String queryTableName, String conditionsql,
            Map paraMap) {
        StringBuilder sb = new StringBuilder(" delete");
        sb.append(" from ").append(queryTableName).append(" ");
        if (conditionsql != null) {
            sb.append(conditionsql);
        }
        IMetaDataFactory.getService().das()
                .executeUpdate(sb.toString(), paraMap);
    }
}
