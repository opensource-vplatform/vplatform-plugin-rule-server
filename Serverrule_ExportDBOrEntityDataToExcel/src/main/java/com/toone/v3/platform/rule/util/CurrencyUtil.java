package com.toone.v3.platform.rule.util;

import com.yindangu.v3.business.metadata.api.IDataObject;
import com.yindangu.v3.business.metadata.api.IDataView;
import com.yindangu.v3.business.plugin.business.api.rule.ContextVariableType;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleContext;
import com.yindangu.v3.business.plugin.execptions.BusinessException;

import java.sql.SQLException;
import java.util.*;

public class CurrencyUtil {
    /**
     * 用于保存执行过程中产生的错误信息
     */
    public static String ERRORMESSAGE="";

    /**
     * 重新对DataView进行排序
     * @param source
     * @param filterExpression 过滤条件
     * @param orderBy 排序字段
     * @param paramMap 参数
     * @return
     */
    public static IDataView orderByForDataView(IDataView source,String filterExpression,String orderBy, Map<String, Object> paramMap){
        IDataView dataView = null;
        List<IDataObject> list = source.select(filterExpression, orderBy, paramMap);//排序
        dataView = source.copyMetaData();
        List<Map<String, Object>> items = new ArrayList<Map<String, Object>>();
        for(IDataObject dataObject:list){
            try {
                Map<String, Object> item = new HashMap<String, Object>();
                Set<String> columnSet = source.getMetadata().getColumnNames();
                for(Object object:columnSet){
                    item.put(object.toString(), dataObject.get(object.toString()));
                }
                items.add(item);
            } catch (SQLException e) {
                throw new BusinessException("DataView重新排序错误，orderBy="+orderBy);
            }
        }
        dataView.insertDataObject(items);
        return dataView;
    }

    /**
     * 获取活动集变量
     * @param context 规则上下文
     * @param type 变量类型
     * @param dataSourceName 变量名称
     * @return
     */
    public static IDataView getVariableDataView(IRuleContext context,String type,String dataSourceName){
        IDataView dataView = null;
        if ("ruleSetVar".equals(type)) {
            dataView = (IDataView) context.getVObject().getContextObject(dataSourceName, ContextVariableType.getInstanceType(type));
//            dataView = (IDataView) RuleSetVariableUtil.getContextVariable(context, dataSourceName);
        } else if ("ruleSetOutput".equals(type)) {
            dataView = (IDataView) context.getVObject().getContextObject(dataSourceName, ContextVariableType.getInstanceType(type));
//            dataView = (IDataView) RuleSetVariableUtil.getOutputVariable(context, dataSourceName);
        } else if ("ruleSetInput".equals(type)) {
            dataView = (IDataView) context.getVObject().getContextObject(dataSourceName, ContextVariableType.getInstanceType(type));
//            dataView = (IDataView) RuleSetVariableUtil.getInputVariable(context, dataSourceName);
        }else{
            ERRORMESSAGE="CurrencyUtil: 无法识别这种类型的变量【"+type+"】";
        }
        return dataView;
    }

}
