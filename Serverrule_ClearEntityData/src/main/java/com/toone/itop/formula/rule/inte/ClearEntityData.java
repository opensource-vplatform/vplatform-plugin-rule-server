package com.toone.itop.formula.rule.inte;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yindangu.v3.business.metadata.api.IDataView;
import com.yindangu.v3.business.plugin.business.api.rule.ContextVariableType;
import com.yindangu.v3.business.plugin.business.api.rule.IRule;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleContext;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleOutputVo;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleVObject;
import com.yindangu.v3.business.plugin.execptions.ConfigException;
import com.yindangu.v3.platform.plugin.util.VdsUtils;
/**
 * 后台规则：清除实体记录
 * 
 * @author dengb
 * 
 */
public class ClearEntityData implements IRule  {
	private static final Logger log = LoggerFactory.getLogger(ClearEntityData.class); 
	public static final String D_RULE_NAME = "清除实体记录";
	public static final String D_RULE_CODE = "ClearEntityData";
	public static final String D_RULE_DESC = "清除实体中的所有记录\r\n" + 
			"支持方法输入实体、方法变量实体、方法输出实体\r\n" + 
			"方法名：" + D_RULE_CODE;
	private static final String D_dtMaster="dtMaster";
	@Override
	public IRuleOutputVo evaluate(IRuleContext context) {
		//log.info("使用新插件接口..");
		//Map<String, Object> inParams = (Map<String, Object>) context.getRuleConfig().getConfigParams();
		List<Map> dtMaster = (List<Map>)context.getPlatformInput(D_dtMaster);
		if (VdsUtils.collection.isEmpty(dtMaster)) {
			return null;
		}
		IRuleVObject ruleVObjec = context.getVObject();
		for (Map dtChileMap : dtMaster) {
			String entityName = (String) dtChileMap.get("entityName");
			String entityType = (String) dtChileMap.get("entityType");
			ContextVariableType contextType = ContextVariableType.getInstanceType(entityType);
			if(contextType == null) {
				throw new ConfigException("不支持类型[" + entityType + "]的变量值设置.");
			}
			IDataView dataView = (IDataView)ruleVObjec.getContextObject(entityName, contextType);
			if(null!=dataView && dataView.getDatas().size()>0){
				dataView.removeAll();
			}
		}
		return context.newOutputVo().put(Boolean.TRUE);
	} 
}
