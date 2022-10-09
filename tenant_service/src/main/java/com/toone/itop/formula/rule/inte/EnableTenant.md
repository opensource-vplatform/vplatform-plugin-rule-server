package com.toone.itop.formula.rule.inte;

import java.util.Map;

import com.toone.itop.formula.spi.Func;
import com.toone.itop.formula.spi.Function;
import com.toone.itop.metadata.apiserver.ITenant;
import com.toone.itop.metadata.apiserver.factory.ITenantFactory;
import com.toone.itop.rule.apiserver.model.RuleContext;
import com.toone.itop.rule.spiserver.AbstractRule;
import com.toone.itop.ruleset.apiserver.util.RuleSetVariableUtil;
import com.toone.vcore.component.annotations.Component;
import com.toone.vcore.component.annotations.Instantiate;
import com.toone.vcore.component.annotations.Provides;

@Component
@Provides(specifications = Function.class)
@Instantiate
@Func(name = "EnableTenant")
public class EnableTenant extends AbstractRule {

	@Override
	public Object evaluate(RuleContext context) {
		Map<String, Object> ruleCfgParams = (Map<String, Object>) context.getRuleConfig().getConfigParams();
		Map<String, Object> runtimeParams = context.getInputParams();
		String returnValue = (String) ruleCfgParams.get("retValue");
		String currentTenantCode = (String) runtimeParams.get("currentTenantCode");
		// 查询数据
		ITenant service = ITenantFactory.getService();
		
		String result = "";
		try {
			if(service.enableTenant(currentTenantCode)) {
				result = "true";
			} else {
				result = "false";
			}
		} catch (Exception e) {
			e.printStackTrace();
			result = "false";
		}
		RuleSetVariableUtil.setVariable(context, returnValue, result);
		return result;
	}

}