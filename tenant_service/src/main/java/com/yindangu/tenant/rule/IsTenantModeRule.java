package com.yindangu.tenant.rule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yindangu.v3.business.VDS;
import com.yindangu.v3.business.jdbc.api.ITenantService;
import com.yindangu.v3.business.plugin.business.api.func.IFuncContext;
import com.yindangu.v3.business.plugin.business.api.func.IFuncOutputVo;
import com.yindangu.v3.business.plugin.business.api.func.IFunction;
/**
 * 当前是否多租户模式
 * @deprecated 不建议使用，为了兼容
 * @author jiqj
 *
 */
public class IsTenantModeRule  implements IFunction  {
	private static final Logger log = LoggerFactory.getLogger(IsTenantModeRule.class); 
	public static final String D_RULE_NAME = "当前是否多租户模式";
	public static final String D_RULE_CODE = "isTenantMode";
	
	public static final String D_RULE_DESC = "判断是否多租户模式";
	

	@Override
	public IFuncOutputVo evaluate(IFuncContext context) {
		ITenantService ts = VDS.getIntance().getTenantService();
		boolean b = ts.isTenantModel();
		IFuncOutputVo rs = context.newOutputVo()
				.put(Boolean.valueOf(b))
				;
		return rs;
	}
}
