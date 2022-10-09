package com.yindangu.tenant.rule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yindangu.v3.business.VDS;
import com.yindangu.v3.business.jdbc.api.ITenantService;
import com.yindangu.v3.business.plugin.business.api.rule.IRule;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleContext;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleOutputVo;
import com.yindangu.v3.business.tenant.ITenantVo;

/**获取当前在线租户*/
public class CurrentTenantRule implements IRule  {
	private static final Logger log = LoggerFactory.getLogger(CurrentTenantRule.class); 
	public static final String D_RULE_NAME = "获取当前租户";
	public static final String D_RULE_CODE = "currentTenant";
	public static final String D_RULE_DESC = "获取当前在线租户,\r\n" + 
			"方法名：" + D_RULE_CODE;
	
	/**输出变量*/
	public static final String D_TenantCode="tenantCode", D_TenantName="tenantName";
	@Override
	public IRuleOutputVo evaluate(IRuleContext context) {
		
			ITenantService ts = VDS.getIntance().getTenantService();
			ITenantVo vo= ts.getCurrentTenantVo();
			String code =null,name=null;
			if(vo != null) {
				code = vo.getTenantCode();
				name = vo.getTenantName();
			}
		
		IRuleOutputVo rs = context.newOutputVo()
				.put(D_TenantCode,(code == null ? "":code))
				.put(D_TenantName,(name == null ? "":name))
				;
		return rs;
	}

}