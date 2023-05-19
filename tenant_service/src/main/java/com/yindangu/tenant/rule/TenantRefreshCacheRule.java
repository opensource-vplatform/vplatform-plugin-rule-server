package com.yindangu.tenant.rule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yindangu.v3.business.VDS;
import com.yindangu.v3.business.jdbc.api.ITenantService;
import com.yindangu.v3.business.plugin.business.api.rule.IRule;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleContext;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleOutputVo;

/** 刷新缓存*/
public class TenantRefreshCacheRule implements IRule  {
	private static final Logger log = LoggerFactory.getLogger(TenantRefreshCacheRule.class); 
	public static final String D_RULE_NAME = "刷新缓存";
	public static final String D_RULE_CODE = "tenantRefreshCache";
	public static final String D_RULE_DESC = "刷新租户配置的缓存,\r\n" + 
			"方法名：" + D_RULE_CODE;
	
	/**输出变量*/
	public static final String D_OutValue="success";
	@Override
	public IRuleOutputVo evaluate(IRuleContext context) {
		ITenantService ts = VDS.getIntance().getTenantService();
		ts.gogoWebCommand("com.toone.itop.tenant.config.TenantConfigRefreshCache");
		IRuleOutputVo rs = context.newOutputVo()
				.put(D_OutValue,Boolean.TRUE)
				;
		return rs;
	}

}