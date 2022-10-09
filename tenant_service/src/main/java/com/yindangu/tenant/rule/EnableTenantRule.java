package com.yindangu.tenant.rule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yindangu.v3.business.VDS;
import com.yindangu.v3.business.jdbc.api.ITenantService;
import com.yindangu.v3.business.plugin.business.api.rule.IRule;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleContext;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleOutputVo;

/**租户生效处理规则*/
public class EnableTenantRule implements IRule  {
	private static final Logger log = LoggerFactory.getLogger(EnableTenantRule.class); 
	public static final String D_RULE_NAME = "租户生效处理";
	public static final String D_RULE_CODE = "EnableTenant";
	public static final String D_RULE_DESC = "租户生效处理,\r\n" + 
			"方法名：" + D_RULE_CODE;
	
	public static final String D_InputTenantCode="tenantCode";
	/**输出变量*/
	public static final String D_OutValue="success";
	@Override
	public IRuleOutputVo evaluate(IRuleContext context) {
		/*Map<String, Object> ruleCfgParams = (Map<String, Object>) context.getRuleConfig().getConfigParams();
		Map<String, Object> runtimeParams = context.getInputParams();
		String returnValue = (String) ruleCfgParams.get("retValue");*/
		Boolean result = Boolean.FALSE;
		String tenantCode="";
		try {
			ITenantService ts = VDS.getIntance().getTenantService();
			tenantCode = (String) context.getInput(D_InputTenantCode);
			if(tenantCode == null || tenantCode.length()==0) {
				log.error("启用租户出错,编码不能为空");
			}
			else {
				boolean en = ts.enableTenant(tenantCode);
				result = Boolean.valueOf(en);	
			}
			//ITenant service = ITenantFactory.getService();
			
		}
		catch(RuntimeException e) {
			log.error("启用租户出错-" + tenantCode,e); 
			throw e;
		}
		catch (Exception e) {
			log.error("启用租户出错:"+tenantCode,e);
			throw new RuntimeException("启用租户出错",e);
		}
		//RuleSetVariableUtil.setVariable(context, returnValue, result);
		IRuleOutputVo rs = context.newOutputVo().put(D_OutValue,result);
		return rs;
	}

}