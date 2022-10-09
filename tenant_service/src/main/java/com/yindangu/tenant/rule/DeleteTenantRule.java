package com.yindangu.tenant.rule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yindangu.v3.business.VDS;
import com.yindangu.v3.business.jdbc.api.ITenantService;
import com.yindangu.v3.business.plugin.business.api.rule.IRule;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleContext;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleOutputVo;

/**租户生效处理规则(禁用是直接修改了记录状态，在前端直接完成了所以没有对应的规则实现)*/
public class DeleteTenantRule implements IRule  {
	private static final Logger log = LoggerFactory.getLogger(DeleteTenantRule.class); 
	public static final String D_RULE_NAME = "删除无效效的租户";
	public static final String D_RULE_CODE = "DeleteTenant";
	public static final String D_RULE_DESC = "只能删除没有生效或者禁用的租户,\r\n" + 
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
				log.error(D_RULE_NAME + "出错,编码不能为空");
			}
			else {
				boolean en = ts.deleteTenant(tenantCode);
				result = Boolean.valueOf(en);	
			}
			//ITenant service = ITenantFactory.getService();
			
		}
		catch(RuntimeException e) {
			log.error(D_RULE_NAME + "出错-" + tenantCode,e); 
			throw e;
		}
		catch (Exception e) {
			log.error(D_RULE_NAME + "出错:"+tenantCode,e);
			throw new RuntimeException(D_RULE_NAME + "出错",e);
		}
		//RuleSetVariableUtil.setVariable(context, returnValue, result);
		IRuleOutputVo rs = context.newOutputVo().put(D_OutValue,result);
		return rs;
	}

}