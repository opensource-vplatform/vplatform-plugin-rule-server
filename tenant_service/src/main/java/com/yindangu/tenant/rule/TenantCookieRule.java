package com.yindangu.tenant.rule;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yindangu.v3.business.VDS;
import com.yindangu.v3.business.jdbc.api.ITenantService;
import com.yindangu.v3.business.plugin.business.api.rule.IRule;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleContext;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleOutputVo;
import com.yindangu.v3.business.tenant.ITenantVo;
import com.yindangu.v3.business.tenant.TenantStatusType;

/**设置当前租户到cookie*/
public class TenantCookieRule implements IRule  {
	private static final Logger log = LoggerFactory.getLogger(TenantCookieRule.class); 
	public static final String D_RULE_NAME = "设置当前租户到cookie";
	public static final String D_RULE_CODE = "setTenantCookie";
	public static final String D_RULE_DESC = "设置当前在线租户,\r\n" + 
			"方法名：" + D_RULE_CODE;
	
	/**输入变量*/
	public static final String D_InputTenantCode="tenantCode";
	//public static final String D_Success ="success";
	@Override
	public IRuleOutputVo evaluate(IRuleContext context) {
		Boolean success = Boolean.FALSE;
		try {
			ITenantService ts = VDS.getIntance().getTenantService();
			String tenantCode = (String) context.getInput(D_InputTenantCode);
			String msg ;
			if(tenantCode==null || tenantCode.length()==0) {//清除
				//ts.setTenantCookie(null);
				msg = null;
			}
			else {
				msg = checkTenantCode(ts,tenantCode);
			}
			if(msg == null) {
				ts.setTenantCookie(tenantCode);
				success = Boolean.TRUE;	
			}
			else {
				log.error(msg);
			}
		}
		catch(Exception e) {
			log.error("获取当前在线租户",e);
		}
		
		IRuleOutputVo rs = context.newOutputVo()
				.put(success)
				;
		return rs;
	}
	/** 
	 * 检查租户编码是否有效
	 * @param tenantCode
	 * @return 返回null 表示通过
	 */
	private String checkTenantCode(ITenantService ts,String tenantCode) {
		String msg = null;
		ITenantVo model = null; 
		List<ITenantVo> tenants = ts.findTenants(tenantCode,null);
		int size = tenants.size();
		if(size ==1) {
			model = tenants.get(0);
		}
		else if(size==0){
			msg=("获取当前在线租户不存在："+ tenantCode);
		}
		else {
			msg=("获取当前在线租户不唯一："+ tenantCode + ",size = "+size);
		}
 
		if(model!=null) {
			if(model.getTenantStatus() == TenantStatusType.valid) {
				msg=null;
			}
			else {
				msg=("设置当前在线租户状态未生效："+ model.getTenantStatus());
			}
		}
		return msg;
	}
}

