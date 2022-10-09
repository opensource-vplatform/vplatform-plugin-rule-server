package com.yindangu.tenant.rule;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yindangu.v3.business.VDS;
import com.yindangu.v3.business.jdbc.api.ITenantService;
import com.yindangu.v3.business.metadata.api.IDAS;
import com.yindangu.v3.business.metadata.api.IDataView;
import com.yindangu.v3.business.plugin.business.api.rule.IRule;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleContext;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleOutputVo;
import com.yindangu.v3.platform.plugin.util.VdsUtils;

/**管理租户的管理员登陆*/
public class TenantAdminLoginRule implements IRule  {
	private static final Logger log = LoggerFactory.getLogger(TenantAdminLoginRule.class); 

	 public static final String FD_CODE="code",FD_NAME="name";
	 
	public static final String D_RULE_NAME = "管理租户的管理员登录";
	public static final String D_RULE_CODE = "admin_login";
	public static final String D_RULE_DESC = "管理租户的管理员登录(" + FD_CODE + ","+ FD_NAME +"),\r\n" + 
			"方法名：" + D_RULE_CODE;
	
	/**输入变量*/
	public static final String D_Account="account",D_PWD="pwd";
	/**输出变量*/
	public static final String D_OUT_SUCCESS="success",D_OUT_AccountId="accountId";
	@Override
	public IRuleOutputVo evaluate(IRuleContext context) {
		String account = (String)context.getInput(D_Account);
		String pwd = (String)context.getInput(D_PWD);
		/*String md5pwd = VdsUtils.crypto.getMd5().encodes(pwd);
		
		String sql = "select id from v_tenant_admin where account=:account and password_tenant=:pwd";
		Map<String,Object> pars = new HashMap<>(2);
		pars.put("account",account);
		pars.put("pwd",md5pwd);
		
		String accountId=null; {
			IDAS das = VDS.getIntance().getDas();
			IDataView dv = das.find(sql,pars);
			List<Map<String,Object>> rds = dv.getDatas();
			if(rds.size()>0) {
				Map<String,Object>  rd = rds.get(0);
				accountId = (String)rd.get("id");
			}
		}*/
		String sessionId = null;//不知道在哪里取
		ITenantService ts = VDS.getIntance().getTenantService();
		String accountId= ts.doAdminLogin(sessionId, account, pwd);
    	IRuleOutputVo vo = context.newOutputVo(); //VDS.getBuilder().getResponseBuilder();
        return vo.put(D_OUT_SUCCESS, Boolean.valueOf(accountId!=null))
        		.put(D_OUT_AccountId,accountId);
	}

}