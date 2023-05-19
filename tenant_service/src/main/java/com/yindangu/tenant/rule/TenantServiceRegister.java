package com.yindangu.tenant.rule;
 
import java.util.Arrays;
import java.util.List;

import com.yindangu.v3.plugin.vds.reg.api.IRegisterPlugin;
import com.yindangu.v3.plugin.vds.reg.api.builder.IRuleBuilder;
import com.yindangu.v3.plugin.vds.reg.api.builder.IRuleBuilder.IRuleInputBuilder;
import com.yindangu.v3.plugin.vds.reg.api.builder.IRuleBuilder.IRuleOutputBuilder;
import com.yindangu.v3.plugin.vds.reg.api.model.IComponentProfileVo;
import com.yindangu.v3.plugin.vds.reg.api.model.IPluginProfileVo;
import com.yindangu.v3.plugin.vds.reg.api.model.IRuleProfileVo;
import com.yindangu.v3.plugin.vds.reg.api.model.VariableType;
import com.yindangu.v3.plugin.vds.reg.common.RegVds;

/**
 * @Author xugang
 * @Date 2021/5/27 11:23
 */
public class TenantServiceRegister implements IRegisterPlugin {
	public static final String D_COMPONENT="tenant_service"  ;
	/** 插件作者 */
	public final static String D_Author = "银弹谷科技";
    /** 组织标识 */
	public final static String D_GroupId = "com.yindangu.platform";
	
    
    @Override
    public IComponentProfileVo getComponentProfile() {
        return RegVds.getPlugin()
                .getComponentProfile()
                .setGroupId(D_GroupId)
                .setCode(D_COMPONENT)
                .setVersion("3.5.0")
                .build();
    }

    @Override
    public List<IPluginProfileVo> getPluginProfile() {
    	IPluginProfileVo enableTenantPro = getEnableTenantRuleProfile();
    	IPluginProfileVo deleteTenantPro = getDeleteTenantRuleProfile();
    	IPluginProfileVo forbidTenantPro = getForbidTenantRuleProfile();
    	
    	IPluginProfileVo currentTenantPro = getCurrentTenantRuleProfile();
    	IPluginProfileVo tenantCookiePro = getTenantCookieProfile();
    	IPluginProfileVo adminLogiPro =getAdminLogiProfile();
    	IPluginProfileVo refreshCachePro = getTenantRefreshCacheRuleProfile();
    	List<IPluginProfileVo> rs = Arrays.asList(enableTenantPro
    			,deleteTenantPro
    			,forbidTenantPro
    			,currentTenantPro
    			,tenantCookiePro
    			,adminLogiPro
    			,refreshCachePro
    			);
        return rs;
    }

    /**租户生效处理规则*/
    private IRuleProfileVo getEnableTenantRuleProfile() {
    	IRuleBuilder ruleBuilder = RegVds.getPlugin().getRulePlugin();
    	ruleBuilder.setAuthor(D_Author)
                .setCode(EnableTenantRule.D_RULE_CODE)
                .setDesc(EnableTenantRule.D_RULE_DESC)
                .setName(EnableTenantRule.D_RULE_NAME)
                .setEntry(EnableTenantRule.class)
                ;
    	IRuleInputBuilder teantCodeBuild = ruleBuilder.newInput()
    		.setCode(EnableTenantRule.D_InputTenantCode)
    		.setType(VariableType.Char)
    		.setName("租户编码") ;
    	
    	IRuleOutputBuilder successBuild = ruleBuilder.newOutput()
    		.setCode(EnableTenantRule.D_OutValue)
    		.setType(VariableType.Boolean)
    		.setName("启用成功");
    		
    	
    	ruleBuilder.addInput(teantCodeBuild.build())
    		.addOutput(successBuild.build());
        return ruleBuilder.build();
    }
    /**删除租户处理规则*/
    private IRuleProfileVo getDeleteTenantRuleProfile() {
    	IRuleBuilder ruleBuilder = RegVds.getPlugin().getRulePlugin();
    	ruleBuilder.setAuthor(D_Author)
                .setCode(DeleteTenantRule.D_RULE_CODE)
                .setDesc(DeleteTenantRule.D_RULE_DESC)
                .setName(DeleteTenantRule.D_RULE_NAME)
                .setEntry(DeleteTenantRule.class)
                ;
    	IRuleInputBuilder teantCodeBuild = ruleBuilder.newInput()
    		.setCode(DeleteTenantRule.D_InputTenantCode)
    		.setType(VariableType.Char)
    		.setName("租户编码") ;
    	
    	IRuleOutputBuilder successBuild = ruleBuilder.newOutput()
    		.setCode(DeleteTenantRule.D_OutValue)
    		.setType(VariableType.Boolean)
    		.setName("删除成功");
    		
    	
    	ruleBuilder.addInput(teantCodeBuild.build())
    		.addOutput(successBuild.build());
        return ruleBuilder.build();
    }
    /**禁用租户处理规则*/
    private IRuleProfileVo getForbidTenantRuleProfile() {
    	IRuleBuilder ruleBuilder = RegVds.getPlugin().getRulePlugin();
    	ruleBuilder.setAuthor(D_Author)
                .setCode(ForbidTenantRule.D_RULE_CODE)
                .setDesc(ForbidTenantRule.D_RULE_DESC)
                .setName(ForbidTenantRule.D_RULE_NAME)
                .setEntry(ForbidTenantRule.class)
                ;
    	IRuleInputBuilder teantCodeBuild = ruleBuilder.newInput()
    		.setCode(ForbidTenantRule.D_InputTenantCode)
    		.setType(VariableType.Char)
    		.setName("租户编码") ;
    	
    	IRuleOutputBuilder successBuild = ruleBuilder.newOutput()
    		.setCode(ForbidTenantRule.D_OutValue)
    		.setType(VariableType.Boolean)
    		.setName("禁用成功");
    		
    	
    	ruleBuilder.addInput(teantCodeBuild.build())
    		.addOutput(successBuild.build());
        return ruleBuilder.build();
    }
    /**获取当前在线租户*/
    private IRuleProfileVo getCurrentTenantRuleProfile() {
    	IRuleBuilder ruleBuilder = RegVds.getPlugin().getRulePlugin();
    	ruleBuilder.setAuthor(D_Author)
                .setCode(CurrentTenantRule.D_RULE_CODE)
                .setDesc(CurrentTenantRule.D_RULE_DESC)
                .setName(CurrentTenantRule.D_RULE_NAME)
                .setEntry(CurrentTenantRule.class)
                ;
 
    	IRuleOutputBuilder outCodeBuild = ruleBuilder.newOutput()
    		.setCode(CurrentTenantRule.D_TenantCode)
    		.setType(VariableType.Char)
    		.setName("租户编号");
    	IRuleOutputBuilder outNameBuild = ruleBuilder.newOutput()
        		.setCode(CurrentTenantRule.D_TenantName)
        		.setType(VariableType.Char)
        		.setName("租户名称");	
    	
    	ruleBuilder.addOutput(outCodeBuild.build())
    		.addOutput(outNameBuild.build());
        return ruleBuilder.build();
    }
    
    /**设置当前租户到cookie*/
    private IRuleProfileVo getTenantCookieProfile() {
    	IRuleBuilder ruleBuilder = RegVds.getPlugin().getRulePlugin();
    	ruleBuilder.setAuthor(D_Author)
                .setCode(TenantCookieRule.D_RULE_CODE)
                .setDesc(TenantCookieRule.D_RULE_DESC)
                .setName(TenantCookieRule.D_RULE_NAME)
                .setEntry(TenantCookieRule.class)
                ;
 
    	IRuleInputBuilder codeBuild = ruleBuilder.newInput()
    		.setCode(TenantCookieRule.D_InputTenantCode)
    		.setType(VariableType.Char)
    		.setName("租户编号"); 
    	
    	ruleBuilder.addInput(codeBuild.build())
    		 ;
        return ruleBuilder.build();
    }
    /**设置当前租户到cookie*/
    private IRuleProfileVo getAdminLogiProfile() {
    	IRuleBuilder ruleBuilder = RegVds.getPlugin().getRulePlugin();
    	ruleBuilder.setAuthor(D_Author)
                .setCode(TenantAdminLoginRule.D_RULE_CODE)
                .setDesc(TenantAdminLoginRule.D_RULE_DESC)
                .setName(TenantAdminLoginRule.D_RULE_NAME)
                .setEntry(TenantAdminLoginRule.class)
                ;
 
    	IRuleInputBuilder codeBuild = ruleBuilder.newInput()
    		.setCode(TenantAdminLoginRule.D_Account)
    		.setType(VariableType.Char)
    		.setName("管理员账号"); 
    	IRuleInputBuilder pwdBuild = ruleBuilder.newInput()
        		.setCode(TenantAdminLoginRule.D_PWD)
        		.setType(VariableType.Char)
        		.setName("密码");
    	
    	IRuleOutputBuilder successBuild = ruleBuilder.newOutput()
        		.setCode(TenantAdminLoginRule.D_OUT_SUCCESS)
        		.setType(VariableType.Boolean)
        		.setName("是否成功");
    	IRuleOutputBuilder accIdBuild = ruleBuilder.newOutput()
        		.setCode(TenantAdminLoginRule.D_OUT_AccountId)
        		.setType(VariableType.Char)
        		.setName("管理员id");
    	
    	ruleBuilder.addInput(codeBuild.build())
    		.addInput(pwdBuild.build())
    		.addOutput(successBuild.build())
    		.addOutput(accIdBuild.build())
    		;
        return ruleBuilder.build();
    }
    /** 刷新租户缓存 */
    private IRuleProfileVo getTenantRefreshCacheRuleProfile() {
    	IRuleBuilder ruleBuilder = RegVds.getPlugin().getRulePlugin();
    	ruleBuilder.setAuthor(D_Author)
                .setCode(TenantRefreshCacheRule.D_RULE_CODE)
                .setDesc(TenantRefreshCacheRule.D_RULE_DESC)
                .setName(TenantRefreshCacheRule.D_RULE_NAME)
                .setEntry(TenantRefreshCacheRule.class)
                ;
 
    	
    	IRuleOutputBuilder successBuild = ruleBuilder.newOutput()
    		.setCode(TenantRefreshCacheRule.D_OutValue)
    		.setType(VariableType.Boolean)
    		.setName("刷新成功");
    		
    	
    	ruleBuilder.addOutput(successBuild.build());
        return ruleBuilder.build();
    }
    
    /**获取有效状态的租户
    private IRuleProfileVo getTenantListProfile() {
    	IRuleBuilder ruleBuilder = RegVds.getPlugin().getRulePlugin();
    	ruleBuilder.setAuthor(D_Author)
                .setCode(TenantListRule.D_RULE_CODE)
                .setDesc(TenantListRule.D_RULE_DESC)
                .setName(TenantListRule.D_RULE_NAME)
                .setEntry(TenantListRule.class)
                ;
 
		IEntityBuilder entryBuild = RegVds.getBuilder().getEntityProfileBuilder();
		IFieldProfileBuilder fdcode =entryBuild.newField()
				.setCode(TenantListRule.FD_CODE)
				.setName("编码")
				.setType(VariableType.Char);
		IFieldProfileBuilder fdcname = entryBuild.newField()
				.setCode(TenantListRule.FD_NAME)
				.setName("名称")
				.setType(VariableType.Char);
		
		IRuleBuilder.IRuleOutputBuilder ruleOutputEntry = ruleBuilder.newOutput()
				.setCode(TenantListRule.D_Tenants)
				.setName("租户列表")
				.setType(VariableType.Entity)
				.addField(fdcode.build())
				.addField(fdcname.build())
				;
    	
    	ruleBuilder.addOutput(codeBuild.build())
    		 ;
        return ruleBuilder.build();
    }*/
}
