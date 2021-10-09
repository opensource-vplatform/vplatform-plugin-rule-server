package com.toone.itop.formula.rule.inte;
 
import java.util.Collections;
import java.util.List;

import com.yindangu.v3.plugin.vds.reg.api.IRegisterPlugin;
import com.yindangu.v3.plugin.vds.reg.api.builder.IRuleBuilder;
import com.yindangu.v3.plugin.vds.reg.api.model.IComponentProfileVo;
import com.yindangu.v3.plugin.vds.reg.api.model.IPluginProfileVo;
import com.yindangu.v3.plugin.vds.reg.api.model.IRuleProfileVo;
import com.yindangu.v3.plugin.vds.reg.common.RegVds;

/**
 * @Author xugang
 * @Date 2021/5/27 11:23
 */
public class UpdateRecordRegister implements IRegisterPlugin {
	public static final String D_COMPONENT="Serverrule_" + UpdateRecord.D_RULE_CODE;
	/** 插件作者 */
	public final static String D_Author = "同望科技";
    /** 组织标识 */
	public final static String D_GroupId = "com.toone.v3.platform";
	
    
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
    	IPluginProfileVo pro = getRuleProfile();
        return Collections.singletonList(pro);
    }

 
    private IRuleProfileVo getRuleProfile() {
    	IRuleBuilder ruleBuilder = RegVds.getPlugin().getRulePlugin();
    	ruleBuilder.setAuthor(D_Author)
                .setCode(UpdateRecord.D_RULE_CODE)
                .setDesc(UpdateRecord.D_RULE_DESC)
                .setName(UpdateRecord.D_RULE_NAME)
                .setEntry(UpdateRecord.class)
                ;

        return ruleBuilder.build();
    }
}
