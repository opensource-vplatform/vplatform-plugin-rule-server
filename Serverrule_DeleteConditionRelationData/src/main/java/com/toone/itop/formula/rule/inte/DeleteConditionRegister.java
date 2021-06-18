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
 * 删除数据库中的记录
 * @author jiqj
 *
 */
public class DeleteConditionRegister implements IRegisterPlugin  { // extends AbstractRule4Tree {
	public static final String D_COMPONENT="Serverrule_" + DeleteConditionRelationData.D_RULE_CODE;
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
                .setVersion("3.4.0")
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
                .setCode(DeleteConditionRelationData.D_RULE_CODE)
                .setDesc(DeleteConditionRelationData.D_RULE_DESC)
                .setName(DeleteConditionRelationData.D_RULE_NAME)
                .setEntry(DeleteConditionRelationData.class)
                ;

        return ruleBuilder.build();
    }
}
