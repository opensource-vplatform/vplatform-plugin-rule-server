package com.toone.v3.platform.rule;

import com.yindangu.v3.plugin.vds.reg.api.IRegisterPlugin;
import com.yindangu.v3.plugin.vds.reg.api.builder.IRuleBuilder;
import com.yindangu.v3.plugin.vds.reg.api.model.IComponentProfileVo;
import com.yindangu.v3.plugin.vds.reg.api.model.IPluginProfileVo;
import com.yindangu.v3.plugin.vds.reg.common.RegVds;

import java.util.Collections;
import java.util.List;

/**
 * @Author xugang
 * @Date 2021/7/23 8:53
 */
public class ExecuteVoidQueryRegister implements IRegisterPlugin {

    /**
     * 插件作者
     */
    public final static String D_Author = "同望科技";
    /**
     * 组织标识
     */
    public final static String D_GroupId = "com.toone.v3.platform";
    public final static String D_RULE_CODE = "ExecuteVoidQuery";
    public final static String D_RULE_NAME = "执行无返回值的查询";
    public final static String D_RULE_DESC = "执行无返回值的查询（如调用存储过程等），可传递查询参数\r\n" +
            "方法名：ExecuteVoidQuery";
    public final static String D_COMPONENT = "Serverrule_" + D_RULE_CODE;

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
        IPluginProfileVo ruleProfile = getRuleProfile();

        return Collections.singletonList(ruleProfile);
    }

    private IPluginProfileVo getRuleProfile() {
        IRuleBuilder ruleBuilder = RegVds.getPlugin().getRulePlugin();
        ruleBuilder.setAuthor(D_Author)
                .setCode(D_RULE_CODE)
                .setDesc(D_RULE_DESC)
                .setName(D_RULE_NAME)
                .setEntry(ExecuteVoidQuery.class)
        ;

        return ruleBuilder.build();
    }
}
