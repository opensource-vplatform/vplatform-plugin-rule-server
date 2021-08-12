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
 * @Date 2021/7/12 10:36
 */
public class ServerRestoreXMLOrJSONRegister implements IRegisterPlugin {

    /**
     * 插件作者
     */
    public final static String D_Author = "同望科技";
    /**
     * 组织标识
     */
    public final static String D_GroupId = "com.toone.v3.platform";
    public final static String D_RULE_CODE = "ServerRestoreXMLOrJSON";
    public final static String D_RULE_NAME = "配置数据还原";
    public final static String D_RULE_DESC = "规则说明：\n" +
            "元素名值规范\n" +
            "1. 元素名可以含字母、数字以及其他的字符\n" +
            "2. 元素名不能以数字或者标点符号开始\n" +
            "3. 元素名不能包含空格\n" +
            "用于依据界面数据产生通用的XML/JSON格式数据\n" +
            "方法名：ServerGenerateXMLOrJSON";
    public final static String D_COMPONENT = "Serverrule_RestoreXMLOrJSON";

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
                .setEntry(ServerRestoreXMLOrJSON.class)
        ;

        return ruleBuilder.build();
    }
}
