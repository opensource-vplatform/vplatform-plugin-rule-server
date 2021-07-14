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
 * @Date 2021/7/12 15:35
 */
public class CallWebApiRegister implements IRegisterPlugin {

    /**
     * 插件作者
     */
    public final static String D_Author = "同望科技";
    /**
     * 组织标识
     */
    public final static String D_GroupId = "com.toone.v3.platform";
    public final static String D_RULE_CODE = "CallWebApi";
    public final static String D_RULE_NAME = "调用WebAPI";
    public final static String D_RULE_DESC = "开发系统，通过URL调用其他V平台系统WEBAPI或调用第三方API，并取得返回数据供其它规则使用。\n" +
            "当为调用WebAPI时，可以设置租户编码，若不填写，当环境为租户模式时默认取当前租户编码。\n" +
            "当为调用第三方API时，可以设置请求模式暂时只支持GET，如果返回值为复杂的数据类型（json、xml、对象等），可能需要通过二次开发的规则或函数来解析。\n" +
            "方法名：CallWebApi";
    public final static String D_COMPONENT = "Serverrule_CallWebApi";

    @Override
    public IComponentProfileVo getComponentProfile() {
        return RegVds.getPlugin()
                .getComponentProfile()
                .setGroupId(D_GroupId)
                .setCode(D_COMPONENT)
                .setVersion("5.10.0")
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
                .setEntry(CallWebApi.class)
        ;

        return ruleBuilder.build();
    }
}
