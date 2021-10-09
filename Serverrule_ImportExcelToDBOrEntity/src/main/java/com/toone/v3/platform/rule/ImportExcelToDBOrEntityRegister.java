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
 * @Date 2021/7/23 16:19
 */
public class ImportExcelToDBOrEntityRegister implements IRegisterPlugin {

    /**
     * 插件作者
     */
    public final static String D_Author = "同望科技";
    /**
     * 组织标识
     */
    public final static String D_GroupId = "com.toone.v3.platform";
    public final static String D_RULE_CODE = "ImportExcelToDBOrEntity";
    public final static String D_RULE_NAME = "Excel导入到数据库表或实体";
    public final static String D_RULE_DESC = "将Excel文件中的工作表数据，导入到对应的物理表/实体中（支持Excel多工作表数据导入到多个物理表/实体）；\r\n" +
            "对应关系中，Sheet索引从0开始；\r\n" +
            "映射关系中，Excel列名对应列头标题，Excel列号对应列头编码（A、B...），表达式对应解析结果；\r\n" +
            "当数据来源为空时，本规则会按中文名称去映射；\r\n" +
            "支持将父子关系列形式的树转换为定义的层级树结构。\r\n" +
            "方法名：ImportExcelToDBOrEntity";
    public final static String D_COMPONENT = "Serverrule_" + D_RULE_CODE;

    @Override
    public IComponentProfileVo getComponentProfile() {
        return RegVds.getPlugin()
                .getComponentProfile()
                .setGroupId(D_GroupId)
                .setCode(D_COMPONENT)
                .setVersion("3.10.0")
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
                .setEntry(ImportExcelToDBOrEntity.class)
        ;

        return ruleBuilder.build();
    }
}
