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
 * @Date 2021/7/27 9:30
 */
public class ExportDBOrEntityDataToExcelRegister implements IRegisterPlugin {

    /**
     * 插件作者
     */
    public final static String D_Author = "同望科技";
    /**
     * 组织标识
     */
    public final static String D_GroupId = "com.toone.v3.platform";
    public final static String D_RULE_CODE = "ExportDBOrEntityDataToExcel";
    public final static String D_RULE_NAME = "导出数据库是实体数据到Excel";
    public final static String D_RULE_DESC = "将表、查询或实体的数据导出到Excel文件中（支持多数据源导出到多个Excel工作表）；\r\n" +
            "允许设置字段对应的Excel列名，当列名未设置，则显示字段编码；\r\n" +
            "列头格式：1.仅显示Excel列名；2.首行Excel列名，次行字段编码；\r\n" +
            "映射关系中，若不勾选“导出数据”，则该列将会出现在Excel中，但不包含数据，是空列。\r\n" +
            "导出的数据，不能超出Excel单元格最大字符数限制（32767）\r\n" +
            "文件类型：当数据量可能超过65536行时，请使用xlsx文件类型。\r\n" +
            "方法名：ExportDBOrEntityDataToExcel";
    public final static String D_COMPONENT = "Serverrule_ExportDBOrEntityDataToExcel";

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
        IPluginProfileVo ruleProfile = getRuleProfile();

        return Collections.singletonList(ruleProfile);
    }

    private IPluginProfileVo getRuleProfile() {
        IRuleBuilder ruleBuilder = RegVds.getPlugin().getRulePlugin();
        ruleBuilder.setAuthor(D_Author)
                .setCode(D_RULE_CODE)
                .setDesc(D_RULE_DESC)
                .setName(D_RULE_NAME)
                .setEntry(ExportDBOrEntityDataToExcel.class)
        ;

        return ruleBuilder.build();
    }
}
