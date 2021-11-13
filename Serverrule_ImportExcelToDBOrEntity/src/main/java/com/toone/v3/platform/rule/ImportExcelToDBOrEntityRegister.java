package com.toone.v3.platform.rule;
 
import java.util.Arrays;
import java.util.List;

import com.yindangu.v3.platform.excel.MergedType;
import com.yindangu.v3.platform.rule.ImportExcelToDBOrEntity;
import com.yindangu.v3.platform.rule.ImportExcelToDBOrEntity2;
import com.yindangu.v3.plugin.vds.reg.api.IRegisterPlugin;
import com.yindangu.v3.plugin.vds.reg.api.builder.IEditorBuilder;
import com.yindangu.v3.plugin.vds.reg.api.builder.IRuleBuilder;
import com.yindangu.v3.plugin.vds.reg.api.model.EditorType;
import com.yindangu.v3.plugin.vds.reg.api.model.IComponentProfileVo;
import com.yindangu.v3.plugin.vds.reg.api.model.IPluginProfileVo;
import com.yindangu.v3.plugin.vds.reg.api.model.IRuleProfileVo.IReferenceProfileVo;
import com.yindangu.v3.plugin.vds.reg.api.model.IRuleProfileVo.IRuleInputProfileVo;
import com.yindangu.v3.plugin.vds.reg.api.model.VariableType;
import com.yindangu.v3.plugin.vds.reg.common.RegVds;

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
        IPluginProfileVo p1 = getRuleProfile();
        IPluginProfileVo p2 = getExcel2Profile();
        
        return Arrays.asList(p1,p2);
    }
    /**
     * toone版本
     * @return
     */
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
    /**
     * 2开版本
     * @return
     */
    private IPluginProfileVo getExcel2Profile() {
    	IRuleBuilder ruleBuilder = RegVds.getPlugin().getRulePlugin();
    	
    	IEditorBuilder editBuild = RegVds.getBuilder().getEditorBuilder();
		editBuild.setType(EditorType.Select)
				.addOption(editBuild.newOption().setLabel("不处理").setValue(MergedType.None.name()).build())
				.addOption(editBuild.newOption().setLabel("合并").setValue(MergedType.MergedALL.name()).build());
		
		IReferenceProfileVo refVo = ruleBuilder.newReference()
			.setGroupId(D_GroupId)
			.setComponentCode(D_COMPONENT)
			.setPluginCode(D_RULE_CODE)
			.build();
		
        IRuleInputProfileVo margedType = ruleBuilder.newInput()
        		.setCode(ImportExcelToDBOrEntity2.D_INPUT_MergedType)
        	.setDefault(MergedType.None.name())
        	.setName("合并单元格处理方式")
        	.setDesc("导入Excel存在合并单元格时处理方式")
        	.setType(VariableType.Char)
        	.setEditor(editBuild.build())
        	.build();
        	
        ruleBuilder.setAuthor(D_Author)
                .setCode(ImportExcelToDBOrEntity2.D_RULE_CODE)
                .setDesc("与" + D_RULE_CODE + "的区别多了些扩展功能(合并、返回sheet名称等，并且可以2次开发)")
                .setName(ImportExcelToDBOrEntity2.D_RULE_NAME)
                .setEntry(ImportExcelToDBOrEntity2.class)
                .addInput(margedType) //合并单元格处理方式
                .setReference(refVo)
        ;

        return ruleBuilder.build();
    }
}
