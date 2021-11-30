package com.toone.itop.formula.rule.inte;
 
import java.util.Collections;
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
public class GenerateQrCodeRegister implements IRegisterPlugin {
	public static final String D_COMPONENT="Serverrule_" + GenerateQrCode.D_RULE_CODE;
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
    	IRuleInputBuilder content = ruleBuilder.newInput()
    			.setCode("QrCodeUrl")
    			.setType(VariableType.Char)
    			.setName("转换内容")
    			.setDesc("二维码内容");
    	IRuleInputBuilder width = ruleBuilder.newInput()
    			.setCode("QrCodeWidth")
    			.setType(VariableType.Integer)
    			.setName("长度"); 

    	IRuleInputBuilder height = ruleBuilder.newInput()
    			.setCode("QrCodeHeight")
    			.setType(VariableType.Integer)
    			.setName("宽度");
    	
    	IRuleInputBuilder margin = ruleBuilder.newInput()
    			.setCode("QrMargin")
    			.setType(VariableType.Integer)
    			.setName("外边距");
    	
    	IRuleInputBuilder errlevel = ruleBuilder.newInput()
    			.setCode("QrErrorLeve")
    			.setType(VariableType.Integer)
    			.setName("容错率");
    	
    	IRuleOutputBuilder returnValue = ruleBuilder.newOutput()
    			.setCode("returnValue")
    			.setType(VariableType.Char)
    			.setName("返回值");
    	
    	ruleBuilder.setAuthor(D_Author)
                .setCode(GenerateQrCode.D_RULE_CODE)
                .setDesc(GenerateQrCode.D_RULE_DESC)
                .setName(GenerateQrCode.D_RULE_NAME)
                .setEntry(GenerateQrCode.class)
                .addInput(content.build())
                .addInput(width.build())
                .addInput(height.build())
                .addInput(margin.build())
                .addInput(errlevel.build())
                .addOutput(returnValue.build())
                ;

        return ruleBuilder.build();
    }
}
