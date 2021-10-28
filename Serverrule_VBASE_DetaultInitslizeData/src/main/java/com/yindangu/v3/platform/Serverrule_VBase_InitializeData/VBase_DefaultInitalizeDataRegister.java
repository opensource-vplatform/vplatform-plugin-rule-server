package com.yindangu.v3.platform.Serverrule_VBase_InitializeData;
 
import java.util.Collections;
import java.util.List;

import com.yindangu.v3.platform.Serverrule_VBase_InitializeData.VBase_DefaultInitializeData.OutputParams;
import com.yindangu.v3.plugin.vds.reg.api.IRegisterPlugin;
import com.yindangu.v3.plugin.vds.reg.api.builder.IRuleBuilder;
import com.yindangu.v3.plugin.vds.reg.api.model.IComponentProfileVo;
import com.yindangu.v3.plugin.vds.reg.api.model.IPluginProfileVo;
import com.yindangu.v3.plugin.vds.reg.api.model.IRuleProfileVo;
import com.yindangu.v3.plugin.vds.reg.api.model.VariableType;
import com.yindangu.v3.plugin.vds.reg.common.RegVds;

/**
 * 
 * @author tangjy
 * @since   2021年10月18日
 */
public class VBase_DefaultInitalizeDataRegister implements IRegisterPlugin {

	public static final String D_RULE_NAME = "平台默认初始化数据处理";
	public static final String D_RULE_CODE = "VBASE_DefaultInitslizeData";
	public static final String D_RULE_DESC = "";
	
	public static final String D_COMPONENT="Serverrule_" + D_RULE_CODE; 
	/** 插件作者 */
	public final static String D_Author = "银弹谷";
    /** 组织标识 */
	public final static String D_GroupId = "com.yindangu.v3.platform";
	
	
    
    public IComponentProfileVo getComponentProfile() {
        return RegVds.getPlugin()
                .getComponentProfile()
                .setGroupId(D_GroupId)
                .setCode(D_COMPONENT)
                .setVersion("3.14.0")
                .build();
    }

    public List<IPluginProfileVo> getPluginProfile() {
    	IPluginProfileVo pro = getRuleProfile();
        return Collections.singletonList(pro);
    }

 
    private IRuleProfileVo getRuleProfile() {
    	IRuleBuilder ruleBuilder = RegVds.getPlugin().getRulePlugin();
    	
//    	// 输入参数-初始化数据实体
//    	IRuleBuilder.IRuleInputBuilder initDataEntityNameInputEntry = ruleBuilder.newInput()
//    			.setCode(InputParams.dataViewName.name())
//				.setName("初始化数据实体")
//				.setType(VariableType.Char);
//    	// 输入参数-初始化表参数
//    	IRuleBuilder.IRuleInputBuilder tableNameInputEntry = ruleBuilder.newInput()
//    			.setCode(InputParams.tableName.name())
//				.setName("初始化表")
//				.setType(VariableType.Char);
    	
    	// 输出参数-是否成功
    	IRuleBuilder.IRuleOutputBuilder isSuccessEntry = ruleBuilder.newOutput()
    			.setCode(OutputParams.isSuccess.name())
				.setName("是否成功")
				.setType(VariableType.Boolean);
    	// 输出参数-异常信息
    	IRuleBuilder.IRuleOutputBuilder errorMsgEntry = ruleBuilder.newOutput()
    			.setCode(OutputParams.errorMsg.name())
				.setName("异常信息")
				.setType(VariableType.Char);
    	
    	ruleBuilder.setAuthor(D_Author)
        .setCode(D_RULE_CODE)
        .setDesc(D_RULE_DESC)
        .setName(D_RULE_NAME)
        .setEntry(VBase_DefaultInitializeData.class)
//        .addInput(initDataEntityNameInputEntry.build())
//        .addInput(tableNameInputEntry.build())
        .addOutput(isSuccessEntry.build())
        .addOutput(errorMsgEntry.build());
    	
        return ruleBuilder.build();
    }
}
