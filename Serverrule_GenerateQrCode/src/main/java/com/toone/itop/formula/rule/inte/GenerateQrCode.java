package com.toone.itop.formula.rule.inte;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
import com.yindangu.itop.qrcode.IQRCode;
import com.yindangu.itop.qrcode.QRCodeImpl;
import com.yindangu.itop.qrcode.IQRCode.QRErrorCorrectionLevel;
import com.yindangu.v3.business.VDS;
import com.yindangu.v3.business.formula.api.IFormulaEngine;
import com.yindangu.v3.business.plugin.business.api.rule.ContextVariableType;
import com.yindangu.v3.business.plugin.business.api.rule.IRule;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleContext;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleOutputVo;
import com.yindangu.v3.business.plugin.execptions.BusinessException;
import com.yindangu.v3.business.plugin.execptions.ConfigException;

/**
 * 二维码生成
 * @author jiqj 
 */
public class GenerateQrCode implements IRule{
	private static final Logger log = LoggerFactory.getLogger(GenerateQrCode.class);
	public static final String D_RULE_CODE="GenerateQrCode"; 
	public static final String D_RULE_NAME="二维码生成";
    public static final String D_RULE_DESC="根据一段文本值生成相应二维码图片到文件服务，返回二维码的文件标识ID；\r\n"
    		+ "二维码的长宽，外边距，容错率可以通过规则配置参数值决定；\r\n"
    		+ "该规则通常和扫描二维码规则配套使用。";
	@SuppressWarnings("deprecation")
	@Override
	public IRuleOutputVo evaluate(IRuleContext context) {
		//Map<String, Object> inParams = (Map) context.getRuleConfig().getConfigParams();
		// 处理后台运行参数context.getPlatformInput("QrCodeUrl")
		String qrCodeUrlExp = (String)context.getPlatformInput("QrCodeUrl");// inParams.get("QrCodeUrl");
		IFormulaEngine engine = VDS.getIntance().getFormulaEngine() ;
		String qrCodeUrl = engine.eval(qrCodeUrlExp);// FormulaEngineFactory.getFormulaEngine().eval(qrCodeUrlExp);
		
		int qrCodeWidth = getInputInt(context, "QrCodeWidth",true);//(Integer) inParams.get("QrCodeWidth");
        int qrCodeHeight = getInputInt(context, "QrCodeHeight",true);//(Integer)inParams.get("QrCodeHeight");
        String returnValue = (String)context.getPlatformInput("returnValue");//inParams.get("returnValue");
        
        if(qrCodeUrl==null){
        	throw new BusinessException("配置的转换内容为空,请配置");
        }
        if(returnValue==null){
        	throw new BusinessException("返回值设置为空,请配置");
        }
        
        int qrErrorLeve =getInputInt(context, "QrErrorLeve",false);
        int qrMargin=getInputInt(context, "QrMargin",false);
        QRErrorCorrectionLevel errorLevel = QRErrorCorrectionLevel.getLevelByValue(qrErrorLeve);
        IQRCode service=  new QRCodeImpl();
        //IQRCode service = VDS.getIntance().getQRCode();
        String fileID = service.encode(qrCodeUrl, qrCodeWidth, qrCodeHeight,qrMargin,errorLevel);
     
    	//RuleSetVariableUtil.setVariable(context, returnValue, fileID);
        String[] variableName= {""};
        ContextVariableType contextType = getVariable(returnValue, variableName);
        context.getVObject().setContextObject(contextType, variableName[0], fileID);
        IRuleOutputVo rs = context.newOutputVo();
        //rs.put(returnValue,fileID);
		return rs;
	}
	/** 参考 RuleSetVariableUtil.setVariable(context, returnValue, fileID);*/
	private ContextVariableType getVariable(String variableExpr,String[] outName) {
 
		String[] items = variableExpr.split("\\.");
		if (items.length != 2) {
			throw new ConfigException("目标活动集变量表达式不正确，无法赋值:" + variableExpr);
		}
		String scope = items[0];
		outName[0] = items[1];
		ContextVariableType rs ;
		if ("BR_IN_PARENT".equalsIgnoreCase(scope)) {
			rs = ContextVariableType.RuleSetInput;
			//RuleSetVariableUtil.setInputVariable(context, variableName, value);
		} else if ("BR_VAR_PARENT".equalsIgnoreCase(scope)) {
			//RuleSetVariableUtil.setContextVariable(context, variableName, value);
			rs = ContextVariableType.RuleSetVar;
		} else if ("BR_OUT_PARENT".equalsIgnoreCase(scope)) {
			//RuleSetVariableUtil.setOutputVariable(context, variableName, value);
			rs = ContextVariableType.RuleSetOutput;
		} else {
			throw new ConfigException("设置活动集变量失败，无法识别该变量表达式:" + variableExpr);
		}
		return rs;
	}
	
	private int getInputInt(IRuleContext context,String key,boolean showError) {
		Object o = context.getPlatformInput(key);
		if(o == null) {
			showError(showError, "请检查开发系统的配置,没有参数值:" + key);
			return -1;
		}
		
		try {
			int rs;
			if(o instanceof Number) {
				rs = ((Number) o).intValue();
			}
			else {
				rs =Integer.parseInt(o.toString());
			}
			return rs;
		}
		catch(NumberFormatException e) {
			showError(showError, "请检查开发系统的配置,参数值不能转换为整型:" + key + "=" + o);
			return -1;
		}
	}
	private void showError(boolean show,String msg) {
		if(show) {
			throw new ConfigException(msg);
		}
		else {
			log.error(msg);
		}
	}
}
