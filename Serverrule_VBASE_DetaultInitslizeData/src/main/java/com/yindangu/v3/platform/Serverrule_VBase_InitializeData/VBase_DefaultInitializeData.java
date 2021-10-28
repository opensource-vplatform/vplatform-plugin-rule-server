package com.yindangu.v3.platform.Serverrule_VBase_InitializeData;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yindangu.v3.business.VDS;
import com.yindangu.v3.business.jdbc.api.model.IColumn;
import com.yindangu.v3.business.jdbc.api.model.IDataSetMetaData;
import com.yindangu.v3.business.metadata.api.IDataObject;
import com.yindangu.v3.business.metadata.api.IDataView;
import com.yindangu.v3.business.plugin.business.api.rule.IRule;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleContext;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleOutputVo;
import com.yindangu.v3.platform.plugin.util.VdsUtils;

/**
 * 初始化数据规则
 * 
 * @author tangjy
 * @since 2021年10月15日
 */
public class VBase_DefaultInitializeData implements IRule {
	
	private enum RuleSetInput{
		annotationContext,
		annotationProps
	}

//	public enum InputParams {
//		/**
//		 * 实体名
//		 */
//		dataViewName, 
//		
//		/**
//		 * 表名
//		 */
//		tableName
//	}

	public enum OutputParams {
		isSuccess, errorMsg
	}

	private static final Logger logger = LoggerFactory.getLogger(VBase_DefaultInitializeData.class);

	public IRuleOutputVo evaluate(IRuleContext context) {

		IRuleOutputVo outputVo = context.newOutputVo();
		
		InitParam inputParam = new InitParam(context);
		String tableName = inputParam.getTableName();
		String componentCode = inputParam.getCompoenntCode();
		String ruleSetCode = inputParam.getRuleSetCode();
		IDataView dataView = inputParam.getInitDataEntity();
		
		if (VdsUtils.string.isEmpty(tableName)) {
			String errorMsg = "规则【" + VBase_DefaultInitalizeDataRegister.D_RULE_CODE + "】执行默认初始化数据失败,使用者(" + componentCode + "." + ruleSetCode + ")未定义(tableName)属性,请检查.";
			logger.error(errorMsg);
			outputVo.put(OutputParams.isSuccess.name(), false);
			outputVo.put(OutputParams.errorMsg.name(), errorMsg);
			return outputVo;
		}
		try {
			Element initDataElement = genInitDataXmlEle(dataView, tableName);
			List<String> xmls = new ArrayList<String>();
			xmls.add(initDataElement.asXML());
			VDS.getIntance().getMis().importBusinessXML(xmls);

			outputVo.put(OutputParams.isSuccess.name(), true);
			outputVo.put(OutputParams.errorMsg.name(), "");
		} catch (Exception e) {
			String errorMsg = "规则【" + VBase_DefaultInitalizeDataRegister.D_RULE_CODE + "】执行默认初始化数据出现不可预知异常，使用者："
					+ componentCode + "." + ruleSetCode + " ；表名：" + tableName + "；初始化数据：" + dataView.toJson();
			logger.error(errorMsg, e);
			outputVo.put(OutputParams.isSuccess.name(), false);
			outputVo.put(OutputParams.errorMsg.name(), errorMsg);

		}

		return outputVo;
	}

	/**
	 * 生成初始化数据XML元素
	 * 
	 * @param initDataDataView
	 * @param tableName
	 * @return
	 */
	private Element genInitDataXmlEle(IDataView initDataDataView, String tableName) {
		List<IDataObject> dataObjs = initDataDataView.select();
		IDataSetMetaData metaData = (IDataSetMetaData) initDataDataView.getMetadata();
		Collection<IColumn> columns = metaData.getColumns();

		Document XMLFile = DocumentHelper.createDocument();
		Element rootEle = XMLFile.addElement("root");
		XMLFile.setRootElement(rootEle);
		Element tableEle = rootEle.addElement("table");
		tableEle.addAttribute("name", tableName)
				.addAttribute("chineseName", "")
				.addAttribute("desc", "")
				.addAttribute("isQuery", "false")
				.addAttribute("enable", "true");
		Element recordsEle = tableEle.addElement("records");
		recordsEle.addAttribute("enable", "true").addAttribute("replace", "true");
		for (IDataObject dataObj : dataObjs) {
			Element recordEle = recordsEle.addElement("record");
			for (IColumn column : columns) {
				Element columnEle = recordEle.addElement(column.getColumnName());
				Object columValue = dataObj.get(column.getColumnName());
				String data = columValue == null ? "" : columValue.toString();
				columnEle.addCDATA(data);
			}
		}

		return rootEle;
	}
	

	
	private class InitParam {
		private String tableName;
		private String compoenntCode;
		private String ruleSetCode;
		private String instanceCode;
		private String annotationCode;
		IDataView initDataEntity;
		
		@SuppressWarnings("deprecation")
		public InitParam(IRuleContext context) {
			Set<String> inputParamKeys = context.getInputParamKeys();
			for (String inputParamKey : inputParamKeys) {
				Object object = context.getInputParams(inputParamKey);
				Object ydgObj =  VDS.getIntance().getAnnotationEngine().toone2Ygd(object);
				// 注解属性入参
				if (RuleSetInput.annotationProps.name().equalsIgnoreCase(inputParamKey)) {
					IDataView propsDV = (IDataView) ydgObj;
					List<IDataObject> propObjs = propsDV.select();
					for(IDataObject propObj : propObjs) {
						String propKey = propObj.get("propKey");
						if("tableName".equalsIgnoreCase(propKey)) {
							this.tableName = propObj.get("propValue");
							break;
						}
					}
				} else if (RuleSetInput.annotationContext.name().equalsIgnoreCase(inputParamKey)) {
					// 同望DataView转IDataView
					IDataView contextDV = (IDataView) ydgObj;
					List<IDataObject> contextObjs = contextDV.select();
					if (!VdsUtils.collection.isEmpty(contextObjs)) {
						IDataObject contextObj = contextObjs.get(0);
						this.compoenntCode = contextObj.get("belongComponentCode");
						this.ruleSetCode = contextObj.get("belongRuleSetCode");
						this.instanceCode = contextObj.get("instanceCode");
						this.annotationCode = contextObj.get("annotationCode");
					}
				} else if (ydgObj instanceof IDataView) {
					initDataEntity = (IDataView) ydgObj;
				}
			}
		}
		
		public String getTableName() {
			return tableName;
		}
		
		public String getCompoenntCode() {
			return compoenntCode;
		}
		
		public String getRuleSetCode() {
			return ruleSetCode;
		}
		
		@SuppressWarnings("unused")
		public String getInstanceCode() {
			return instanceCode;
		}
		
		@SuppressWarnings("unused")
		public String getAnnotationCode() {
			return annotationCode;
		}
		
		public IDataView getInitDataEntity() {
			return initDataEntity;
		}
	}
	
}
