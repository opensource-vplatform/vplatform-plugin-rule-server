package com.yindangu.v3.platform.Serverrule_VBase_InitializeData;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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

/**
 * 初始化数据规则
 * 
 * @author tangjy
 * @since 2021年10月15日
 */
public class VBase_InitializeData implements IRule {

	public enum InputParams {
		/**
		 * 实体名
		 */
		dataViewName, 
		
		/**
		 * 表名
		 */
		tableName
	}

	public enum OutputParams {
		isSuccess, errorMsg
	}

	private static final Logger logger = LoggerFactory.getLogger(VBase_InitializeData.class);

	public IRuleOutputVo evaluate(IRuleContext context) {
		IRuleOutputVo outputVo = context.newOutputVo();
		// 表名
		String tableName = (String) context.getInput(InputParams.tableName.name());
		String dataViewName = (String) context.getInput(InputParams.dataViewName.name());

		try {
			Object runtimeParams = VDS.getIntance().getFormulaEngine().eval(dataViewName);
			if (runtimeParams instanceof IDataView) {
				IDataView dataView = (IDataView) runtimeParams;
				Element initDataElement = genInitDataXmlEle(dataView, tableName);
				List<String> xmls = new ArrayList<String>();
				xmls.add(initDataElement.asXML());
				VDS.getIntance().getMis().importBusinessXML(xmls);

				outputVo.put(OutputParams.isSuccess.name(), true);
				outputVo.put(OutputParams.errorMsg.name(), "");
			} else {
				logger.error("规则【" + VBase_InitalizeDataRegister.D_RULE_CODE + "】获取初始化数据实体发生错误，获取不到对应的实体变量，请注意需要加上变量范围前缀，实体：" + dataViewName);
				outputVo.put(OutputParams.isSuccess.name(), false);
				outputVo.put(OutputParams.errorMsg.name(), "获取初始化数据实体发生错误，获取不到对应的实体变量，请注意需要加上变量范围前缀，实体：" + dataViewName);

			}
		}catch(Exception e) {
			logger.error("规则【" + VBase_InitalizeDataRegister.D_RULE_CODE + "】初始化数据出现异常：" + e.getMessage());
			outputVo.put(OutputParams.isSuccess.name(), false);
			outputVo.put(OutputParams.errorMsg.name(), "初始化数据出现异常：" + e.getMessage());
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

}
