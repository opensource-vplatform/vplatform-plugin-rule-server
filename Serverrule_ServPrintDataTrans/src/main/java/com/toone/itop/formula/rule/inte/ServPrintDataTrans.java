package com.toone.itop.formula.rule.inte;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yindangu.v3.business.VDS;
import com.yindangu.v3.business.formula.api.IFormulaEngine;
import com.yindangu.v3.business.jdbc.api.model.IColumn;
import com.yindangu.v3.business.jdbc.api.model.IDataSetMetaData;
import com.yindangu.v3.business.metadata.api.IDAS;
import com.yindangu.v3.business.metadata.api.IDataView;
import com.yindangu.v3.business.plugin.business.api.rule.ContextVariableType;
import com.yindangu.v3.business.plugin.business.api.rule.IRule;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleContext;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleOutputVo;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleVObject;
import com.yindangu.v3.business.plugin.execptions.ConfigException;
import com.yindangu.v3.business.plugin.execptions.EnviException;
import com.yindangu.v3.business.report.apiserver.IReportService;
import com.yindangu.v3.business.vsql.apiserver.IVSQLConditions;
import com.yindangu.v3.business.vsql.apiserver.IVSQLQuery;
import com.yindangu.v3.business.vsql.apiserver.IVSQLQueryUpdate;
import com.yindangu.v3.business.vsql.apiserver.IVSQLQueryUpdate.UpdateType;
import com.yindangu.v3.platform.plugin.util.VdsUtils;
/**
 * 报表打印数据转换
 * @author jiqj
 *
 */
public class ServPrintDataTrans implements IRule{

	private static final Logger logger = LoggerFactory.getLogger(ServPrintDataTrans.class);
	public static final String D_RULE_NAME = "";
	public static final String D_RULE_CODE = "ServPrintDataTrans";
	public static final String D_RULE_DESC = "报表打印数据转换";
	
	@Override
	@SuppressWarnings({ "unchecked", "deprecation"  })
	public IRuleOutputVo evaluate(IRuleContext context) {
		//Map<String, Object> inParams = (Map<String, Object>) context.getRuleConfig().getConfigParams();
		ContextVariableType resultEntityType = getContextVariableType((String) context.getPlatformInput("resultEntityType"));
		ServPrintParamVo spv = new ServPrintParamVo();
		spv.setResultEntity((String) context.getPlatformInput("resultEntity"))
			.setResultCodeField((String)context.getPlatformInput("resultCodeField"))
			.setResultEntityType(resultEntityType)
			.setResultContentField ((String)context.getPlatformInput("resultContentField"))
			.setContinuousPrints(context.getPlatformInput("continuousPrint"))
			.setPrintType( (String)context.getPlatformInput("operType"))

			.setSelectTemplates(getTemplates((String)context.getPlatformInput("selectTemplates")))
			.setTempletes ((List<Map<String, Object>>)context.getPlatformInput("templetes"));
		
		// 解析数据映射，获取最终结果并返回
		Map<String, Map<String, IDataView>> templateMapping = parseTemplateData(spv.getTempletes(), context, spv.getSelectTemplates());

		// 获取输出实体的 DataView
		IReportService rps = VDS.getIntance().getReportService();
		IDataView resultDataView;
		if("writeReport".equals(spv.getPrintType())){
			resultDataView = rps.view(templateMapping, spv.getResultCodeField(), spv.getResultContentField(), spv.isContinuousPrint());
		}else{
			resultDataView = rps.print(templateMapping, spv.getResultCodeField(), spv.getResultContentField(), spv.isContinuousPrint());
		}
		
		// 返回结果到对应变量
		//setDataViewWithType(context, resultEntity, resultEntityType, resultDataView);
		context.getVObject().setContextObject(spv.getResultEntityType(), spv.getResultEntity(), resultDataView);

		return context.newOutputVo();
	}

	/**
	 * 解析表达式
	 * 
	 * @param 表达式
	 * @return 结果
	 */
	private List<String> getTemplates(String expression) {
		if (VdsUtils.string.isEmpty(expression)) {
			throw new ConfigException(" ===表达式不能为空！");
		}
		
		try {
			IFormulaEngine en = VDS.getIntance().getFormulaEngine();
			// 执行表达式
			Object expressObj =en.eval(expression);
			String val;
			if(expressObj==null || VdsUtils.string.isEmpty(val = expressObj.toString())){
				return Collections.emptyList();//new ArrayList<String>(); 
			}
			else { // 输出的结果
				return Arrays.asList(val.split(","));
			}
		} catch (Exception e) {
			logger.warn(expression + " ===表达式不正确！请检查！");
			throw new RuntimeException(expression + " ===表达式不正确！请检查！", e);
		} 
	}

	/**
	 * 解析模板mapping
	 * 
	 * @param templates
	 * @param context
	 * @param selectedTemplates
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private Map<String, Map<String, IDataView>> parseTemplateData(List<Map<String, Object>> templates, IRuleContext context,  List<String> selectedTemplates) {
		int templatesSize = templates.size();
		Map<String, Map<String, IDataView>> results = new LinkedHashMap<String, Map<String, IDataView>>(templatesSize);

		for (int i = 0; i < selectedTemplates.size(); i++) {
			String selectedTemplate = selectedTemplates.get(i);
			for (int j = 0; j < templatesSize; j++) {
				Map<String, Object> items = templates.get(j);
				String templateCode = (String) items.get("templeteCode");
				if(selectedTemplate.equals(templateCode)){
					List<Map<String, Object>> templeteDataSources = (List<Map<String, Object>>) items.get("templeteDataSources");
					Map<String, IDataView> tmpDataSource = parseTemplateDataSourse(templeteDataSources, context);
					results.put(templateCode, tmpDataSource);
				}
			}
			
		}

		return results;
	}

	/**
	 * 解析模板数据源
	 * 
	 * @param templates
	 * @return 模板数据
	 */
	@SuppressWarnings({ "unchecked", "deprecation" })
	private Map<String, IDataView> parseTemplateDataSourse(List<Map<String, Object>> templeteDataSources, IRuleContext context) {
		Map<String, IDataView> result = new LinkedHashMap<String, IDataView>();
		IRuleVObject vobject = context.getVObject() ;
		
		for (int i = 0; i < templeteDataSources.size(); i++) {
			Map<String, Object> tmpDataSources = templeteDataSources.get(i);

			String templateEntity = (String) tmpDataSources.get("templateEntity");
			String sourceEntity = (String) tmpDataSources.get("sourceEntity");
			String sourceEntityTypes = (String) tmpDataSources.get("sourceEntityType"); // 来源实体类型
			List<Map<String, Object>> mappingList = (List<Map<String, Object>>) tmpDataSources.get("mappingList");
			
			
			ContextVariableType sourceEntityType = getContextVariableType(sourceEntityTypes);
			IDataView sourceDataView  = (IDataView)vobject.getContextObject(sourceEntity, sourceEntityType);
			
			// 解析实体映射的值
			IDataView tmpDataView = parseEntityFieldMapping(mappingList, templateEntity, sourceDataView);

			result.put(templateEntity, tmpDataView); // 存储实体编码: 实体映射
		}

		return result;
	}

	/**
	 * @param mappingList
	 * @param templateEntity
	 * @param sourceEntityName
	 * @param sourceEntityType
	 * @param context
	 * @return
	 */
	private IDataView parseEntityFieldMapping(List<Map<String, Object>> mappingList, String templateEntity
			, IDataView sourceDataView ) {

		//IDataView sourceDataView = getDataViewWithType(context, sourceEntityName, sourceEntityType); // 来源实体的DataView
		IDataView temlateDataView = copyDataViewByMapping(sourceDataView, mappingList); // 模板的DataView

		// 真正解析实体数据,根据数据生产包含结果的 DataView
		IDataView result = appendRecord(sourceDataView, null, null, temlateDataView, mappingList, null);
		return result;
	}
	/**
	enum VariableType {
		// 活动集输出变量
		RuleSetOutput("ruleSetOutput"),
		// 活动集输入变量
		RuleSetInput("ruleSetInput"),
		// 活动集上下文变量
		RuleSetVar("ruleSetVar"),
		// 窗体实体
		Window("window");

		private String type;

		private VariableType(String type) {
			this.type = type;
		}

		public static VariableType getInstanceType(String key) {
			VariableType ret = null;
			for (VariableType type : VariableType.values()) {
				if (key.equals(type.type)) {
					ret = type;
				}
			}
			return ret;
		}
	}*/
	private ContextVariableType getContextVariableType(String resultEntityType) { 
		ContextVariableType t = ContextVariableType.getInstanceType(resultEntityType);
		if(t == null) {
			throw new EnviException("不存在的变量类型:" + resultEntityType);
		}
		return t;
	}

	/**
	 * 获取实体DataView对象
	 * 
	 * @param context
	 * @param sourceName
	 * @param sourceType
	 
	private IDataView getDataViewWithType(RuleContext context, String sourceName, String sourceType) {
		DataView sourceDV = null;
		switch (VariableType.getInstanceType(sourceType)) {
		case RuleSetInput:
			sourceDV = (DataView) RuleSetVariableUtil.getInputVariable(context, sourceName);
			break;
		case RuleSetVar:
			sourceDV = (DataView) RuleSetVariableUtil.getContextVariable(context, sourceName);
			break;
		case RuleSetOutput:
			sourceDV = (DataView) RuleSetVariableUtil.getOutputVariable(context, sourceName);
			break;
		case Window:
		default:
			throw new ExpectedException("不支持类型[" + sourceType + "]的变量值设置.");
		}
		return sourceDV;
	}* @return DataView
	 */

	/**
	 * 复制来源DataView到模板实体
	 * 
	 * @param context
	 * @param sourceName
	 * @param sourceType
	 * @return DataView
	 */
	private IDataView copyDataViewByMapping(IDataView sourceDataView, List<Map<String, Object>> fieldMapping) { 
		try {
			IDAS das = VDS.getIntance().getDas();//IMetaDataFactory.getService().das();
			IDataView descdataView = das.createDataViewByMetadata();
			IDataSetMetaData metadata = sourceDataView.getMetadata();
			
			for (int i = 0; i < fieldMapping.size(); i++) { // 这里循环映射得到列创建新列
				Map<String, Object> tmpFieldMapping = fieldMapping.get(i);
				String sourceFieldName = getFieldCode((String) tmpFieldMapping.get("sourceEntityField"));
				String descFieldName = getFieldCode((String) tmpFieldMapping.get("templateEntityField"));
				IColumn vColumn = metadata.getColumn(sourceFieldName);
				descdataView.addColumn(descFieldName, descFieldName, vColumn.getColumnType(), vColumn.getPrecision(), vColumn.getLength());
			}

			return descdataView;
		} catch (SQLException e) {
			logger.warn("创建模板实体字段有误！请检查！");
			throw new ConfigException("创建模板实体字段有误！请检查！", e);
		}

	}

	/**
	 * 获取字段名称
	 * 
	 * @param fieldName
	 * @return String
	 */
	private final String getFieldCode(String fieldName) {
		String result = fieldName;
		int pointPosition = fieldName.lastIndexOf(".");

		if (pointPosition > 0) {
			result = fieldName.substring(pointPosition + 1);
		}
		return result;
	}

	/**
	 * 赋值目标实体
	 * 
	 * @param context
	 * @param destName
	 * @param destType
	 * @param destDataView
	 
	private void setDataViewWithType(RuleContext context, String destName, String destType, DataView destDataView) {
		switch (VariableType.getInstanceType(destType)) {
		case RuleSetInput:
			RuleSetVariableUtil.setInputVariable(context, destName, destDataView);
			break;
		case RuleSetVar:
			RuleSetVariableUtil.setContextVariable(context, destName, destDataView);
			break;
		case RuleSetOutput:
			RuleSetVariableUtil.setOutputVariable(context, destName, destDataView);
			break;
		default:
			throw new ExpectedException("不支持输出类型[" + destType + "]的实体.");
		}
	}*/

	/**
	 * @param sourceView
	 * @param queryCondition
	 * @param params
	 * @param destView
	 * @param fieldMap
	 * @param tmp_params
	 * @return
	 */
	private IDataView appendRecord(IDataView sourceView, String queryCondition, Map<String, Object> params
			, IDataView destView, List<Map<String, Object>> fieldMap, Map<String, Object> tmp_params) {
		IVSQLQuery vquery = VDS.getIntance().getVSQLQuery();
		IVSQLQueryUpdate queryUpdate= vquery.newQueryUpdate();
		//VSQLQueryUpdate queryUpdate = new VSQLQueryUpdate();
		//queryUpdate.setUpdateType(VSQLConst.OpInsert);
		queryUpdate.setUpdateType(UpdateType.Insert);
		
		IVSQLConditions cdt = vquery.getVSQLConditions("( 1=1 )");
		queryUpdate.setVSqlConditions(cdt);
		//Map<String, Map<String, String>> opFields = buildOpFields(VSQLConst.OpUpdate, fieldMap, tmp_params);
		Map<String, Map<String, String>> opFields = buildOpFields(fieldMap, tmp_params);
		queryUpdate.setSetMap(opFields.get("setMap")); // item 映射
		Map<String, String> emptyMap = new HashMap<String, String>();
		queryUpdate.setSetOpMap(emptyMap);
		queryUpdate.setFieldFuncs(emptyMap);

		return destView.excuteSql(queryUpdate, params, sourceView,null);
	}

	/**
	 * 获取查询的条件
	 * 
	 * @param conditions
	 * @return 
	private IVSQLConditions getVSqlConditions(String conditions) {
		if (VdsUtils.string.isEmpty(conditions)) {
			return null;
		}
		VDS.getIntance().getVSqlParse().getVSQLConditions(conditions);
		IVSQLConditions vSqlConditions = IVSQLConditionsFactory.getService().init();
		vSqlConditions.setSqlConStr(conditions);
		vSqlConditions.setLogic(VSQLConst.LogicAnd);
		return vSqlConditions;
	}*/

	/**
	 * 要操作的字段 key=目标表字段，value=源表字段 <br>
	 * 要操作字段的更新方式：key=目标表字段，value=字段更新处理方式：（累加/覆盖）<br>
	 * 相同记录判定的字段 key=目标表字段，value=源表字段
	 * 
	 * @param fields
	 * @return
	 */
	protected Map<String, Map<String, String>> buildOpFields( List<Map<String, Object>> fieldMappings, Map<String, Object> tmp_params) {
		Map<String, Map<String, String>> result = new HashMap<String, Map<String, String>>();
		Map<String, String> setMap = new HashMap<String, String>();
		result.put("setMap", setMap);
		for (Map<String, Object> map : fieldMappings) {
			String queryField = (String) map.get("sourceEntityField");
			if (queryField.indexOf(".") != -1) {
				queryField = queryField.substring(queryField.lastIndexOf(".") + 1);
			}
			String tableField = (String) map.get("templateEntityField");
			if (tableField.indexOf(".") != -1) {
				tableField = tableField.substring(tableField.lastIndexOf(".") + 1);
			}
			if (VdsUtils.string.isEmptyAny(queryField,tableField)) {
				continue;
			}
			setMap.put(tableField, queryField);
		}
		return result;
	}
}
