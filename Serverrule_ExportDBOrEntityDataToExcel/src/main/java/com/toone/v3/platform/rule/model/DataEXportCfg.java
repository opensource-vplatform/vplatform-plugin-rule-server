package com.toone.v3.platform.rule.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 导出数据格式 <code>
 * {
 * 	"dataName" : "my_product_query",
 * 	"dataType" : "Query",
 * 	"dsColumnMap" : [{
 * 				"chineseName" : "类别",
 * 				"fieldName" : "productType",
 * 				"needExport" : true,
 * 				"orderBy" : "asc",
 * 				"orderNo" : "2"
 * 			}],
 * 	"fileType" : "Excel",
 * 	"template" : "",
 * 	"title" : "\"产品销售情况表\"",
 * 	"dsWhere" : [{
 * 				"columnType" : "1",
 * 				"displayField" : "season(season)",
 * 				"displayValue" : "第 2 季度",
 * 				"field" : "season",
 * 				"fieldType" : "1",
 * 				"leftBracket" : null,
 * 				"logicOperation" : null,
 * 				"operation" : " = ",
 * 				"rightBracket" : null,
 * 				"value" : "第 2 季度",
 * 				"valueType" : "5"
 * 			}],
 * 	"dsQueryParam" : [{
 * 				"componentControlID" : "d38294b4c81f49eeb99f3cc56d51fded",
 * 				"queryfield" : "type",
 * 				"queryfieldValue" : "JGTextBox5",
 * 				"type" : "6"
 * 			}]
 * }
 </code>
 *
 * @author wangbin
 * @createDate 2012-2-22
 */
@SuppressWarnings("unchecked")
public class DataEXportCfg {

    /**
     * 数据源名称(表或者查询名称)
     */
    private String			dataName;

    /**
     * 数据源类型
     */
    private String			dataType;

    /**
     * 导出数据格式：缺省导出Excel格式
     */
    private String			fileType	= "Excel";

    /**
     * 模板文件Id 已作废
     */
    /*
     * @Deprecated private String template;
     */

    /**
     * 导出文件名或者导出标题
     */
    private String			title;

    /**
     * 插件类全名或者SpringBeanName
     */
    private String			pluginName;

    /**
     * 导出列
     */
    private List<ColumnCfg>	dsColumnMap	= new ArrayList<ColumnCfg>();

    /**
     * 导出的文件名(可以由导出工具设置，当用户未明确指定文件名title属性时，从此处获取)
     */
    private String			exportFileName;

    /**
     * 导出的文件扩展名，通常导出工具应该设置此值(未设置的话取类型的缺省扩展名)
     */
    private String			exportFileExtension;

    /**
     * 查询条件SQL
     */
    private String			condSql;

    /**
     * 查询参数对象
     */
    private Map				paramObject	= new HashMap();

    /**
     * 获取导出文件名
     *
     * @return
     */
    public String getExportFileName() {
        String fileName = title;
        if (isEmpty(fileName)) {
            fileName = exportFileName;
            if (isEmpty(fileName)) {
                fileName = "文件导出";
            }
        }

        return fileName + "." + getExportFileExtension();
    }

    /**
     * 获取导出扩展名
     *
     * @return
     */
    public String getExportFileExtension() {
        return !isEmpty(exportFileExtension) ? exportFileExtension : getExportType().getDefaultExtension();
    }

    private boolean isEmpty(String str) {
        if(str == null || str.equals("")) {
            return true;
        }

        return false;
    }

    /**
     * 获取导出类型
     *
     * @return
     */
    public FileType getExportType() {
        return FileType.valueOf(fileType.toUpperCase());
    }

    /**
     * 获取数据源类型
     *
     * @return
     */
    public DataType getQueryDataType() {
        return DataType.valueOf(dataType.toUpperCase());
    }

    public String getDataName() {
        return dataName;
    }

    public void setDataName(String dataName) {
        this.dataName = dataName;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public String getFileType() {
        return fileType;
    }

    public void setFileType(String fileType) {
        this.fileType = fileType;
    }

    /*
     * public String getTemplate() { return template; }
     *
     * public void setTemplate(String template) { this.template = template; }
     */
    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public List<ColumnCfg> getDsColumnMap() {
        return dsColumnMap;
    }

    public void setDsColumnMap(List<ColumnCfg> dsColumnMap) {
        this.dsColumnMap = dsColumnMap;
    }

    public String getPluginName() {
        return pluginName;
    }

    public void setPluginName(String pluginName) {
        this.pluginName = pluginName;
    }

    public void setExportFileExtension(String exportFileExtension) {
        this.exportFileExtension = exportFileExtension;
    }

    public void setExportFileName(String exportFileName) {
        this.exportFileName = exportFileName;
    }

    public String getCondSql() {
        return condSql;
    }

    public void setCondSql(String condSql) {
        this.condSql = condSql;
    }

    public Map getParamObject() {
        return paramObject;
    }

    public void setParamObject(Map paramObject) {
        this.paramObject = paramObject;
    }

}
