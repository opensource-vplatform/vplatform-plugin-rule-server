package com.toone.v3.platform.rule.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author xugang
 * @Date 2021/7/27 9:35
 */
public class EXportDataCfg {

    /**
     * 数据源名称(表或者查询名称)
     */
    private String			dataSource;

    /**
     * 数据源类型
     */
    private String			dataSourceType;

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
    private List<ColumnBCfg>	mapping	= new ArrayList<ColumnBCfg>();

    /**
     * 导出的文件名(可以由导出工具设置，当用户未明确指定文件名title属性时，从此处获取)
     */
    private String			defaultFileName;

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
    public String getDefaultFileName() {
        String fileName = title;
        if (isEmpty(fileName)) {
            fileName = defaultFileName;
            if (isEmpty(fileName)) {
                fileName = "文件导出";
            }
        }

        return fileName + "." + getExportFileExtension();
    }

    private boolean isEmpty(String str) {
        if(str == null || str.equals("")) {
            return true;
        }

        return false;
    }

    /**
     * 获取导出扩展名
     *
     * @return
     */
    public String getExportFileExtension() {
        return !isEmpty(exportFileExtension) ? exportFileExtension : getExportType().getDefaultExtension();
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
        return DataType.valueOf(dataSourceType.toUpperCase());
    }

    public String getDataSource() {
        return dataSource;
    }

    public void setDataSource(String dataSource) {
        this.dataSource = dataSource;
    }

    public String getDataSourceType() {
        return dataSourceType;
    }

    public void setDataSourceType(String dataSourceType) {
        this.dataSourceType = dataSourceType;
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

    public List<ColumnBCfg> getMapping() {
        return mapping;
    }

    public void setMapping(List<ColumnBCfg> mapping) {
        this.mapping = mapping;
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

    public void setDefaultFileName(String defaultFileName) {
        this.defaultFileName = defaultFileName;
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
