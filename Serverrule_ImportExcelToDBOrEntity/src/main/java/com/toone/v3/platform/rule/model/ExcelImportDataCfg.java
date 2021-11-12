package com.toone.v3.platform.rule.model;

import java.util.List;

/**
 * 数据导入导出配置
 *
 * @author liangzican
 * @version 1.0
 * @Date 2016-04-26
 * @description:
 */
public class ExcelImportDataCfg {
    /**
     * 左右树的excel对应树型编码列名称
     */
    private String innerCode;

    public String getInnerCode() {
        return innerCode;
    }

    public void setInnerCode(String innerCode) {
        this.innerCode = innerCode;
    }

    //导入的文件类型，EXCEL/XML/CSV
    private String fileType;

    //导入目标表名
    private String target;

    //列信息集合
    private List<ColumnImportCfg> mapping;

    //正文起始行，从0开始
    private int dataStartRow;

    //寻找上传或者下载的解析处理类，如果不空，则使用插件类来完成导入功能。
    private String pluginName;

    //指定从哪一个控件上传的附件解析
    private String attachmentName;

    public String getFileType() {
        return fileType;
    }

    public void setFileType(String fileType) {
        this.fileType = fileType;
    }

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
    }

    public List<ColumnImportCfg> getMapping() {
        return mapping;
    }

    public void setMapping(List<ColumnImportCfg> mapping) {
        this.mapping = mapping;
    }
    /**正文起始行，从1开始(上版本的描述是0，需要验证一下)*/
    public int getDataStartRow() {
        return dataStartRow;
    }
    /**正文起始行，从1开始(上版本的描述是0，需要验证一下)*/
    public void setDataStartRow(int dataStartRow) {
        this.dataStartRow = dataStartRow;
    }

    public String getPluginName() {
        return pluginName;
    }

    public void setPluginName(String pluginName) {
        this.pluginName = pluginName;
    }

    public String getAttachmentName() {
        return attachmentName;
    }

    public void setAttachmentName(String attachmentName) {
        this.attachmentName = attachmentName;
    }
}
