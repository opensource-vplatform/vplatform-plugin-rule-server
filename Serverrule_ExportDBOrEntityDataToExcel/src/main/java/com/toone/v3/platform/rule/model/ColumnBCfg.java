package com.toone.v3.platform.rule.model;

/**
 * 数据导入导出列配置信息
 *
 * @author zhangliang
 * @Date 2012-2-21
 * @version 1.0
 * @description:
 */
public class ColumnBCfg {
    // '表名.字段名' 如果需要跳过某一列，此处设置空
    private String fieldCode;

    // ’字段中文名’字段中文名或别名对应excel
    private String excelColName;

    // "数据来源类型",表达式 Express、实体字段 Entity、系统变量 System、组件变量 Component、Excel Excel
    private String source;

    // "变量名或表达式"
    private String value;

    // 实施人员在开发系统中上传的EXCEL模板
    private String template;

    // 是否转换成PDF格式
    private String exportPDF;

    // 是否需要导出
    private boolean exportData;

    // 导出时排序方式：asc//desc//空(表示该列非排序字段)
    private String orderType;

    // 各列的排列顺序（这个顺序和各列数组的排序一样，所以实际上没有什么用处）
    private int orderNo;

    private String fieldType;

    public String getFieldType() {
        return fieldType;
    }

    public void setFieldType(String fieldType) {
        this.fieldType = fieldType;
    }

    public String getTemplate() {
        return template;
    }

    public void setTemplate(String template) {
        this.template = template;
    }

    public String getExportPDF() {
        return exportPDF;
    }

    public void setExportPDF(String exportPDF) {
        this.exportPDF = exportPDF;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getOrderType() {
        return orderType;
    }

    public void setOrderType(String orderType) {
        this.orderType = orderType;
    }

    public int getOrderNo() {
        return orderNo;
    }

    public void setOrderNo(int orderNo) {
        this.orderNo = orderNo;
    }

    public String getFieldCode() {
        return fieldCode;
    }

    public void setFieldCode(String fieldCode) {
        this.fieldCode = fieldCode;
    }

    public String getExcelColName() {
        return excelColName;
    }

    public void setExcelColName(String excelColName) {
        this.excelColName = excelColName;
    }

    public boolean isExportData() {
        return exportData;
    }

    public void setExportData(boolean exportData) {
        this.exportData = exportData;
    }

}
