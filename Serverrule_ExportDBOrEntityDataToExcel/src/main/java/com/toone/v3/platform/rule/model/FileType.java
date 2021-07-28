package com.toone.v3.platform.rule.model;

/**
 * 导出数据格式
 *
 */
public enum FileType {
    /**
     * 导出数据格式
     */
    EXCEL("xls");

    /**
     * 默认的扩展名
     */
    private String	defaultExtension;

    private FileType(String defaultExtension) {
        this.defaultExtension = defaultExtension;
    }

    public String getDefaultExtension() {
        return defaultExtension;
    }

}

