package com.toone.v3.platform.rule.model;

import java.util.Map;

/**
 * Model映射
 *
 * @author huj
 */
@SuppressWarnings({"rawtypes"})
public class FModelMapper {
    /**
     * Model ID
     */
    private String id;

    /**
     * Model对应的类命中径名
     */
    private String className;

    /**
     * 数据开始行，第一行值为1
     */
    private int startRow;

    /**
     * 导出时是否要写表头。默认为true.
     */
    private boolean title = true;

    /**
     * 属性名为key,PropertyMapper为value
     */
    private Map properties;

    /**
     * 表格标题
     */
    private String titleText;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public Map getProperties() {
        return properties;
    }

    public void setProperties(Map propertiesMap) {
        this.properties = propertiesMap;
    }

    public int getStartRow() {
        return startRow;
    }

    public void setStartRow(int startRow) {
        this.startRow = startRow;
    }

    public boolean isTitle() {
        return title;
    }

    public void setTitle(boolean title) {
        this.title = title;
    }

    public String getTitleText() {
        return titleText;
    }

    public void setTitleText(String titleText) {
        this.titleText = titleText;
    }

}
