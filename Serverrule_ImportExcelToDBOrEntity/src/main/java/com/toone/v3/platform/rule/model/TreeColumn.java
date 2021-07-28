package com.toone.v3.platform.rule.model;

import com.yindangu.v3.business.plugin.execptions.ConfigException;

/**
 * 树结构字段
 *
 * @author wangbin
 */
public enum TreeColumn {

    TableID("tableID"),

    TableName("tableName"),

    TypeField("type"),

    ParentField("pidField"),

    TreeCodeField("treeCodeField"),

    OrderField("orderField"),

    IsLeafField("isLeafField"),

    LevelField("levelField"),

    LeftField("leftField"),

    RightField("rightField"),

    BizCodeField("bizCodeField"),

    BizCodeFormat("bizCodeFormat"),

    BusiFilterField("busiFilterField"),

    GraphSelfRelaEdgeCode("graphSelfRelaEdgeCode");
    /* 规则和树XML配置中的属性名 */
    private String columnName;

    TreeColumn() {
        columnName = name();
    }

    TreeColumn(String columnName) {
        this.columnName = columnName;
    }

    public static TreeColumn valueOfColumnName(String columnName) {
        for (TreeColumn column : values()) {
            if (column.columnName.equalsIgnoreCase(columnName)) {
                return column;
            }
        }
        throw new ConfigException("树字段名称不正确！" + columnName);
    }

    public String columnName() {
        return columnName;
    }

}
