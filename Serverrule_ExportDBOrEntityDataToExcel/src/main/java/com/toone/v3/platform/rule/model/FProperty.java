package com.toone.v3.platform.rule.model;

import com.yindangu.v3.business.jdbc.api.model.ColumnType;

public class FProperty {

    // 属性名
    private String name;

    // 属性在Excel中所在的列号，第一列值为1
    private int column = -1;

    // 表头列名，用于写入。
    private String title;

    // 数据类型
    private ColumnType type;

    // 列宽
    private int width; // NOPMD

    //是否需要导出数据(占据一列的空位，只导出标题)
    private boolean needExport;

    public int getColumn() {
        return column;
    }

    public void setColumn(int column) {
        this.column = column;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    @Override
    public String toString() {
        return "[name=" + name + "  column=" + column + "   title=" + title + "]";
    }

    public ColumnType getType() {
        return type;
    }

    public void setType(ColumnType type) {
        this.type = type;
    }

    // NOPMD
    public int getWidth() { // NOPMD
        return width;
    }

    // NOPMD
    public void setWidth(int width) {// NOPMD
        this.width = width;
    }

    public boolean isNeedExport() {
        return needExport;
    }

    public void setNeedExport(boolean needExport) {
        this.needExport = needExport;
    }

}
