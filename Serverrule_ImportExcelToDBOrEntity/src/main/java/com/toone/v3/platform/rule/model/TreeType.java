package com.toone.v3.platform.rule.model;

import com.yindangu.v3.business.plugin.execptions.ConfigException;

/**
 * 平台实体树结构类型枚举
 * @OpenAPI
 */
public enum TreeType {
    /**
     * 左右树
     */
    LR(2),
    /**
     * 系统树形编码树
     */
    TC(1),
    /**
     * 业务编码树
     */
    BizCode(3),
    /**
     * 图算法自关联边的树
     */
    GraphSelfRela(4);
    private int	type;

    private TreeType(int type) {
        this.type = type;
    }

    /**
     * 获取树形编码int标识
     * @return
     */
    public int getTypeIndex() {
        return type;
    }

    /**
     * 根据树形编码获取树形编码int标识
     * @param treeType 树形编码
     * @return
     */
    public static int getTypeIndex(TreeType treeType) {
        if (treeType != null) {
            return treeType.type;
        } else {
            throw new ConfigException("树类型不明确。");
        }
    }

    /**
     * 根据树形编码int标识获取树形编码
     * @param index 树形编码int标识
     * @return
     */
    public static TreeType getTreeTypeByIndex(int index) {
        for (TreeType type : TreeType.values()) {
            if (index == type.type) {
                return type;
            }
        }
        return null;
    }

}
