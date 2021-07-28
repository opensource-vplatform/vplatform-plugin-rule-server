package com.toone.v3.platform.rule.model;

import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFFont;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.Workbook;

/**
 * @title:POI style
 * @description:POI style
 * @copyright:广东同望科技股份有限公司Copyright (c) 2009
 * @author：郭国兴
 * @date Jan 6, 2009 1:18:24 PM
 * @version：1.0
 */
public class FPOStyle {
    /**
     * workbook
     */
    private Workbook wb = null;
    /**
     * 主标题Font
     */
    private Font titleFont = null;
    /**
     * 副标题Font
     */
    private Font subtitleFont = null;
    /**
     * 表头Font
     */
    private Font headerFont = null;
    /**
     * 内容Font
     */
    private Font contentFont = null;
    /**
     * 金融单位Font
     */
    private Font financeFont = null;

    /**
     * 主标题Style
     */
    private CellStyle titleStyle = null;
    /**
     * 副标题Style
     */
    private CellStyle subtitleStyle = null;
    /**
     * 表头Style
     */
    private CellStyle headerStyle = null;
    /**
     * 内容Style
     */
    private CellStyle contentStyle = null;

    /**
     * 金融单位Style
     */
    private CellStyle financeStyle = null;

    public FPOStyle(Workbook wb) {
        this.wb = wb;
    }

    /**
     *
     * <description> 方法描述：获取一个标题style
     *
     * </description>
     *
     * @创建日期：Jan 6, 2009 1:53:35 PM
     * @return
     */
    public CellStyle getTitleStyle() {
        return getTitleStyle(getTitleFont());
    }

    /**
     *
     * <description> 方法描述：获取一个标题style
     *
     * </description>
     *
     * @创建日期：Jan 6, 2009 1:53:41 PM
     * @param font
     * @return
     */
    public CellStyle getTitleStyle(Font font) {
        if (titleStyle == null) {
            titleStyle = getCellStyle(HSSFCellStyle.BORDER_NONE);
            titleStyle.setAlignment(HSSFCellStyle.ALIGN_CENTER);
            titleStyle.setFont(font);
        }
        return titleStyle;
    }

    /**
     *
     * <description> 方法描述：获取一个无边框单元格style
     *
     * </description>
     *
     * @创建日期：Jan 6, 2009 1:53:48 PM
     * @return
     */
    public CellStyle getSubtitleStyle() {
        return getSubtitleStyle(getSubtitleFont());
    }

    /**
     *
     * <description> 方法描述：获取一个无边框单元格style
     *
     * </description>
     *
     * @创建日期：Jan 6, 2009 1:53:55 PM
     * @param font
     * @return
     */
    public CellStyle getSubtitleStyle(Font font) {
        if (subtitleStyle == null) {
            subtitleStyle = getCellStyle(HSSFCellStyle.BORDER_NONE);
            subtitleStyle.setAlignment(HSSFCellStyle.ALIGN_LEFT);
            subtitleStyle.setFont(font);
        }
        return subtitleStyle;
    }

    /**
     *
     * <description> 方法描述：获取一个表头style
     *
     * </description>
     *
     * @创建日期：Jan 6, 2009 1:54:01 PM
     * @return
     */
    public CellStyle getHeaderStyle() {
        return getHeaderStyle(getHeaderFont());
    }

    /**
     *
     * <description> 方法描述：获取一个表头style
     *
     * </description>
     *
     * @创建日期：Jan 6, 2009 1:54:07 PM
     * @param font
     * @return
     */
    public CellStyle getHeaderStyle(Font font) {
        if (headerStyle == null) {
            headerStyle = getCellStyle(HSSFCellStyle.BORDER_THIN);
            headerStyle.setAlignment(HSSFCellStyle.ALIGN_CENTER);
            headerStyle.setVerticalAlignment(HSSFCellStyle.VERTICAL_CENTER);
            headerStyle.setWrapText(true);
            headerStyle.setFont(font);
        }
        return headerStyle;
    }

    /**
     *
     * <description> 方法描述：获取一个内容style
     *
     * </description>
     *
     * @创建日期：Jan 6, 2009 1:54:13 PM
     * @return
     */
    public CellStyle getContentStyle() {
        return getContentStyle(getContentFont());
    }

    /**
     *
     * <description> 方法描述：获取一个内容style
     *
     * </description>
     *
     * @创建日期：Jan 6, 2009 1:54:20 PM
     * @param font
     * @return
     */
    public CellStyle getContentStyle(Font font) {
        if (contentStyle == null) {
            contentStyle = getCellStyle(HSSFCellStyle.BORDER_THIN);
            contentStyle.setAlignment(HSSFCellStyle.ALIGN_CENTER);
            contentStyle.setWrapText(true);
            contentStyle.setFont(font);
        }
        return contentStyle;
    }

    /**
     *
     * <description> 方法描述：获取一个金融单位style
     *
     * </description>
     *
     * @创建日期：Jan 6, 2009 1:54:26 PM
     * @return
     */
    public CellStyle getFinanceStyle() {
        return getFinanceStyle(getFinanceFont());
    }

    /**
     *
     * <description> 方法描述：获取一个金融单位style
     *
     * </description>
     *
     * @创建日期：Jan 6, 2009 1:54:32 PM
     * @param font
     * @return
     */
    public CellStyle getFinanceStyle(Font font) {
        if (financeStyle == null) {
            financeStyle = getCellStyle(HSSFCellStyle.BORDER_THIN);
            financeStyle.setAlignment(HSSFCellStyle.ALIGN_RIGHT);
            financeStyle.setWrapText(true);
            financeStyle.setFont(font);
        }
        return financeStyle;
    }

    /**
     *
     * <description> 方法描述：创建四边相同风格的CellStyle
     *
     * </description>
     *
     * @创建日期：Jan 6, 2009 1:54:38 PM
     * @param type
     * @return
     */
    public CellStyle getCellStyle(short type) {
        CellStyle style = this.wb.createCellStyle();
        style.setBorderBottom(type);// 下边框
        style.setBorderLeft(type);// 左边框
        style.setBorderRight(type);// 右边框
        style.setBorderTop(type);// 上边框
        return style;
    }

    private Font getContentFont() {
        if (this.contentFont == null) {
            this.contentFont = wb.createFont();
        }
        return this.contentFont;
    }

    private Font getFinanceFont() {
        if (this.financeFont == null) {
            this.financeFont = wb.createFont();
        }
        return this.financeFont;
    }

    private Font getHeaderFont() {
        if (this.headerFont == null) {
            this.headerFont = wb.createFont();
            this.headerFont.setBoldweight(HSSFFont.BOLDWEIGHT_BOLD);
        }
        return this.headerFont;
    }

    private Font getSubtitleFont() {
        if (this.subtitleFont == null) {
            this.subtitleFont = wb.createFont();
        }
        return this.subtitleFont;
    }

    private Font getTitleFont() {
        if (this.titleFont == null) {
            this.titleFont = wb.createFont();
            this.titleFont.setFontHeightInPoints((short) 18);
            this.titleFont.setBoldweight(HSSFFont.BOLDWEIGHT_BOLD);
        }
        return this.titleFont;
    }
}
