package com.toone.v3.platform.rule.util;

import com.toone.v3.platform.rule.model.FModelMapper;
import com.toone.v3.platform.rule.model.FPOStyle;
import com.toone.v3.platform.rule.model.FProperty;
import com.yindangu.commons.CaseInsensitiveLinkedMap;
import com.yindangu.v3.business.jdbc.api.model.ColumnType;
import com.yindangu.v3.business.jdbc.api.model.IDataSetMetaData;
import com.yindangu.v3.business.metadata.api.IDataObject;
import com.yindangu.v3.business.plugin.execptions.BusinessException;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.beanutils.converters.SqlDateConverter;
import org.apache.commons.beanutils.converters.SqlTimestampConverter;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.poi.hssf.usermodel.HSSFDateUtil;
import org.apache.poi.hssf.usermodel.HSSFRichTextString;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.usermodel.XSSFRichTextString;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @Author xugang
 * @Date 2021/7/26 10:11
 */
public class FPOExcel {

    static {
        ConvertUtils.register(new UtilConverterDate(), java.util.Date.class);
        ConvertUtils.register(new SqlDateConverter(null), java.sql.Date.class);
        ConvertUtils.register(new SqlTimestampConverter(null), Timestamp.class);
    }

    private String excelVersion="xls";

    public void setExcelVersion(String excelVersion) {
        this.excelVersion = excelVersion;
    }

    public FPOExcel(String excelVersion){
        setExcelVersion(excelVersion);
    }

    public List readExcel(InputStream inputStream, FModelMapper mm, int sheetno, IDataSetMetaData dataSetMetaData) {
        if(mm==null){
            return readExcel(inputStream,dataSetMetaData);
        }
        String errorMsg = "";
        Cell cell = null;
        try {
            List result = new ArrayList();// NOPMD
            Workbook workbook = WorkbookFactory.create(inputStream);
            int startRow = mm.getStartRow();
            Sheet sheet = workbook.getSheetAt(sheetno);
            int rowCount = sheet.getLastRowNum();
            if (rowCount < 1) {
                return result;
            }
            // 检测表头
            errorMsg = checkColHeader(sheet, mm);
            if (!isEmpty(errorMsg)) {
                throw new BusinessException(errorMsg);
            }
            for (int j = startRow; j < rowCount + 1; j++) {
                Row row = sheet.getRow(j);
                if (row != null) {
                    Map propValueMap = new CaseInsensitiveLinkedMap();// NOPMD

                    for (Iterator iter = mm.getProperties().entrySet().iterator(); iter.hasNext();) {
                        Map.Entry entry = (Map.Entry) iter.next();
                        FProperty pm = (FProperty) (entry.getValue());
                        cell = row.getCell(pm.getColumn());
                        if (cell == null) {
                            continue;
                        }
                        parseCellValue(propValueMap, cell, pm,dataSetMetaData);
                    }
                    //如果此行数据全部为空，则不需要保存
                    if (propValueMap.isEmpty()) {
                        continue;
                    }
                    result.add(propValueMap);
                }
            }

            return result;
        }catch (IllegalStateException e) {
            if (cell != null) {
                int excelCol = cell.getColumnIndex() + 1;
                int excelRow = cell.getRowIndex() + 1;
                throw new BusinessException("Excel第"+sheetno+"页第" + excelRow + "行第" + excelCol + "列:" + e.getMessage());
            }
            throw new BusinessException("Excel第"+sheetno+"页：数据格式有问题："+e.getMessage(), e);
        } catch (BusinessException e) {
            throw e;
        } catch (Exception e) {
            throw new BusinessException("Excel第"+sheetno+"页：数据格式有问题："+e.getMessage(), e);
        }
    }

    /**
     * 读取Excel文件，并把文件内的数据每行组装成一个Map对象，Key为列号，Value为单元格内容
     *
     * @param inputStream
     * @param dataSetMetaData
     * @return
     */
    private List readExcel(InputStream inputStream,IDataSetMetaData dataSetMetaData) {// NOPMD
        try {
            List result = new ArrayList();// NOPMD
            Workbook workbook = WorkbookFactory.create(inputStream);
            int sheetNum = workbook.getNumberOfSheets();
            for (int i = 0; i < sheetNum; i++) {
                Sheet sheet = workbook.getSheetAt(i);
                int rowCount = sheet.getPhysicalNumberOfRows();
                for (int j = 0; j < rowCount; j++) {
                    Row row = sheet.getRow(j);
                    if (row != null) {
                        Map propValueMap = new CaseInsensitiveLinkedMap();// NOPMD

                        int cellCount = row.getLastCellNum();
                        for (int k = 0; k < cellCount; k++) {
                            Cell cell = row.getCell(k);
                            if (cell == null) {
                                continue;
                            }
                            parseCellValue(propValueMap, cell, null,dataSetMetaData);
                        }
                        //如果此行数据全部为空，则不需要保存
                        if (propValueMap.isEmpty()) {
                            continue;
                        }

                        result.add(propValueMap);
                    }
                }
            }
            return result;
        }
        catch (Exception e) {
            throw new BusinessException(e.getMessage(), e);
        }
        finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                }
                catch (IOException e) {
                }
            }
        }
    }

    /**
     * 解析单元格值，返回是否为空
     *
     * @param propValueMap
     * @param cell
     * @param pm
     * @return
     */
    private boolean parseCellValue(Map propValueMap, Cell cell, FProperty pm,IDataSetMetaData dataSetMetaData) {
        Object val = null;

        boolean flag = false;

        String key = cell.getColumnIndex() + "";
        if (pm != null) {
            key = pm.getName();
        }
        SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat DATE_TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        if (Cell.CELL_TYPE_NUMERIC == cell.getCellType()) {
            // 判断是不是日期格式
            if (DateUtil.isCellDateFormatted(cell)) {
                if (pm != null && ColumnType.Date.equals(pm.getType())) {
                    val = DATE_FORMAT.format(cell.getDateCellValue());
                    flag = true;
                }
                else if (pm != null && ColumnType.LongDate.equals(pm.getType())) {
                    val = DATE_TIME_FORMAT.format(cell.getDateCellValue());
                    flag = true;
                }
                else {
                    val = cell.getDateCellValue();
                    flag = true;
                }
            }
            else {
                // 读出数值
                val = cell.getNumericCellValue();
                flag = true;
            }
        }
        else if (Cell.CELL_TYPE_STRING == cell.getCellType()) {
            // 读出字符串
            String cellVal = null;
            try {
                if (dataSetMetaData != null && dataSetMetaData.getColumn(key).getColumnType() == ColumnType.Boolean) {
                    if (cell.getRichStringCellValue().getString().trim().equals("是")) {
                        val = Boolean.TRUE;
                        flag = true;
                    } else if (cell.getRichStringCellValue().getString().trim().equals("否")) {
                        val = Boolean.FALSE;
                        flag = true;
                    } else if (!cell.getRichStringCellValue().getString().trim().equals("")) {
                        val = cell.getRichStringCellValue().getString().trim();
                        flag = true;
                    }

                } else if (dataSetMetaData != null
                        && dataSetMetaData.getColumn(key).getColumnType() == ColumnType.Integer) {
                    cellVal = cell.getRichStringCellValue().getString().trim();
                    if(isBlank(cellVal)) {
                        val = null;
                    } else {
                        val = Integer.parseInt(cellVal);
                    }
                    flag = true;
                } else if (dataSetMetaData != null
                        && dataSetMetaData.getColumn(key).getColumnType() == ColumnType.Number) {
                    cellVal = cell.getRichStringCellValue().getString().trim();
                    if(isBlank(cellVal)) {
                        val = null;
                    } else {
                        val = Double.parseDouble(cellVal);
                    }
                    flag = true;
                } else if (!cell.getRichStringCellValue().getString().trim().equals("")) {
                    val = cell.getRichStringCellValue().getString().trim();
                    flag = true;
                }
            } catch (SQLException e) {
                throw new BusinessException("获取字段出错", e);
            } catch(NumberFormatException e) {
                throw new IllegalStateException("单元格【" + cellVal + "】格式不匹配", e);
            }

        }
        else if (Cell.CELL_TYPE_FORMULA == cell.getCellType()) {
            // 读出公式的结果
            Object object = null;
            // 根据数据表数据类型来得到公式的值
            if (pm == null || pm.getType() == null) {
                try {
                    object = String.valueOf(cell.getStringCellValue());
                } catch (IllegalStateException e) {
                    object = cell.getNumericCellValue();
                }
            } else if (ColumnType.Text.equals(pm.getType()) || ColumnType.LongText.equals(pm.getType())) {
                //	有可能抛出异常
                try{
                    object = cell.getRichStringCellValue().getString().trim();
                } catch (IllegalStateException e) {
                    throw new IllegalStateException("不能将数值计算结果导入到文本字段", e);
                }

            } else if (ColumnType.Boolean.equals(pm.getType())) {
                object = cell.getBooleanCellValue();
            } else if (HSSFDateUtil.isCellDateFormatted(cell)) {  // 2020-11-09 zhangyb 当公式为Date的时候会返回匪夷所思的结果： 比如：2020/11/09 会返回 44144
                // 兼容处理
                Date date = cell.getDateCellValue();
                object = DateFormatUtils.format(date, "yyyy-MM-dd");
            } else{
                try {
                    object = String.valueOf(cell.getStringCellValue());
                } catch (IllegalStateException e) {
                    object = cell.getNumericCellValue();
                }
            }
            val = object;
            flag = true;
        }
        else if (Cell.CELL_TYPE_BOOLEAN == cell.getCellType()) {
            // 读出布尔值
            val = Boolean.valueOf(cell.getBooleanCellValue());
            flag = true;
        }
        else if (cell.getCellType() != Cell.CELL_TYPE_BLANK && cell.getCellType() != Cell.CELL_TYPE_ERROR) {
            val = cell.getRichStringCellValue().getString().trim();
        }
        if (flag) {
            if(pm != null){
                //处理整数浮点数变成字符串时后面有.0问题
                boolean isStringType=(pm.getType().equals(ColumnType.Text) ||
                        pm.getType().equals(ColumnType.LongText) );
                if(isStringType && val instanceof Number){
                    val=number2String((Number)val);
                }
            }
            propValueMap.put(key, val);
        }
        return flag;
    }

    /**
     * 浮点数转换为字符串(大数可能是科学计数表示,如:1.21156789125679E12)
     * @param d
     * @return
     */
    private String number2String(Number d){

        String a=d+"";
        /**
         * 例如:1211567891256.79浮点数,
         * bigDecimal.toPlainString=1211567891256.7900390625,toString=1.21156789125679E12
         * 10000.00000001浮点数,bigDecimal.toPlainString=10000.00000001000080374069511890411376953125,toString=10000.00000001
         * 10000浮点数,toString=1000.0
         *
         */
        if(a.endsWith(".0"))
            return a.substring(0,a.length()-2);
        else{
            long x=d.longValue();
            if(d.doubleValue()-x==0)
                return x+"";
            return a;
        }

    }

    /**
     * 检查excel表的表头配置，并设置列头的索引号，返回错误信息（为空表示配置正确）
     *
     * @param sheet
     * @param mm
     * @return
     */
    private String checkColHeader(Sheet sheet, FModelMapper mm) {
        String errorMsg = "";
        String noExistColumnNameStr = "";

//		int endRow = mm.getStartRow();
        String sheetName = sheet.getSheetName();
        Cell cell = null;
        try {
            for (int i = 0; i < mm.getStartRow(); i++) {
                Row firstRow = sheet.getRow(i);
                if (firstRow == null) {
                    continue;

                }
                //去掉break改成一个excel列可以对应多个字段
                for (Iterator iter = firstRow.cellIterator(); iter.hasNext();) {
                    cell = (Cell) iter.next();
                    String cellValue = cell.getRichStringCellValue().getString();
                    for (Iterator iter2 = mm.getProperties().entrySet().iterator(); iter2.hasNext();) {
                        Map.Entry entry = (Map.Entry) iter2.next();
                        FProperty pm = (FProperty) (entry.getValue());
                        if (pm.getColumn() == -1 && pm.getTitle() != null
                                && pm.getTitle().equals(trimAllSpaceAndRF(cellValue))) {
                            pm.setColumn(cell.getColumnIndex());
                        }
                    }
                }

            }
        } catch (IllegalStateException e) {
            if (cell != null) {
                int colIndex = cell.getColumnIndex() + 1;
                int rowIndex = cell.getRowIndex() + 1;
                throw new BusinessException("读取单元格内容失败 行="+rowIndex + " 列="+colIndex);
            }
            throw e;
        }

        for (Iterator iter = mm.getProperties().entrySet().iterator(); iter.hasNext();) {
            Map.Entry entry = (Map.Entry) iter.next();
            FProperty pm = (FProperty) (entry.getValue());
            if (pm.getColumn() == -1) {
                if (noExistColumnNameStr.length() > 0)
                    noExistColumnNameStr += "，";
                noExistColumnNameStr += pm.getTitle();
            }
        }

        if (!isEmpty(noExistColumnNameStr))
            errorMsg = "Excel文件的" + sheetName + "中没有发现列名:" + noExistColumnNameStr + "，请检查！";
        return errorMsg;
    }

    /**
     * 写入数据到WorkBook中
     */
    public void writeExcel(List modelList, FModelMapper mm, Workbook wb,String sheetName) {
        // ModelMapper起始行参数从1开始， POI起始行从0开始
        int row = mm.getStartRow() - 1;
        Sheet sheet = wb.getSheet(sheetName);
        String titleText = mm.getTitleText();
        // 如果标题行不空，并且用户设置预留有标题行的位置
//		if (!StringUtils.isEmpty(titleText) && row - 2 >= 0) {
//			createTitleRow(sheet, titleText, row - 2, mm.getProperties().size());
//		}
        //暂时不处理没有sheet页标题的情况
        createTitleRow(sheet, titleText, 0, mm.getProperties().size());
        // 如果有表头行的位置
        if (row==2) {
            writeColumnTitle(sheet, mm); // 写单个表头
        }else if (row ==3) {
            writeColumnDoubleTitle(sheet, mm); // 写两个表头
        }
//		if (row - 1 >= 0) {
//			writeColumnTitle(sheet, mm); // 写表头
//		}
        for (Iterator it = modelList.iterator(); it.hasNext(); row++) {
            Object model = it.next();
            for (Iterator iter = mm.getProperties().entrySet().iterator(); iter.hasNext();) {
                Map.Entry entry = (Map.Entry) iter.next();
                FProperty pm = (FProperty) (entry.getValue());

                if (pm.getName() != null && pm.getName().trim().length() > 0 && pm.isNeedExport()) {
                    Object value = getPropertyValue(pm.getName(), model);
                    writeCellValue(pm.getColumn() - 1, row, value, sheet, pm.getType());
                }

                /**
                 * 梁朝辉 2015-02-13
                 * 开发平台中规则描述：如果“导出数据”不勾选，这列将会出现在Excel中，但没有数据，是空列。
                 * rtx与徐良兴沟通确定：isNeedExport这个属性应该为导出该字段的空列，而不是不导出该字段
                 *
                 * 以下为原代码：
                 *
                 * if (pm.getName() != null && pm.getName().trim().length() > 0 && pm.isNeedExport()) {
                 *		Object value = getPropertyValue(pm.getName(), model);
                 *		writeCellValue(pm.getColumn() - 1, row, value, sheet, pm.getType());
                 *	}
                 */
                // name为空不写入
                if (pm.getName() != null && pm.getName().trim().length() > 0) {
                    // 如果不需要导出数据，则把单元格的值设置为空
                    Object value = pm.isNeedExport() == true ? getPropertyValue(pm.getName(), model) : null;
                    writeCellValue(pm.getColumn() - 1, row, value, sheet, pm.getType());
                }
            }
        }
    }

    private Object getPropertyValue(String propertyName, Object object) {
        try {
            if (object instanceof IDataObject) {
                return ((IDataObject) object).get(propertyName);
            } else {
                return BeanUtils.getProperty(object, propertyName);
            }
        } catch (Exception e) {
            LoggerFactory.getLogger(getClass()).error("获取对象属性值出错。", e);
        }
        return null;
    }

    /**
     * 写表头，包含中文和英文
     *
     * @return
     */

    private int writeColumnDoubleTitle(Sheet hs, FModelMapper mm) {
        int startRow = mm.getStartRow();
        // 如果设置为不写入表头，则不写表头，直接返回。
        if (!mm.isTitle()) {
            return startRow;
        }
        int titleRow = startRow - 1; // 表头行为数据行的前一行。
        if (startRow == 1) { // 如果配置中数据行是第一行。
            // 设置表头行为第一行，数据行往下移一行。
            titleRow = 1;
            startRow = titleRow + 2;
        }
        for (Iterator iter = mm.getProperties().entrySet().iterator(); iter.hasNext();) {
            Map.Entry entry = (Map.Entry) iter.next();
            FProperty pm = (FProperty) (entry.getValue());

            /**
             * 梁朝辉 2015-02-13
             * 开发平台中规则描述：如果“导出数据”不勾选，这列将会出现在Excel中，但没有数据，是空列。
             * rtx与徐良兴沟通确定：isNeedExport这个属性应该为导出该字段的空列，而不是不导出该字段
             *
             * 以下为原代码(该代码已被注释)：
             * if(!pm.isNeedExport())
             * 		continue;
             */
            // if(!pm.isNeedExport())
            // continue;
            String cname = isEmpty(pm.getTitle())?"":pm.getTitle();//获取中文名
            String value = pm.getName();
//			if (StringUtils.isEmpty(value))
//				value = pm.getName();
            //设置中文
            writeCellValue(pm.getColumn()-1, titleRow-2, cname, hs, pm.getType());
            // 设置列宽
            hs.setColumnWidth((pm.getColumn()-1), pm.getWidth());

            //设置英文
            writeCellValue(pm.getColumn() - 1, titleRow - 1, value, hs, pm.getType());
            // 设置列宽
            hs.setColumnWidth((pm.getColumn() - 1), pm.getWidth());
            hs.getRow(0).setHeight((short) (255*2));

        }
        return startRow;
    }

    /**
     * 写表头
     *
     * @return
     */

    private int writeColumnTitle(Sheet hs, FModelMapper mm) {
        int startRow = mm.getStartRow();
        // 如果设置为不写入表头，则不写表头，直接返回。
        if (!mm.isTitle()) {
            return startRow;
        }
        int titleRow = startRow - 1; // 表头行为数据行的前一行。
        if (startRow == 1) { // 如果配置中数据行是第一行。
            // 设置表头行为第一行，数据行往下移一行。
            titleRow = 1;
            startRow = titleRow + 1;
        }
        for (Iterator iter = mm.getProperties().entrySet().iterator(); iter.hasNext();) {
            Map.Entry entry = (Map.Entry) iter.next();
            FProperty pm = (FProperty) (entry.getValue());

            /**
             * 梁朝辉 2015-02-13
             * 开发平台中规则描述：如果“导出数据”不勾选，这列将会出现在Excel中，但没有数据，是空列。
             * rtx与徐良兴沟通确定：isNeedExport这个属性应该为导出该字段的空列，而不是不导出该字段
             *
             * 以下为原代码(该代码已被注释)：
             * if(!pm.isNeedExport())
             * 		continue;
             */
            // if(!pm.isNeedExport())
            // continue;

            String value = pm.getTitle();
            if (isEmpty(value))
                value = pm.getName();
            writeCellValue(pm.getColumn() - 1, titleRow - 1, value, hs, pm.getType());
            // 设置列宽
            hs.setColumnWidth((pm.getColumn() - 1), pm.getWidth());
            hs.getRow(0).setHeight((short) (255*2));

        }
        return startRow;
    }

    // 往Excel表格中写入数据。
    private void writeCellValue(int column, int row, Object value, Sheet hs, ColumnType type) {
//		try {
        Cell cell;
        if (hs.getRow(row) == null) {
            cell = hs.createRow(row).createCell(column);

        }
        else {
            cell = hs.getRow(row).createCell(column);
        }
        writeCell(cell, value, type);
//		}
    }

    private void writeCell(Cell cell, Object value, ColumnType type) {
        if(value==null)
            value="";
        if ((ColumnType.Number==type||ColumnType.Integer==type) &&
                value instanceof Number) {
            try {
                double dou = ((Number)value).doubleValue();
                cell.setCellValue(dou);
                cell.setCellType(Cell.CELL_TYPE_NUMERIC);
            }
            catch (NumberFormatException e) {
                cell.setCellValue(getRichTextString(value.toString()));
                cell.setCellType(Cell.CELL_TYPE_STRING);
            }
        }
        else {
            cell.setCellValue(getRichTextString(value.toString()));
            cell.setCellType(Cell.CELL_TYPE_STRING);
        }

    }

    /**
     * 创建标题
     *
     * @param sheet
     * @param title
     * @param rowIndex
     */
    private void createTitleRow(Sheet sheet, String title, int rowIndex, int colCount) {
        FPOStyle poiStyle = new FPOStyle(sheet.getWorkbook());
        Row row = sheet.createRow(rowIndex);
        Cell cell = row.createCell(0);
        cell.setCellType(Cell.CELL_TYPE_STRING);
        cell.setCellStyle(poiStyle.getTitleStyle());
        cell.setCellValue(getRichTextString(title));
        sheet.addMergedRegion(new CellRangeAddress(rowIndex, rowIndex, 0, colCount - 1));
    }

    public RichTextString getRichTextString(String title){
        if(this.excelVersion.equals("xls")){
            return new HSSFRichTextString(title);
        }else{
            return new XSSFRichTextString(title);
        }

    }

    // 去掉所有空格和换行符
    public String trimAllSpaceAndRF(String s) {
        if (s == null)
            return "";
        String t = "";
        s = s.trim();
        //去掉换行符，如：aa bb\r  cc \n  dd\r\n ee，会被转换为aa bbccddee
        StringTokenizer st = new StringTokenizer(s, "\r\n");
        while (st.hasMoreTokens()) {
            t += st.nextToken().trim();
        }
        return t;
    }

    private boolean isBlank(String str) {
        if(str == null || str.trim().equals("")) {
            return true;
        }

        return false;
    }

    private boolean isEmpty(String str) {
        if(str == null || str.equals("")) {
            return true;
        }

        return false;
    }
}
