package com.toone.v3.platform.rule.util;

import com.toone.v3.platform.rule.model.FModelMapper;
import com.toone.v3.platform.rule.model.FProperty;
import com.yindangu.commons.CaseInsensitiveLinkedMap;
import com.yindangu.v3.business.jdbc.api.model.ColumnType;
import com.yindangu.v3.business.jdbc.api.model.IDataSetMetaData;
import com.yindangu.v3.business.plugin.execptions.BusinessException;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.poi.hssf.usermodel.HSSFDateUtil;
import org.apache.poi.ss.usermodel.*;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @Author xugang
 * @Date 2021/7/26 10:11
 */
public class FPOExcel {

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
