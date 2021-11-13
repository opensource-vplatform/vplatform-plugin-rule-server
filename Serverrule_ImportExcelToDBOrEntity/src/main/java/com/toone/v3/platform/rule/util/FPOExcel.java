package com.toone.v3.platform.rule.util;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.toone.v3.platform.rule.model.FModelMapper;
import com.yindangu.v3.business.jdbc.api.model.IDataSetMetaData;
import com.yindangu.v3.business.plugin.execptions.ConfigException;
import com.yindangu.v3.platform.excel.MergedType;
import com.yindangu.v3.platform.excel.SheetReader;
import com.yindangu.v3.platform.excel.SheetReader.SheetReaderBuilder;

/**
 * @Author xugang
 * @deprecated 不在维护
 */
class FPOExcel {
	private static final Logger log = LoggerFactory.getLogger(FPOExcel.class);
	 

	/**
	 * 读取指定sheet的内容
	 * @param inputStream 不能null
	 * @param mm 允许null
	 * @param sheetno 第一个sheet是0
	 * @param dataSetMetaData 允许null
	 * @return
	 */
    public List<Map<String,Object>> readExcelSheet(InputStream inputStream, FModelMapper mm, int sheetno, IDataSetMetaData dataSetMetaData) {
    	if(sheetno <0) {
    		throw new ConfigException("sheetIndex不能小于0");
    	}
    	SheetReaderBuilder rsb = new SheetReader.SheetReaderBuilder();
        SheetReader sheetRead = rsb.setInputStream(inputStream)
        		.setTable(dataSetMetaData)
        		.setSheetIndex(sheetno)
        		.setFieldMap(mm)
        		.builder();
        List<Map<String,Object>> rds = sheetRead.readData();
        return rds;
    }
    
    public List<Map<String,Object>> readExcelSheet(SheetReaderBuilder builder) {
    	SheetReader sheetRead = builder.builder();
    	return sheetRead.readData();
    }
    
    /**
     * 读取Excel文件，并把文件内的数据每行组装成一个Map对象，Key为列号，Value为单元格内容
     * 读取所有sheet内容合并输出
     * @param inputStream
     * @param dataSetMetaData
     * @return
     */
    private List<Map<String,Object>> readSheetAll(InputStream is,FModelMapper mapper,IDataSetMetaData dataSetMetaData,MergedType type) {// NOPMD
    	SheetReaderBuilder rsb = new SheetReader.SheetReaderBuilder();
        SheetReader sheetRead = rsb.setInputStream(is)
        		.setTable(dataSetMetaData)
        		.setSheetIndex(-1)
        		.setFieldMap(mapper)
        		.builder();
        List<Map<String,Object>> rds = sheetRead.readData();
        return rds;
        
    	/*try {
        	List<Map<String,Object>> result = new ArrayList<Map<String,Object>>();// NOPMD
            Workbook workbook = WorkbookFactory.create(is);
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
                SheetReaderBuilder rsb = new SheetReader.SheetReaderBuilder();
                List<Map<String,Object>> rds = rsb.setSheet(sheet).builder().readData();
                result.addAll(rds);
            }
        	
            return result;
        }
        catch(PluginException e) {
        	throw e;
        }
        catch (Exception e) {
            throw new BusinessException("读取所有sheet内容合并输出", e);
        }
        finally {
            close(is);
        }*/
    }
  
}
