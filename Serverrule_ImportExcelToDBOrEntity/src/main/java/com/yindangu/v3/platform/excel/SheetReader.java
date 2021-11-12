package com.yindangu.v3.platform.excel;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.poi.hssf.usermodel.HSSFDateUtil;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.RichTextString;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.toone.v3.platform.rule.model.FModelMapper;
import com.toone.v3.platform.rule.model.FProperty;
import com.yindangu.commons.CaseInsensitiveLinkedMap;
import com.yindangu.v3.business.jdbc.api.model.ColumnType;
import com.yindangu.v3.business.jdbc.api.model.IDataSetMetaData;
import com.yindangu.v3.business.plugin.execptions.BusinessException;
import com.yindangu.v3.business.plugin.execptions.ConfigException;
import com.yindangu.v3.business.plugin.execptions.EnviException;
import com.yindangu.v3.business.plugin.execptions.PluginException;
import com.yindangu.v3.platform.excel.SheetReaderMergedRegionVo.RegionVo; 
/**
 * 读取Sheet内容
 * @author jiqj
 *
 */
public class SheetReader{
	private static final Logger log = LoggerFactory.getLogger(SheetReader.class);
	/**读取一个Sheet内容*/
	public static class SheetReaderBuilder{
		//private Sheet sheet ;
		private int sheetIndex;
		private FModelMapper fieldMap; 
		private IDataSetMetaData table;
		private MergedType merged;
		private InputStream inputStream;
		public SheetReaderBuilder() {
			sheetIndex = -2;
		}
		
		public SheetReaderBuilder setMerged(MergedType merged) {
			this.merged = merged;
			return this;
		}/*
		public SheetReaderBuilder setSheet(Sheet sheet) {
			this.sheet = sheet;
			return this;
		}
		*/
		/**
		 * 
		 * @param sheetIndex -1表示全部
		 * @return
		 */
		public SheetReaderBuilder setSheetIndex(int sheetIndex) {
			this.sheetIndex = sheetIndex;
			return this;
		}

		public SheetReaderBuilder setFieldMap(FModelMapper mm) {
			this.fieldMap = mm;
			return this;
		}
 
		public SheetReaderBuilder setTable(IDataSetMetaData table) {
			this.table =table;
			return this;
		}
		public SheetReader builder() {
			if(inputStream == null) {
				throw new ConfigException("读取Excel数据流:Workbook.inputStream == null");
			}
			if(sheetIndex == -2) {
				throw new ConfigException("请配置读取的sheet下标，-1表示全部");
			}
			if(merged == null) { //使用默认配置
				//merged = ImportExcelConfig.getConfig().getMergedType();
				merged = MergedType.None; 
			}
			return new SheetReader(this);
		}

		public SheetReaderBuilder setInputStream(InputStream inputStream) {
			this.inputStream = inputStream;
			return this;
		}
	}
	
	private SheetReaderBuilder builder ;
	private SheetReader(SheetReaderBuilder builder) {
		this.builder = builder;
	}
	
	/**
	 * 读取一个sheet的所有内容，转为List<Map>
	 * @param sheet
	 * @return Map是不区分大小写的CaseInsensitiveLinkedMap
	 * 
	 * 关于 int rowCount = sheet.getPhysicalNumberOfRows() 与 int rowCount = sheet.getLastRowNum();<br/>
	 * {@linkplain POIExcel#readExcel(String)}、{@linkplain POIExcel#readExcelMap(InputStream)} 、{@linkplain POIExcel#readExcel(InputStream, VTable)}
	 * 使用 sheet.getPhysicalNumberOfRows()<br/>
	 * 其他的 readExcel使用了 sheet.getLastRowNum();<br/>
	 * https://blog.csdn.net/xiaozaq/article/details/54097720
	 */
	public List<Map<String,Object>> readData(){
		int sheetIndex = builder.sheetIndex;
		try {
			Workbook workbook = WorkbookFactory.create(builder.inputStream);
	        int sheetCount = workbook.getNumberOfSheets();
	        
	        List<Map<String,Object>> records;
			if(sheetIndex ==-1) { //全部
				records = new ArrayList<>();
				for(int i =0 ;i < sheetCount ;i++) {
					Sheet sheet = workbook.getSheetAt(sheetIndex);
					boolean checkHeader =(i ==0);//第一个sheet检查列头
					List<Map<String,Object>> rds = this.readSheet(sheet, checkHeader);
					records.addAll(rds);
				}
			}
			else if(sheetIndex >=0 && sheetIndex < sheetCount) { //指定
				Sheet sheet = workbook.getSheetAt(sheetIndex);
				records = this.readSheet(sheet,true);
			}
			else {
	        	throw new EnviException("sheet下标越界(sheet总数是:" + sheetCount + ",sheetIndex:" + sheetIndex + ")");
			}
			return records;
		}
	    catch (PluginException e) {
	        throw e;
	    } catch (Exception e) {
	        throw new EnviException("读取Excel出错："+e.getMessage(), e);
	    }
		finally {
			close(builder.inputStream);
		}
	}
	protected List<Map<String,Object>> readSheet(Sheet sheet,boolean checkHeader){
		int startRow = 1;
		IDataSetMetaData table = builder.table;
		
		//int rowCount = sheet.getPhysicalNumberOfRows();
		int rowEndIndex = sheet.getLastRowNum(); //最后一行下标 Count -1
		if(rowEndIndex <=0) {
			return Collections.emptyList();
		}
		
		Collection<FProperty> columns = null;
		if(builder.fieldMap != null) {
			columns = builder.fieldMap.getProperties().values();
			startRow = builder.fieldMap.getStartRow() ;
			checkColHeader(sheet, builder.fieldMap, checkHeader);
		}
		if(startRow < 1) {
			startRow = 1;
		}
		
		SheetReaderMergedRegionVo mergedRegionVo = null;
		if(builder.merged !=MergedType.None) {
			mergedRegionVo = new SheetReaderMergedRegionVo(sheet,builder.merged);
		}

		List<Map<String,Object>> record = new ArrayList<Map<String,Object>>();
		for (int rowIdx = startRow - 1; rowIdx <= rowEndIndex; rowIdx++) {
			Row row = sheet.getRow(rowIdx);
			if (row == null) {
				continue ;
			}
			
			List<FieldValue> values = new ArrayList<FieldValue>();
			if(columns == null) { //全部获取
				int cellCount = row.getLastCellNum();
				for (int col = 0; col < cellCount; col++) {
					Cell cell = row.getCell(col);
					if (cell == null) {
						continue ;
					}
					String fieldName = String.valueOf(cell.getColumnIndex());
					Object value = parseCellValue(cell, fieldName);	
					values.add(new FieldValue(fieldName, value, rowIdx, col));
				}
			}
			else { //有映射对象
				for(FProperty pm : columns) {
					int col = pm.getColumn();
					Cell cell = row.getCell(col);
					if (cell == null) {
						continue ;
					}
					String fieldName = pm.getName();
					ColumnType type = pm.getType();
					Object value = parseCellValue(cell, fieldName,type,table);

					values.add(new FieldValue(fieldName, value, rowIdx, col));
				}
			}
			
			//如果此行数据全部为空，则不需要保存
			if (values.size()>0 ) {
				Map<String,Object> rds = getMergedValue(mergedRegionVo,values);
				
				if(rds.size()>0) {
					record.add(rds);
				}
			}	
		}
		return record;
	} 
	/**
	 * 处理合并单元格
	 * @param mergedRegionVo
	 * @param values
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private Map<String,Object> getMergedValue(SheetReaderMergedRegionVo mergedRegionVo,List<FieldValue> values) {
		int emptyValueCount =0,size = values.size();
		while(emptyValueCount < size && values.get(emptyValueCount).getValue() == null) {
			emptyValueCount++;//空值个数
		}
		if(emptyValueCount == size) { //全部value都是null
			return Collections.emptyMap();
		}
		
		Map<String,Object> rds = new CaseInsensitiveLinkedMap();// NOPMD
		if(mergedRegionVo == null) { //不需要合并单元格处理
			for(FieldValue e :  values) {
				if(e.getValue()!=null) {
					rds.put(e.getKey(), e.getValue());
				}
			}
			return rds;
		}
		////////////////////////////////
		
		for(FieldValue fv :values) {//没有取得cell的值，就判断是否合并值
			Object val = fv.getValue(); 
			RegionVo regionVo = mergedRegionVo.getMergedRegion(fv.getRow(), fv.getColumn());
			if(regionVo != null) {
				if(val == null) { 
					val = regionVo.getMergedValue();
				}
				else if(regionVo.getMergedValue() == null){//合并单元格，只有第一个格是有值的
					regionVo.setMergedValue(val);
				}
				else {
					String s ="合并单元格，只有第一个格是有值的,请检查逻辑是否正确：row=" 
							+ fv.getRow()+ ",col=" + fv.getColumn() + ",第一个值:" + regionVo.getMergedValue()
							+ ",第二个值：" +val;
					throw new ConfigException(s);
				}
			}
			
			if(val!=null) {
				rds.put(fv.getKey(), val);
			}
		}
		return rds;
	}
	
	/**
	 * 没有vtable的情况
	 * @param cell
	 * @param String key = String.valueOf(cell.getColumnIndex());
	 * @return
	 */
	private Object parseCellValue(Cell cell,String key) {
		return parseCellValue(cell, key, null, null);
	}
	/**每个类实例都创建，所以可以全局*/
	private SimpleDateFormat DATE_FORMAT,DATE_TIME_FORMAT;
	private String formatDate(Date date) {
		if(DATE_FORMAT == null) {
			DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
		}
		return DATE_FORMAT.format(date);
	}
	private String formatDateTime(Date date) {
		if(DATE_TIME_FORMAT == null) {
			DATE_TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		}
		return DATE_TIME_FORMAT.format(date);
	}
	/**
	 * 
	 * 解析单元格值，返回是否为空
	 * @param cell
	 * @param fieldName 可以null，来源列名
	 * @param type 可以null,来源数据类型
	 * @param vtable 可以null, 目标数据类型
	 * @return cell转换后的值 ，表示没有取得值
	 */
	private Object parseCellValue(Cell cell, String fieldName, ColumnType sourceType ,IDataSetMetaData  metaData) {
		Object val = null;
		boolean flag ;
		/*String key =String.valueOf(cell.getColumnIndex());
		ColumnType type = null;
		if(pm!=null) {
			key = pm.getName();
			type = pm.getType();
		}*/ 
		int cellType = cell.getCellType();
		if (Cell.CELL_TYPE_NUMERIC == cellType) {
			// 判断是不是日期格式
			if (DateUtil.isCellDateFormatted(cell)) {
				if (ColumnType.Date == sourceType) {
					val = formatDate(cell.getDateCellValue());//val = DateUtils.DATE_FORMAT.format(cell.getDateCellValue());
					flag = true;
				}
				else if (ColumnType.LongDate  == sourceType) {
					val = formatDateTime(cell.getDateCellValue());//val = DateUtils.DATE_TIME_FORMAT.format(cell.getDateCellValue());
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
		else if (Cell.CELL_TYPE_STRING == cellType) {
			// 读出字符串
			String cellValue = getRichStringCellValue(cell); 
			ColumnType targetType = getColumnType(metaData, fieldName);
			flag = true;
			if(cellValue.length() ==0) {
				flag = false;//没有值
			}
			else if(ColumnType.Boolean.equals(targetType)){
				if ("是".equals(cellValue)) {
					val = Boolean.TRUE; 
				}
				else if ("否".equals(cellValue))  {
					val = Boolean.FALSE; 
				}else   {
					val = cellValue; 
				}
			}
			else if(ColumnType.Integer.equals(targetType)){
				val = new Integer(cellValue);
			}
			else if(ColumnType.Number.equals(targetType)){
				val = new Double(cellValue);
			}
			else{
				val = cellValue;
			}
		}
		else if (Cell.CELL_TYPE_FORMULA == cellType) {
			// 根据数据表数据类型来得到公式的值，读出公式的结果
			boolean readfs = true;
			if (sourceType == null) {
				readfs = false;
			}
			else if (ColumnType.Text==sourceType || ColumnType.LongText==sourceType) {
				val = getRichStringCellValue(cell);
			}
			else if (ColumnType.Boolean==sourceType) {
				val = cell.getBooleanCellValue();
			}
			else if (HSSFDateUtil.isCellDateFormatted(cell)) {  
				// 兼容处理 2020-11-09 zhangyb 当公式为Date的时候会返回匪夷所思的结果： 比如：2020/11/09 会返回 44144
                Date date = cell.getDateCellValue();
                val = formatDate(date);//DateFormatUtils.format(date, "yyyy-MM-dd");
            }
			else {
				readfs = false;
			}
			
			if(!readfs){
				try {  
					val = String.valueOf(cell.getStringCellValue());  
				} catch (IllegalStateException e) {  
					val = cell.getNumericCellValue();  
				} 
			}
			flag = true;
		}
		else if (Cell.CELL_TYPE_BOOLEAN == cellType) {// 读出布尔值
			val = Boolean.valueOf(cell.getBooleanCellValue());
			flag = true;
		}
		else if (cellType != Cell.CELL_TYPE_BLANK && cellType != Cell.CELL_TYPE_ERROR) {
			val = getRichStringCellValue(cell);//cell.getRichStringCellValue().getString().trim();
			flag = true;
		}
		else {
			flag = false;
		}
		
		Object rs = null;
		if (flag){ 
			boolean isStringType = (ColumnType.Text.equals(sourceType) || ColumnType.LongText.equals(sourceType));
			if(isStringType && val instanceof Number){
				//处理整数浮点数变成字符串时后面有.0问题
				val= number2String((Number)val);
			}
			rs =  val ;
		}
		return rs;
	}
	private ColumnType getColumnType(IDataSetMetaData meta,String columnName) {
		try {
			return (meta == null ? null : meta.getMetaColumnType(columnName));
		} 
		catch(PluginException e) {
			throw e;
		}
		catch (SQLException e) {
            throw new ConfigException("获取字段类型" + columnName + "出错", e);
		}
	}
    /**
     * 浮点数转换为字符串(大数可能是科学计数表示,如:1.21156789125679E12)
     * @param d
     * @return
     */
    private String number2String(Number d){

        String a=String.valueOf(d);
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
            return (d.doubleValue()-x==0  ? String.valueOf(x) : a);
        }

    }
	private String getRichStringCellValue(Cell cell) {
		if(cell == null) {
    		return null;
    	}
		try{
    		RichTextString rts = cell.getRichStringCellValue();
    		return rts.getString().trim();
        }
		catch (IllegalStateException e) {
			 String sheetName = cell.getSheet().getSheetName();
			 int colIndex = cell.getColumnIndex() + 1;
             int rowIndex = cell.getRowIndex() + 1;
			 throw new BusinessException("读取[" + sheetName + "]单元格内容失败,不能将数值计算结果导入到文本字段(行="+rowIndex + " 列="+colIndex + ")",e);
        }
	}
	
	/**
     * 检查excel表的表头配置，并设置列头的索引号，返回错误信息（为空表示配置正确）
     * @param sheet
     * @param mm 如果是null，返回true
     * @param showError 是否抛异常
     * @return true表示通过
     */
    private boolean checkColHeader(Sheet sheet, FModelMapper mm,boolean showError) {
    	if(mm == null) {
    		return true;
    	} 
        
    	Collection<FProperty> mps = mm.getProperties().values();
        for (int i = 0,endRows = mm.getStartRow()-1; i <endRows ; i++) {
            Row firstRow = sheet.getRow(i);
            if (firstRow == null) {
                continue;

            }
            //去掉break改成一个excel列可以对应多个字段
            for (Cell c:firstRow) { 
                String cellValue = getRichStringCellValue(c) ;// cell.getRichStringCellValue().getString();
                String trimValue = trimAllSpaceAndRF(cellValue);
                for (FProperty pm : mps ) {
                    if (pm.getColumn() == -1 &&  trimValue.equalsIgnoreCase(pm.getTitle())) {
                    	pm.setColumn(c.getColumnIndex());
                    }
                }
            }
        } 

        StringBuilder noExistColumnName = new StringBuilder();
        for (FProperty pm : mps ) {
            if (pm.getColumn() == -1) {
                if (noExistColumnName.length() > 0) {
                	noExistColumnName.append("，");
                }
                noExistColumnName.append(pm.getTitle());
            }
        }

        boolean pass = (noExistColumnName .length() ==0);
        if (!pass) {
        	String sheetName = sheet.getSheetName();
        	String errorMsg = "Excel文件的" + sheetName + "中没有发现列名:" + noExistColumnName.toString() + "，请检查！";
        	if(showError) {
        		throw new BusinessException(errorMsg);
        	}
        	else {
        		log.warn(errorMsg);
        	}
        }
        return pass;
    }
    
    /** 去掉所有空格和换行符*/
    private String trimAllSpaceAndRF(String s) {
        if (s == null || (s = s.trim()).length()==0) {
            return "";
        }
        if(s.indexOf('\r') >=0 || s.indexOf('\n') >=0) {
        	StringBuilder t = new StringBuilder(); //去掉换行符，如：aa bb\r  cc \n  dd\r\n ee，会被转换为aa bbccddee
            StringTokenizer st = new StringTokenizer(s, "\r\n");
            while (st.hasMoreTokens()) {
                t .append( st.nextToken().trim());
            }
            return t.toString();
        }
        else {
        	return s;
        }
    }
    private void close(Closeable os) {
    	if(os == null) {
    		return ;
    	}
    	try {
    		os.close();
    	}
    	catch(IOException e) {
    		log.error("",e);
    	}
    }
	private class FieldValue implements Map.Entry<String, Object>{
		private final String key;
		private Object value;
		private final int column ,row;
		public FieldValue(String key,Object value,int row,int column) {
			this.key = key;
			this.value = value;
			this.row = row;
			this.column =column;
		}
		@Override
		public String getKey() { 
			return key;
		}

		@Override
		public Object getValue() { 
			return value;
		}

		@Override
		public Object setValue(Object val) {
			Object o = this.value;
			this.value = val;
			return o;
		}
		public int getColumn() {
			return column;
		}
		public int getRow() {
			return row;
		}
		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("key:").append(key).append(",value:").append(value)
				.append(",row:").append(row).append(",column:").append(column);
			return sb.toString();
		}
	}
}
