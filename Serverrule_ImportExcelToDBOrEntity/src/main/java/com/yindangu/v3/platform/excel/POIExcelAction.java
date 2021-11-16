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
import com.yindangu.v3.platform.excel.MergedRegionVo.RegionVo;
import com.yindangu.v3.platform.plugin.util.VdsUtils; 
/**
 * 读取Sheet内容
 * @author jiqj
 *
 */
public class POIExcelAction{
	private static final Logger log = LoggerFactory.getLogger(POIExcelAction.class);
	/**读取一个Sheet内容*/
	public static class ReaderBuilder{
		//private Sheet sheet ;
		private int sheetIndex;
		private FModelMapper fieldMap; 
		private IDataSetMetaData table;
		private MergedType merged;
		private InputStream inputStream;
		public ReaderBuilder() {
			sheetIndex = -2;
		}
		
		public ReaderBuilder setMerged(MergedType merged) {
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
		public ReaderBuilder setSheetIndex(int sheetIndex) {
			this.sheetIndex = sheetIndex;
			return this;
		}

		public ReaderBuilder setFieldMap(FModelMapper mm) {
			this.fieldMap = mm;
			return this;
		}
 
		public ReaderBuilder setTable(IDataSetMetaData table) {
			this.table =table;
			return this;
		}
		public POIExcelAction builder() {
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
			return new POIExcelAction(this);
		}

		public ReaderBuilder setInputStream(InputStream inputStream) {
			this.inputStream = inputStream;
			return this;
		}
	}
	
	private ReaderBuilder builder ;
	private POIExcelAction(ReaderBuilder builder) {
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
	 * @return 返回记录数（取得数据需要通过 {@linkplain #getExcelRecords()}方法）
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
			//close(builder.inputStream); //谁打开谁关闭
		}
	}
	/**必须保证 rowIndex 是有序的*/
	private List<ExcelRowVo> excelRows; 
	
	private String firstSheetName;
	/** 返回第一个sheetName*/
	public String getSheetName() {
		return firstSheetName;
	}
	/**设置第一个sheetName*/
	private void setFirstSheetName(String sheetName) {
		if(VdsUtils.string.isEmpty(firstSheetName)) {
			firstSheetName = sheetName ;
		}
	}
	private MergedRegionVo<ExcelCellVo> mergedRegionVo ;
	/**所有的合并行列*/
	private MergedRegionVo<ExcelCellVo> getMergedRegionVo() {
		return mergedRegionVo;
	}
	private void setMergedRegionVo(MergedRegionVo<ExcelCellVo> vo) {
		mergedRegionVo = vo;
	}
	
	protected List<Map<String,Object>> readSheet(Sheet sheet,boolean checkHeader){
		int startRow = 1;
		IDataSetMetaData table = builder.table;
		
		//int rowCount = sheet.getPhysicalNumberOfRows();
		int rowEndIndex = sheet.getLastRowNum(); //最后一行下标 Count -1
		if(rowEndIndex <=0) {
			return Collections.emptyList();
		}
		setFirstSheetName(sheet.getSheetName());
		
		Collection<FProperty> columns = null;
		if(builder.fieldMap != null) {
			columns = builder.fieldMap.getProperties().values();
			startRow = builder.fieldMap.getStartRow() ;
			checkColHeader(sheet, builder.fieldMap, checkHeader);
		}
		if(startRow < 1) {
			startRow = 1;
		}
		
		MergedRegionVo<ExcelCellVo> mergedRegionVo = null;
		if(builder.merged !=MergedType.None) {
			mergedRegionVo = new MergedRegionVo<ExcelCellVo>(sheet,builder.merged);
			this.setMergedRegionVo(mergedRegionVo); 
		}

		List<Map<String,Object>> record = new ArrayList<Map<String,Object>>(rowEndIndex);
		this.excelRows = new ArrayList<ExcelRowVo>(rowEndIndex);
		
		for (int rowIdx = startRow - 1; rowIdx <= rowEndIndex; rowIdx++) {
			Row row = sheet.getRow(rowIdx); //下标由0开始
			if (row == null) {
				continue ;
			}
			
			List<ExcelCellVo> values = new ArrayList<ExcelCellVo>();
			this.excelRows.add(new ExcelRowVo(rowIdx, values));
			
			if(columns == null) { //全部获取
				int cellCount = row.getLastCellNum();
				for (int col = 0; col < cellCount; col++) {
					Cell cell = row.getCell(col);
					if (cell == null) {
						continue ;
					}
					String fieldName = String.valueOf(cell.getColumnIndex());
					Object value = parseCellValue(cell, fieldName);	
					values.add(new ExcelCellVo(fieldName, value, rowIdx, col));
				}
			}
			else { //有映射对象
				for(FProperty pm : columns) {
					int col = pm.getColumn();
					Cell cell = row.getCell(col);
					Object value ;
					String fieldName = pm.getName();
					ColumnType type = pm.getType();
					if (cell == null) {
						value = null ;
					}
					else {
						value = parseCellValue(cell, fieldName,type,table);
					}
					//每个字段都要有行列信息
					values.add(new ExcelCellVo(fieldName, value, rowIdx, col));
				}
			}
			
			//如果此行数据全部为空，则不需要保存
			if (values.size()>0 ) {
				Map<String,Object> rds = getMergedValue(values);
				
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
	 * @return 返回每个字段名对应的值
	 */
	@SuppressWarnings("unchecked")
	private Map<String/**字段名*/,Object/**字段值*/> getMergedValue(List<ExcelCellVo> values) {
		int emptyValueCount =0,size = values.size();
		while(emptyValueCount < size && values.get(emptyValueCount).getValue() == null) {
			emptyValueCount++;//空值个数
		}
		if(emptyValueCount == size) { //全部value都是null
			return Collections.emptyMap();
		}
		
		Map<String,Object> rds = new CaseInsensitiveLinkedMap();// NOPMD
		MergedRegionVo<ExcelCellVo> mergedRegionVo = getMergedRegionVo();
		if(mergedRegionVo == null) { //不需要合并单元格处理
			for(ExcelCellVo e :  values) {
				if(e.getValue()!=null) {
					rds.put(e.getKey(), e.getValue());
				}
			}
			return rds;
		}
		////////////////////////////////
		
		for(ExcelCellVo cell :values) {//没有取得cell的值，就判断是否合并值
			RegionVo<ExcelCellVo> regionVo = mergedRegionVo.getMergedRegion(cell.getRow(), cell.getColumn());
			ExcelCellVo mergedCell ;//合并格的第一元素
			if(regionVo == null) { //没有找到，表示没有合并
				mergedCell = cell;
			}
			else {
				mergedCell =(ExcelCellVo) regionVo.getValue(); //第一元素
				if(mergedCell == null){//合并单元格，只有第一个格是有值的
					mergedCell = cell;
					regionVo.setValue(cell);
				}
			}
			
			rds.put(mergedCell.getKey(), mergedCell.getValue());
			
		}
		return rds;
	}
	
	/**
	 * 读取指定行列的数据（合并方式与{@linkplain ReaderBuilder#setMerged(MergedType)}保持一致）
	 * @param rowIndex 第一行由0开始
	 * @param columnIndex 第一列行由0开始
	 * @return
	 */
	public Object getCellValue(int rowIndex,int columnIndex) {
		if(rowIndex < 0 || columnIndex < 0) {
			throw new EnviException("下标越界，行列都必须大于0");
		} 

		MergedRegionVo<ExcelCellVo> mg = this.getMergedRegionVo();
		ExcelCellVo resultCell = null;
		if(mg != null) {//合并取值
			RegionVo<ExcelCellVo> vo = mg.getMergedRegion(rowIndex, columnIndex);
			resultCell = (vo == null ? null : vo.getValue()) ; //返回null表示没有合并格
		}
		if(resultCell == null) {//每个单元格分散处理(找对应行，对应列)
			BinarySearch  searchRow = new BinarySearch(excelRows);
			ExcelRowVo rows = searchRow.findByIndex(rowIndex);
			//ExcelCellVo cell = null;
			if(rows != null) { //不存在数据 
				BinarySearch searchCell = new BinarySearch(rows.getCells());
				resultCell = searchCell.findByIndex(columnIndex);
			}
		}
			
		return (resultCell == null ? null : resultCell.getValue()) ;
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
            	if(c == null || c.getCellType() != Cell.CELL_TYPE_STRING) {
            		continue;//标题只认字符串类型
            	}
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
    
    private class ExcelRowVo implements IBinarySearchAtom{
    	private int index;
    	private List<ExcelCellVo> cells;
    	public ExcelRowVo(int rowIndex,List<ExcelCellVo> cells) {
    		this.index = rowIndex;
    		this.cells = cells;
    	}
    	@Override
    	public String toString() {
    		return String.valueOf(index) + "," + String.valueOf(cells== null?0:cells.size());
    	}
 
		public List<ExcelCellVo> getCells() {
			return cells;
		}
 
		/**行下标，0开始
		public void setIndex(int rowIndex) {
			this.index = rowIndex;
		}*/

		/**行下标，0开始*/
		@Override
		public int getIndex() { 
			return index;
		}
    }
    /**
     * 每格单元格的数据
     * @author jiqj 
     */
	private class ExcelCellVo implements Map.Entry<String, Object>,IBinarySearchAtom{
		private final String key;
		private Object value;
		private final int column ,row;
		public ExcelCellVo(String key,Object value,int row,int column) {
			this.key = key;
			this.value = value;
			this.row = row;
			this.column =column;
		}
		/**字段名*/
		@Override
		public String getKey() { 
			return key;
		}
		/**值*/
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
		

		/**列下标,0开始*/
		@Override
		public int getIndex() { 
			return getColumn();
		}
		/**列下标,0开始*/
		public int getColumn() {
			return column;
		}
		/**行下标,0开始*/
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
	private static interface IBinarySearchAtom{
		public int getIndex();
	}
	/**玩下算法: 二分查找法*/
	private static class BinarySearch{
		private final List<? extends IBinarySearchAtom> sortAtoms;
		private int callDeep ;
		public BinarySearch(List<? extends IBinarySearchAtom> atoms) {
			sortAtoms = atoms;
			callDeep = 0;
		}
		@SuppressWarnings("unchecked")
		public <T> T findByIndex(int findIndex) {
			return (T)findByIndex0(findIndex, 0, sortAtoms.size()-1);
		}
		
		/**
		 * 二分查找法,前提 {@linkplain #excelRows}必须有序
		 * @param findIndex 需要查找的id
		 * @param startIndex 开始位置 x >=0 
		 * @param lastIndex 结束位置  <= x
		 * @return
		 */
		private IBinarySearchAtom findByIndex0(final int findIndex,final int startIndex,final int lastIndex) {
			if(startIndex > lastIndex) {
				return null;
			}
			else if(callDeep >128) {
				throw new EnviException("BinarySearch.findByIndex0递归深度异常，请检查逻辑，超:" + callDeep);
			}
			callDeep ++;
			int middleIndex =  (lastIndex + startIndex)/ 2;
			IBinarySearchAtom vo = sortAtoms.get(middleIndex);
			int rowIdx = vo.getIndex();
			if(findIndex < rowIdx) { // 记录在前半部分(结束位置左移位)
				vo = findByIndex0(findIndex, startIndex, middleIndex-1);
			}
			else if(findIndex> rowIdx){ //记录在后半部分(开始位置右移位)
				vo = findByIndex0(findIndex, middleIndex+1 , lastIndex);
			}
			else { //相同
				//return vo;
			}
			callDeep --;
			return vo;
		}
	}
}
