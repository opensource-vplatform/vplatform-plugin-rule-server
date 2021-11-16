package com.yindangu.v3.platform.rule;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yindangu.v3.business.VDS;
import com.yindangu.v3.business.file.api.model.IAppFileInfo;
import com.yindangu.v3.business.plugin.business.api.rule.IRule;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleContext;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleOutputVo;
import com.yindangu.v3.business.plugin.execptions.ConfigException;
import com.yindangu.v3.business.plugin.execptions.PluginException;
import com.yindangu.v3.business.vds.IVDS;
import com.yindangu.v3.platform.excel.MergedType;
import com.yindangu.v3.platform.excel.POIExcelAction;
import com.yindangu.v3.platform.plugin.util.VdsStringUtil;
import com.yindangu.v3.platform.plugin.util.VdsUtils;
import com.yindangu.v3.platform.rule.SaveDataBaseAction.ExcelResultVo;

/**
 * 导入excel数据，第2版，与第1版区别是增加了2开的参数
 * @Author xugang
 * @Date 2021/7/23 16:19
 */
public class ImportExcelToDBOrEntity2 implements IRule {

	public final static String D_RULE_CODE = "ImportExcelToDBOrEntity2";

    public final static String D_RULE_NAME = "Excel导入到数据库表或实体(支持2次开发)";
    /** 导入Excel存在合并单元格时处理方式 {@linkplain MergedType}*/
    public static final String D_INPUT_MergedType = "mergedType";
    public static final String D_OUTPUT_SheetName = "sheetName";
    /**取指定行列的值*/
    public static final String D_INPUT_PointEntity = "PointEntity ";
    
    public static final String D_OUTPUT_PointEntity = "PointEntity ";
    public static final String D_INPUT_PointRow = "row";
    public static final String D_INPUT_PointCol = "col";
    public static final String D_OUTPUT_PointValue = "cellValue";
    public static final String D_OUTPUT_PointSheetNo = "sheetNo";
    public static final String D_OUTPUT_PointSheetName = "sheetName";
    
    //public static final String D_OUT_MergedType = "mergedType";
    
    private static final Logger logger = LoggerFactory.getLogger(ImportExcelToDBOrEntity2.class);

    /**
     * 返回sheetName，如果导入多个sheetName，就返回第一个
     */
    @SuppressWarnings("unchecked")
	@Override
    public IRuleOutputVo evaluate(IRuleContext context) {
        long starTime = System.currentTimeMillis();
        
        InputStream inputStream = null;
        try {
        	String execlFileExps  ;
        	MergedType mergedType ;  
        	List<PostionVo> cellposition; 
        	{
        		Object fs =context.getPlatformInput("fileSource"); 
        		execlFileExps = (fs == null ? null : fs.toString());
        		inputStream = getInputStream(execlFileExps);
        		String types = (String)context.getInput(D_INPUT_MergedType);
        		if((mergedType = MergedType.getType(types)) == null) {
        			mergedType = MergedType.None;//兼容历史，不合并
        		}
        		 
        		List<Map> post = (List<Map>)context.getInput(D_INPUT_PointEntity);
        		cellposition = PostionVo.convertlistMapToPostionVo(post);
        	}

        	String sheetName = "";
            List<Map<String, Object>> itemsList = (List<Map<String, Object>>) context.getPlatformInput("items");
            for (Map<String, Object> singleItem : itemsList) { 
                
                ExcelToDBOrEntity exceldb = new ExcelToDBOrEntity();
                ExcelResultVo rv = exceldb.importToDB(context, singleItem, inputStream,mergedType);
                if(VdsUtils.string.isEmpty(sheetName)) {
                	sheetName = rv.getSheetName();
                }
                //readSheetNo.add(rv.getSheetIndex());
                PostionVo.readCellValue(rv, cellposition);
            }
            close(inputStream);
            readOtherSheet(execlFileExps, cellposition,mergedType);
            
            List<Map<String,Object>> outEntity = PostionVo.toMap(cellposition);
            IRuleOutputVo outputVo = context.newOutputVo();
            outputVo.put(ImportExcelToDBOrEntity2.D_OUTPUT_SheetName, sheetName);
            outputVo.put(ImportExcelToDBOrEntity2.D_OUTPUT_PointEntity,outEntity);
            return outputVo;
        }
        catch(PluginException e) {
        	throw e;
        }
        catch (Exception e) {
            throw new ConfigException("后台导入Excel规则，执行失败！\n" + e.getMessage(), e);
        } finally {
        	logger.info("后台导入Excel数据,总时长：" + (System.currentTimeMillis() - starTime) + "毫秒");
        	close(inputStream);
        }        
    }
    private InputStream getInputStream(String execlFileExps) {
    	if(execlFileExps == null) {
			throw new ConfigException("规则参数丢失:请检查发布包的[ruleConfig]是否有配置[fileSource]");
		}
    	IVDS vds = VDS.getIntance() ;
		String fileId = vds.getFormulaEngine().eval(execlFileExps); // 文件标识
        if (VdsUtils.string.isEmpty(fileId)) { 
            throw new ConfigException("后台导入Excel规则，获取文件ID标识为空，请检查");
        }
        IAppFileInfo  fileInfo = vds.getFileOperate().getFileInfo(fileId);
        if (fileInfo == null) {
            throw new ConfigException("后台导入Excel规则，获取文件标识为【" + fileId + "】文件对象，请检查");
        }
        InputStream is = fileInfo.getDataStream();
        return is;
    }
    
    /**
     * 读取其他没有打开的sheet的单元格
     * @param cellposition
     */
    private void readOtherSheet(String execlFileExps, List<PostionVo> cellposition,MergedType merged) {
    	Map<Integer,Boolean> sheetIndexs = new HashMap<>();
    	for(PostionVo vo : cellposition) {
    		if(vo.getSheetName() == null && vo.getSheetNo()>=0) { //-1就取第一个sheet
    			Integer idx = Integer.valueOf(vo.getSheetNo());
    			sheetIndexs.put(idx, Boolean.TRUE);
    		}
    	} 
    	InputStream is = null;
    	try {
    		for(Integer idx: sheetIndexs.keySet()) {
	    		is = getInputStream(execlFileExps); // 文件标识
	    		
	    		POIExcelAction.ReaderBuilder eb = new POIExcelAction.ReaderBuilder();
	    		POIExcelAction excel = eb.setFieldMap(null)
	    			.setInputStream(is)
	    			.setMerged(merged)
	    			.setSheetIndex(idx.intValue())
	    			.setTable(null)
	    			.builder();
	    		excel.readData();
	    		
	    		String sheetName = excel.getSheetName();
	    		for(PostionVo vo : cellposition) {
	        		if(idx.intValue() == vo.getSheetNo()) {
	    	    		Object cv = excel.getCellValue(vo.getRow() -1,vo.getCol() -1);
	    	    		vo.setValue(cv);
	    	    		vo.setSheetName(sheetName); 
	        		}
	        	}
	    		
	    		close(is);
    		}
    	}
    	finally {
    		close(is);
    	}
    }
    
    private static class PostionVo{
    	private int sheetNo,row,col;
    	private Object value;
    	private String sheetName;
    	public PostionVo() {
    		row = -1;
    		col =-1;
    		sheetName = null;//未赋值
    	}
    	/**对应入参，由1开始*/
		public int getRow() {
			return row;
		}
		public void setRow(int row) {
			this.row = row;
		}
		/**对应入参，由1开始*/
		public int getCol() {
			return col;
		}
		public void setCol(int col) {
			this.col = col;
		}
 
		public void setValue(Object value) {
			this.value = value;
		}
		
		/**
		 * 从map取值
		 * @param m
		 */
		private PostionVo forMap(Map<String,Number> m) {
			for(Entry<String, Number> e : m.entrySet()) { //转小写
	    		String key =  e.getKey();
	    		if(D_INPUT_PointRow.equalsIgnoreCase(key)) {
	    			this.setRow(e.getValue().intValue());
	    		}
	    		else if(D_INPUT_PointCol.equalsIgnoreCase(key)) {
	    			this.setCol(e.getValue().intValue());
	    		}
	    		else if(D_OUTPUT_PointSheetNo.equalsIgnoreCase(key)) {
	    			this.setSheetNo(e.getValue().intValue());
	    		}
	    	}
			return this;
		}
		private Map<String,Object> toMap(){
			Map<String,Object> rs = new HashMap<String,Object>();
			rs.put(D_OUTPUT_PointSheetNo, Integer.valueOf(sheetNo));
			rs.put(D_OUTPUT_PointSheetName, sheetName);
			rs.put(D_OUTPUT_PointValue, value);
			
			rs.put(D_INPUT_PointRow, Integer.valueOf(row));
			rs.put(D_INPUT_PointCol, Integer.valueOf(col));
			return rs;
		}
		public int getSheetNo() {
			return sheetNo;
		}
		public void setSheetNo(int sheetNo) {
			this.sheetNo = sheetNo;
		}
		/**
		 * 
		 * @return 如果返回null，未赋值，表示这个sheet未打开
		 */
		public String getSheetName() {
			return this.sheetName;
		}
		public void setSheetName(String sheetName) {
			this.sheetName = sheetName;
		}
	    @SuppressWarnings({ "rawtypes", "unchecked" })
		protected static List<PostionVo> convertlistMapToPostionVo(List<Map> positions){
	    	int size = (positions == null ? 0 :positions.size());
	    	if(size ==0) {
	    		return Collections.emptyList();
	    	}
	    	List<PostionVo>  post = new ArrayList<>(size);
	    	for(Map record : positions) {
	    		PostionVo vo = (new PostionVo()).forMap(record);
	    		post.add(vo);
	    	}
	    	return post;
	    }
	    protected static List<Map<String,Object>> toMap(List<PostionVo> positions) {
	    	List<Map<String,Object>> rs = new ArrayList<>(positions.size());
	    	for(PostionVo vo : positions) { 
	    		Map<String,Object> m = vo.toMap();
	    		rs.add(m);
	    	}
	    	return rs;
	    }
	    protected static int readCellValue(ExcelResultVo excelResult,List<PostionVo> positions) {
	    	String sheetName = excelResult.getSheetName();
	    	int sheetNo = excelResult.getSheetIndex();
	    	//List<Map> rs = new ArrayList<>(positions.size());
	    	int count = 0;
	    	for(PostionVo vo : positions) {
	    		if(sheetNo == vo.getSheetNo()) {
		    		Object cv = excelResult.getCellValue(vo.getRow() - 1,vo.getCol() -1);
		    		vo.setValue(cv);
		    		vo.setSheetName(sheetName);
		    		count++;
	    		}
	    	}
	    	return count;
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
    		logger.error("",e);
    	}
    }
     
}
