package com.yindangu.v3.platform.excel;

import java.util.ArrayList;
import java.util.List;

import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.util.CellRangeAddress;

import com.yindangu.v3.business.plugin.execptions.ConfigException;
 

/**读excel时，分析Sheet中的合并行信息*/
class MergedRegionVo {
	
	public static class RegionVo{
		private int x,y;
		private int width,height;
		private Object mergedValue;
		public RegionVo() {
			
		}
		public RegionVo(int x,int y ,int width,int height) {
			this.x = x;
			this.y = y;
			this.width = width;
			this.height = height;
		}
		
		/**x == firstColumn，另外:lastColumn = firstColumn + width -1 */
		public int getX() {
			return x;
		}
		/** y == firstRow，另外:lastRow = firstRow + height -1 */
		public int getY() {
			return y;
		}
		/** width ==1表示列没有合并，width >0 || height>0 == true */
		public int getWidth() {
			return width;
		}
		
		/** height ==1表示行没有合并，width >0 || height>0 == true */
		public int getHeight() {
			return height;
		}
		
		/**合并单元格的值*/
		public Object getMergedValue() {
			return mergedValue;
		}
		public void setMergedValue(Object mergedValue) {
			this.mergedValue = mergedValue;
		}
		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("x:").append(x).append(",y:").append(y).append(",width:").append(width).append(",height:").append(height);
			return sb.toString();
		}
		/*
		public void setWidth(int width) {
			this.width = width;
		}
		public void setHeight(int height) {
			this.height = height;
		}
		
		public void setX(int x) {
			this.x = x;
		}
		public void setY(int y) {
			this.y = y;
		}
		*/
	}
	private int sheetMergeCount ;
	private List<RegionVo> regions;
	private MergedType type;
	
	/**
	 * @param sheet
	 * @param type 合并类型，如果为null就使用默认配置
	 */
	public MergedRegionVo(Sheet sheet,MergedType type) {
		if(type == null || type == MergedType.None) { 
			throw new ConfigException("MergedType类型无效:" + type); 
		}
		this.type = type;
		this.readMergedRegion(sheet);
	}
	
	/**
	 * poi读取合并的单元格信息
	 * 原文链接：https://blog.csdn.net/str0708/article/details/84856424 
	 * @param sheet
	 */
	protected void readMergedRegion(Sheet sheet) {
		int count = sheet.getNumMergedRegions();
		this.sheetMergeCount = count;
		regions = new ArrayList<RegionVo>(count);
		
		for (int i = 0; i < count; i++) {
			CellRangeAddress range = sheet.getMergedRegion(i);
		    int firstColumn = range.getFirstColumn();
		    int lastColumn = range.getLastColumn();
		    int firstRow = range.getFirstRow();
		    int lastRow = range.getLastRow();
		    int width = lastColumn - firstColumn +1;
		    int height = lastRow - firstRow +1;
		    
		    RegionVo vo = new RegionVo(firstColumn, firstRow, width, height);
		    regions.add(vo);
		}
		
	}

	public int getSheetMergeCount() {
		return sheetMergeCount;
	}
	/**
	 * 取得坐标范围的值
	 * @param row 行
	 * @param col 列
	 * @return
	 */
	public RegionVo getMergedRegion(int row,int col) {
		if(regions == null) {
			throw new ConfigException("未初始化合并的单元格信息，请先调用readMergedRegion()方法");
		}
		RegionVo rs = null;
		for(int i = 0,size = regions.size();i < size ;i++) {
			RegionVo v = regions.get(i);
			int x = v.getX(),y = v.getY();
			if(row >= y && col >= x) {
				boolean merged = false;
				int lastColumn = x + v.getWidth(),lastRow =y + v.getHeight(); 
				if(type == MergedType.MergedCol) {//同行，列合并
					merged =(row == y && col < lastColumn); 
				}
				else if(type == MergedType.MergedRow) {//同列，行合并
					merged =(col == x && row < lastRow );
				}
				else if(type == MergedType.MergedALL){//行、列合并
					merged =( row < lastRow && col < lastColumn);
				}
				else {
					throw new ConfigException("MergedType类型无效:" + type);
				}
				
				if(merged) {
					rs = v;
					break;
				}
			}
		}
		return rs;
	}
}
