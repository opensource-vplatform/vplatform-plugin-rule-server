package com.yindangu.v3.platform.excel;
/** 合并方式 */
public enum MergedType {
	/**不合并*/
	None,
	/** 合并列 */
	MergedCol,
	/** 合并行 */
	MergedRow,
	/** 合并行、列 */
	MergedALL;
	
	public static MergedType getType(String type) {
		if(type == null || (type = type.trim()).length()==0) {
			return null;
		}
		MergedType rs = null;
		for(MergedType t : MergedType.values() ) {
			if(t.name().equalsIgnoreCase(type)) {
				rs = t;
				break;
			}
		}
		return rs;
	}
}