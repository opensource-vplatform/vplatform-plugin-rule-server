package com.toone.itop.rule.apiserver.util;

/**
 * 所有的方法都用不上
 * @author jiqj
 *
 */
public abstract class AbstractRule4Tree  {
	/**
	@Override
	public Object evaluate(@SuppressWarnings("rawtypes") List args) {
		RuleContext ruleContext = ((RuleContext) Variables.getParamValue("ruleContext"));
		try {
			DASRuntimeContextFactory.getService().init();
			return this.evaluate(ruleContext);
		} finally {
			DASRuntimeContextFactory.getService().clear();
		}
	}
*/
	/**
	 * 设置规则运行时的树结构信息
	 * 
	 * @param tableName
	 * @param treeStructMap
	 
	protected final void setTreeStruct(String tableName, Map<String, String> treeStructMap) {
		DASRuntimeContextFactory.getService().setTreeStructMap(tableName, treeStructMap);
	}
*/
	/**
	 * 设置规则运行时树结构信息
	 * 
	 * @param treeStructMaps
	
	protected final void addTreeStructs(@SuppressWarnings("rawtypes") List<Map> treeStructMaps) {
		DASRuntimeContextFactory.getService().addTreeStructMaps(treeStructMaps);
	} */

}
