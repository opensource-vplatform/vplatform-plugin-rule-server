package com.toone.itop.formula.rule.inte;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.toone.itop.jdbc.apiserver.model.BizDataModel;
import com.toone.itop.jdbc.apiserver.model.VTable;
import com.toone.itop.jdbc.apiserver.tenant.TenantModel;
import com.toone.itop.metadata.apiserver.factory.IMDInitFactory;
import com.toone.itop.neo4j.apiserver.factory.Neo4jDBServiceFactory;

public class TenantEnableThreadClient implements Runnable {
	private TenantModel										tenantModel;
	private List<VTable>									tables;
	private boolean											isCurrentTenantMainDataSource;
	private Map<String, Map<String, Map<String, Object>>>	tableDatas;
	private Set<String>										tenantTableNames;

	public TenantEnableThreadClient(TenantModel tenantModel, List<VTable> tables, boolean isCurrentTenantMainDataSource, Map<String, Map<String, Map<String, Object>>> tableDatas,
			Set<String> tenantTableNames) {
		this.tenantModel = tenantModel;
		this.tables = tables;
		this.isCurrentTenantMainDataSource = isCurrentTenantMainDataSource;
		this.tableDatas = tableDatas;
		this.tenantTableNames = tenantTableNames;
	}

	@Override
	public void run() {
		BizDataModel.setTenantModel(tenantModel);
		try {
			if (!isCurrentTenantMainDataSource) {
				IMDInitFactory.getService().initCurDataSourceSysTables();
			}
			IMDInitFactory.getService().initDataSourceDatas(tables, tableDatas, tenantTableNames, isCurrentTenantMainDataSource);
			Neo4jDBServiceFactory.getInstance().syn2Neo4jDatas();
		} finally{
			BizDataModel.removeTenantModel();
		}
		
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
