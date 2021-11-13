package com.yindangu.v3.platform.rule;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.toone.v3.platform.rule.model.ColumnImportCfg;
import com.toone.v3.platform.rule.model.ExcelImportDataCfg;
import com.toone.v3.platform.rule.model.FModelMapper;
import com.toone.v3.platform.rule.model.FProperty;
import com.toone.v3.platform.rule.model.TreeType;
import com.toone.v3.platform.rule.util.ObjPropertyUtil2;
import com.yindangu.commons.CaseInsensitiveLinkedMap;
import com.yindangu.v3.business.VDS;
import com.yindangu.v3.business.file.api.model.IAppFileInfo;
import com.yindangu.v3.business.jdbc.api.model.ColumnType;
import com.yindangu.v3.business.jdbc.api.model.ITable;
import com.yindangu.v3.business.metadata.api.IDataView;
import com.yindangu.v3.business.plugin.business.api.rule.ContextVariableType;
import com.yindangu.v3.business.plugin.business.api.rule.IRule;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleContext;
import com.yindangu.v3.business.plugin.business.api.rule.IRuleOutputVo;
import com.yindangu.v3.business.plugin.execptions.ConfigException;
import com.yindangu.v3.business.plugin.execptions.PluginException;
import com.yindangu.v3.business.vds.IVDS;
import com.yindangu.v3.platform.excel.MergedType;
import com.yindangu.v3.platform.plugin.util.VdsUtils;
import com.yindangu.v3.platform.rule.SaveDataBaseAction.ExcelContextType;
import com.yindangu.v3.platform.rule.SaveDataBaseAction.ParamsVo;

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
    //public static final String D_OUT_MergedType = "mergedType";
    
    private static final Logger logger = LoggerFactory.getLogger(ImportExcelToDBOrEntity2.class);

    @SuppressWarnings("unchecked")
	@Override
    public IRuleOutputVo evaluate(IRuleContext context) {
        long starTime = System.currentTimeMillis();
        IVDS vds = VDS.getIntance() ;
        String fileId = vds.getFormulaEngine().eval(context.getPlatformInput("fileSource").toString()); // 文件标识
        if (fileId == null || fileId.length()==0) { 
            throw new ConfigException("后台导入Excel规则，获取文件ID标识为空，请检查");
        }
        
        InputStream inputStream = null;
        try {
        	MergedType mergedType ;{
        		String types = (String)context.getInput(D_INPUT_MergedType);
        		if((mergedType = MergedType.getType(types)) == null) {
        			mergedType = MergedType.None;//兼容历史，不合并
        		}
        	}
        	ImportExcelToDBOrEntity exceldb = new ImportExcelToDBOrEntity();
        	
            List<Map<String, Object>> itemsList = (List<Map<String, Object>>) context.getPlatformInput("items");
            for (Map<String, Object> singleItem : itemsList) {
                IAppFileInfo appFileInfo = vds.getFileOperate().getFileInfo(fileId);
                if (appFileInfo == null) {
                    throw new ConfigException("后台导入Excel规则，获取文件标识为【" + fileId + "】文件对象，请检查");
                }
                inputStream = appFileInfo.getDataStream();
                exceldb.importToDB(context, singleItem, inputStream,mergedType);
                close(inputStream);
            }
            
            IRuleOutputVo outputVo = context.newOutputVo();
            outputVo.put(null);
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
