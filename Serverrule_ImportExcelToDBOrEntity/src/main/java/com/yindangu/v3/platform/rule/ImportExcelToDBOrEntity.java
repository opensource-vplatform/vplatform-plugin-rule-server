package com.yindangu.v3.platform.rule;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

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

/**
 * 导入excel数据，第一版，与开发系统绑死的
 * @Author xugang
 * @Date 2021/7/23 16:19
 */
public class ImportExcelToDBOrEntity implements IRule { 
	private static final Logger logger = LoggerFactory.getLogger(ImportExcelToDBOrEntity.class);
	/**
     * 默认方法
     * @param context
     * @return
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public IRuleOutputVo evaluate(IRuleContext context) {
        long starTime = System.currentTimeMillis();
        String fileId = getVDS().getFormulaEngine().eval(context.getPlatformInput("fileSource").toString()); // 文件标识
        if (fileId == null || fileId.length()==0) { 
            throw new ConfigException("后台导入Excel规则，获取文件ID标识为空，请检查");
        }
        
        InputStream inputStream = null;
        try {
            List<Map<String, Object>> itemsList = (List) context.getPlatformInput("items");
            for (Map<String, Object> singleItem : itemsList) {
                IAppFileInfo appFileInfo = getVDS().getFileOperate().getFileInfo(fileId);
                if (appFileInfo == null) {
                    throw new ConfigException("后台导入Excel规则，获取文件标识为【" + fileId + "】文件对象，请检查");
                }
                inputStream = appFileInfo.getDataStream();
                ExcelToDBOrEntity db = new ExcelToDBOrEntity();
                db.importToDB(context, singleItem, inputStream,MergedType.None);
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
	private IVDS getVDS() {
        return VDS.getIntance();
    }
}
