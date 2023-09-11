package com.toone.v3.platform.rule.util;

import com.toone.v3.platform.rule.model.*;
import com.yindangu.v3.business.jdbc.api.model.ColumnType;
import com.yindangu.v3.business.metadata.api.IDataObject;
import com.yindangu.v3.business.metadata.api.IDataView;
import com.yindangu.v3.business.plugin.execptions.BusinessException;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;

import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 处理Excel格式数据导出
 *
 * @author wangbin
 * @createDate 2012-2-23
 */
@SuppressWarnings("rawtypes")
public class ExcelExportData {


    private void setExportFileExtension(String version, DataEXportCfg cfg){
        if(isEmpty(version)){
            version="xls";
        }
        cfg.setExportFileExtension(version);
    }

    private void setExportFileExtension(String version, EXportDataCfg cfg){
        if(isEmpty(version)){
            version="xls";
        }
        cfg.setExportFileExtension(version);

    }

    private Workbook getWorkbook(String version){
        if(isEmpty(version)){
            return new HSSFWorkbook();
        }else if(version.equals("xls")){
            return new HSSFWorkbook();
        }else{
            return new SXSSFWorkbook(1000);
        }
    }

    public void exportData(Map<String, Object> sourceDatas, OutputStream out, String version) throws Exception {
        Workbook wb =getWorkbook(version);
        for(String sheetName:sourceDatas.keySet()){
            Map map = (Map)sourceDatas.get(sheetName);
            DataEXportCfg cfg =(DataEXportCfg)map.get("DataEXportCfg");
            setExportFileExtension(version, cfg);
            IDataView datas = (IDataView)map.get("DataView");
            boolean isCU = (Boolean) map.get("IsCU");
            List<IDataObject> list = datas.select();
            FModelMapper mapper = new FModelMapper();
            mapper.setTitle(true);
            String titleString = cfg.getTitle();
            int baserow = !isEmpty(titleString)?3:2;
            mapper.setTitleText(titleString);
            if(isCU){
                mapper.setStartRow(baserow+1);
            }else{
                mapper.setStartRow(baserow);
            }
            Map<String, FProperty> columnMap = new HashMap<String, FProperty>();
            mapper.setProperties(columnMap);
            for (int i = 0,colIndex=0; i < cfg.getDsColumnMap().size(); i++) {
                ColumnCfg columnCfg = cfg.getDsColumnMap().get(i);
                /**
                 * 梁朝辉 2015-02-13
                 * 开发平台中规则描述：如果“导出数据”不勾选，这列将会出现在Excel中，但没有数据，是空列。
                 * rtx与徐良兴沟通确定：isNeedExport这个属性应该为导出该字段的空列，而不是不导出该字段
                 *
                 * 以下为原代码(该代码已被注释)：
                 * if(!columnCfg.isNeedExport())
                 * 		continue;
                 */
                // if(!columnCfg.isNeedExport())
                // continue;
                FProperty prop = new FProperty();
                prop.setColumn(++colIndex);
                prop.setName(columnCfg.getFieldName());
                prop.setTitle(columnCfg.getChineseName());
                prop.setWidth(5000);

                // 如果不需要导出数据，则设置字段类型为文本
                ColumnType fieldType=ColumnType.Text;
                if (columnCfg.isNeedExport())
                    fieldType = datas.getMetadata().getColumn(columnCfg.getFieldName()).getColumnType();
                prop.setType(fieldType);

                prop.setNeedExport(columnCfg.isNeedExport());
                columnMap.put(columnCfg.getFieldName(), prop);
            }
            Sheet sheet =  wb.createSheet(sheetName);
            wb.setSheetOrder(sheet.getSheetName(), wb.getNumberOfSheets()-1);
            try{
                new FPOExcel(version).writeExcelSheet(list, mapper,sheet);
            }catch (Exception e) {
                // TODO: handle exception
                if(list.size()>65535){
//                    throw new BusinessException("导出记录数过多，超过xls格式文件的限制65535,请把导出规则配置换成xlsx格式，当前构件:"+CompContext.getCompCode()+";规则配置：name="+cfg.getDataName()+",title="+cfg.getTitle(),e);
                    throw new BusinessException("导出记录数过多，超过xls格式文件的限制65535,请把导出规则配置换成xlsx格式;规则配置：name="+cfg.getDataName()+",title="+cfg.getTitle(),e);
                }
            }

        }
        Workbook workBook = wb;
        workBook.write(out);
    }

    public void exportDataBack(Map<String, Object> sourceDatas, OutputStream out,String version) throws Exception {
        Workbook wb = getWorkbook(version);
        for(String sheetName:sourceDatas.keySet()){
            Map map = (Map)sourceDatas.get(sheetName);
            EXportDataCfg cfg =(EXportDataCfg)map.get("EXportDataCfg");
            setExportFileExtension(version, cfg);
            IDataView datas = (IDataView)map.get("DataView");
            boolean isCU = (Boolean) map.get("IsCU");
            List<IDataObject> list = datas.select();
            FModelMapper mapper = new FModelMapper();
            mapper.setTitle(true);
            String titleString = cfg.getTitle();
            int baserow = !isEmpty(titleString)?3:2;
            mapper.setTitleText(titleString);
            if(isCU){
                mapper.setStartRow(baserow+1);
            }else{
                mapper.setStartRow(baserow);
            }
            Map<String, FProperty> columnMap = new HashMap<String, FProperty>();
            mapper.setProperties(columnMap);
            for (int i = 0,colIndex=0; i < cfg.getMapping().size(); i++) {
                ColumnBCfg columnCfg = cfg.getMapping().get(i);
                /**
                 * 梁朝辉 2015-02-13
                 * 开发平台中规则描述：如果“导出数据”不勾选，这列将会出现在Excel中，但没有数据，是空列。
                 * rtx与徐良兴沟通确定：isNeedExport这个属性应该为导出该字段的空列，而不是不导出该字段
                 *
                 * 以下为原代码(该代码已被注释)：
                 * if(!columnCfg.isNeedExport())
                 * 		continue;
                 */
                // if(!columnCfg.isNeedExport())
                // continue;
                FProperty prop = new FProperty();
                prop.setColumn(++colIndex);
                prop.setName(columnCfg.getFieldCode());
                prop.setTitle(columnCfg.getExcelColName());
                prop.setWidth(5000);

                // 如果不需要导出数据，则设置字段类型为文本
                ColumnType fieldType=ColumnType.Text;
                if (columnCfg.isExportData())
                    fieldType = datas.getMetadata().getColumn(columnCfg.getFieldCode()).getColumnType();
                prop.setType(fieldType);

                prop.setNeedExport(columnCfg.isExportData());
                columnMap.put(columnCfg.getFieldCode(), prop);
            }
            Sheet sheet = wb.createSheet(sheetName);
            wb.setSheetOrder(sheet.getSheetName(), wb.getNumberOfSheets()-1);
            new FPOExcel(version).writeExcelSheet(list, mapper, sheet);
        }
        Workbook workBook = wb;
        workBook.write(out);
    }

    private boolean isEmpty(String str) {
        if(str == null || str.equals("")) {
            return true;
        }

        return false;
    }
}