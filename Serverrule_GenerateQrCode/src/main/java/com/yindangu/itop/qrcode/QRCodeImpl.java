package com.yindangu.itop.qrcode;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import javax.imageio.ImageIO;
import javax.imageio.stream.ImageOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.zxing.BarcodeFormat;
import com.google.zxing.EncodeHintType;
import com.google.zxing.MultiFormatWriter;
import com.google.zxing.common.BitMatrix;
import com.google.zxing.qrcode.decoder.ErrorCorrectionLevel;
import com.yindangu.v3.business.VDS;
import com.yindangu.v3.business.file.api.IFileOperate;
import com.yindangu.v3.business.file.api.model.IAppFileInfo;
import com.yindangu.v3.business.plugin.execptions.ConfigException;
import com.yindangu.v3.business.plugin.execptions.EnviException;
import com.yindangu.v3.platform.plugin.util.VdsUtils;

/**
 * 抄com.toone.itop.common.qrcode.impl.TQRCodeImpl
 * @author jiqj
 *
 */
public class QRCodeImpl implements IQRCode{
	private static final Logger log = LoggerFactory.getLogger(QRCodeImpl.class);
	@Override
	public String encode(String target, int width, int height, int margin, QRErrorCorrectionLevel level) { 
		InputStream is = null;
		try {
			MultiFormatWriter multiFormatWriter = new MultiFormatWriter();
			Map<EncodeHintType, Object> hints = new HashMap<>();
			//不知道这句什么意思
//			hints.put(EncodeHintType.DATA_MATRIX_SHAPE, "UTF-8");
			hints.put(EncodeHintType.CHARACTER_SET, "UTF-8");
			if(margin>=0){
				hints.put(EncodeHintType.MARGIN,margin);
			}
			if(level!=null){
				ErrorCorrectionLevel errorCorrectionLevel = getErrorCorrectionLevel(level);
				hints.put(EncodeHintType.ERROR_CORRECTION,errorCorrectionLevel);
			}
			
			BitMatrix bitMatrix = multiFormatWriter.encode(target,BarcodeFormat.QR_CODE, width, height, hints);
			is = getImageStream(bitMatrix);
			String fileID = saveFile(is);
			return fileID; 
		} catch (Exception e) {
			throw new EnviException("生成二维码出错",e);
		}
		finally {
			close(is);
		}
	}
	/**
	 * 转成流
	 * @param bitMatrix
	 * @return
	 */
	private InputStream getImageStream(BitMatrix bitMatrix) {
		InputStream is = null;
		BufferedImage bi = MatrixToImageWriter.toBufferedImage(bitMatrix);
		bi.flush();
		ByteArrayOutputStream bs = new ByteArrayOutputStream();
		ImageOutputStream imOut;
		try {
			imOut = ImageIO.createImageOutputStream(bs);
			ImageIO.write(bi, "png", imOut);
			is = new ByteArrayInputStream(bs.toByteArray());
		} catch (IOException e) {
			e.printStackTrace();
		}
		return is;
	}
	private static void close(Closeable is) {
		if(is == null) {
			return ;
		}
		try {
			is.close();
		}
		catch(IOException e) {
			log.error("close is error",e);
		}
	}
	/**
	 * 图片存入本地数据库
	 * 
	 * @param in
	 * @return
	 */
	private String saveFile(InputStream is) {
		IFileOperate fs = VDS.getIntance().getFileOperate();
		String fileId = VdsUtils.uuid.generate();//UUID.generate();
		IAppFileInfo appFileInfo = fs.newAppFileInfo();//new AppFileInfo();
		appFileInfo.setOldFileName("qrcode_image_"+fileId+".png");
		appFileInfo.setId(fileId);
		appFileInfo.setDataStream(is);
		//IFileOperateFactory.getIFileOperate().upload(is, appFileInfo);
		if(fileId.indexOf("20211130-test-debug")==0 && is instanceof ByteArrayInputStream) {
			saveTestFile((ByteArrayInputStream)is);
		}
		fs.saveFileInfo(appFileInfo);
		return fileId;
	}
	private void saveTestFile(ByteArrayInputStream is) {
		FileOutputStream os =null;
		try {
			File fs = new File("d:/temp/qrcode.png");
			if(!fs.getParentFile().exists()) {
				fs.getParentFile().mkdirs();
			}
			is.reset();
			os = new FileOutputStream(fs);
			byte[] bys = new byte[1024*8];
			int len ;
			while((len = is.read(bys))!=-1) {
				os.write(bys, 0, len);
			}
		}
		catch(IOException e) {
			log.error("这个错误忽略",e);
		}
		finally {
			is.reset();
			close(os);
		}
		
	}
	private ErrorCorrectionLevel getErrorCorrectionLevel(QRErrorCorrectionLevel level) {
		ErrorCorrectionLevel rs = null;
		switch (level) {
		case L:
			rs=ErrorCorrectionLevel.L;
			break;
		case M:
			rs=ErrorCorrectionLevel.M;
			break;
		case Q:
			rs=ErrorCorrectionLevel.Q;
			break;
		case H:
			rs=ErrorCorrectionLevel.H;
			break;
		default:
			throw new ConfigException("不能识别的类型:" + level);
		}
		return rs;
	}

}
