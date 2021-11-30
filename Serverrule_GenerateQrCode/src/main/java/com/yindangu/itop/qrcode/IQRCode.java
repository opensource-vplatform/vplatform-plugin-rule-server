package com.yindangu.itop.qrcode;

import com.google.zxing.MultiFormatWriter;
import com.google.zxing.common.BitMatrix;
import com.yindangu.v3.business.dbc.constraints.NotBlank;
import com.yindangu.v3.business.dbc.constraints.NotNull;

public interface IQRCode {
	/** 容错率 7%~30%*/
	public enum QRErrorCorrectionLevel {
		/** 7% */
		L(7),
		/** 15% */
		M(15),
		/** 25% */
		Q(25),
		/** 30% */
		H(30);
		private final int value;

		private QRErrorCorrectionLevel(int v) {
			this.value = v;
		}

		public int getValue() {
			return value;
		}

		public static QRErrorCorrectionLevel getLevelByValue(int v) {
			QRErrorCorrectionLevel rs;
			switch (v) {
			case 7:
				rs = L;
				break;
			case 15:
				rs = M;
				break;
			case 25:
				rs = Q;
				break;
			case 30:
				rs = H;
				break;
			default:
				rs = null;
				break;
			}
			return rs;
		}

		public static QRErrorCorrectionLevel getLevelByName(String name) {
			if(name == null || (name = name.trim()).length()==0) {
				return null;
			}
			char fc = Character.toUpperCase( name.charAt(0));
			QRErrorCorrectionLevel rs;
			switch (fc) {
			case 'L':
				rs = L;
				break;
			case 'M':
				rs = M;
				break;
			case 'Q':
				rs = Q;
				break;
			case 'H':
				rs = H;
				break;
			default:
				rs = null;
				break;
			}
			return rs;
		}
	 }
	 
	/**
	 * 生成二维码
	 * @param target 编码
	 * @param width 宽
	 * @param height 高
	 * @param margin 留白（ 0~4 ）
	 * @param errorCorrectionLeve 容错率  (允许null，默认为{@linkplain MultiFormatWriter#encode(String, com.google.zxing.BarcodeFormat, int, int)}
 	 * @return 文件ID
	 */
	@NotNull
	String encode(
			@NotBlank
			String target,
			@NotNull
			int width,
			@NotNull
			int height,
			@NotNull
			int margin,
			@NotNull
			QRErrorCorrectionLevel level);
}
