package com.example.hive.udf;

import static java.net.URLDecoder.decode;
import static org.apache.commons.codec.binary.Base64.decodeBase64;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public final class DecodeToMap extends UDF {
	public Map<String, String> evaluate(Text s) throws UnsupportedEncodingException {
		String resultIds = decode(s.toString(), "UTF-8").replace(" ", "+");
		byte[] decodedData;
		try {
			decodedData = decodeBase64(resultIds);
		} catch (Exception e) {
			return new HashMap<String, String>();
		}

		return parseDecodedData(decodedData);
	}

	private Map<String, String> parseDecodedData(byte[] decodedData) {
		Map<String, String> recoveredItemIds = new HashMap<String, String>();
		if (decodedData.length % 4 != 0) {
			return recoveredItemIds;
		}
		int results = decodedData.length / 4;
		int j = 0;
		for (int nn = 0; nn < results; nn++) {
			int fromByte = 0;
			for (int i = 0; i < 4; i++) {
				int n = (decodedData[j] < 0 ? decodedData[j] + 256 : (int) decodedData[j]) << (8 * i);
				fromByte += n;
				j++;
			}
			recoveredItemIds.put(fromByte + "", nn + "");
		}
		return recoveredItemIds;
	}
}