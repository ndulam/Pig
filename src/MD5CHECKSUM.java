/**
 * Applying the checksum for the columns
 *
 */
package com.transformation.udf;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

public class MD5CHECKSUM extends EvalFunc<String> {

	public String exec(Tuple input) throws ExecException {

		MessageDigest md = null;
		StringBuffer sb = null;

		try {

			int size = input.size();
			StringBuffer str = new StringBuffer();
			for (int i = 0; i < size; i++) {
				str.append((String) input.get(i));
			}

			md = MessageDigest.getInstance("MD5");
			md.update(str.toString().getBytes());
			byte[] digest = md.digest();
			sb = new StringBuffer();
			for (byte b : digest) {
				sb.append(String.format("%02x", b & 0xff));
			}

		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			throw new ExecException(e);
		}
		return sb.toString();
	}
}
