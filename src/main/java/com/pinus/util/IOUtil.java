package com.pinus.util;

import java.io.*;

/**
 * input output utility.
 * 
 * @author duanbn
 */
public class IOUtil {

	public static byte[] getBytes(Object obj) {
		ObjectOutputStream oos = null;
		ByteArrayOutputStream baos = null;

		try {
			baos = new ByteArrayOutputStream();
			oos = new ObjectOutputStream(baos);
			oos.writeObject(obj);
			oos.flush();
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			try {
				if (baos != null) {
					baos.close();
				}
				if (oos != null) {
					oos.close();
				}
			} catch (IOException e) {
			}
		}

		return baos.toByteArray();
	}

	public static <T> T getObject(byte[] data, Class<T> clazz) {
		ObjectInputStream ois = null;
		ByteArrayInputStream bais = null;
		try {
			bais = new ByteArrayInputStream(data);
			ois = new ObjectInputStream(bais);
			return (T) ois.readObject();
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			try {
				if (bais != null) {
					bais.close();
				}
				if (ois != null) {
					ois.close();
				}
			} catch (IOException e) {
			}
		}
	}

}
