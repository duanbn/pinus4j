package com.pinus.util;

import java.io.File;
import java.net.URL;

import org.junit.Test;

public class XmlUtilTest {

	@Test
	public void testXmlUtil() throws Exception {
		XmlUtil.getInstance();
	}

	@Test
	public void testXmlUtilString() throws Exception {
		URL url = Thread.currentThread().getContextClassLoader().getResource("storage-config.xml");
		XmlUtil.getInstance(new File(url.getFile()));
	}

}
