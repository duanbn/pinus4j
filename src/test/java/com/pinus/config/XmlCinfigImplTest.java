package com.pinus.config;

import java.util.Map;

import org.junit.Test;

import com.pinus.cluster.beans.DBClusterInfo;
import com.pinus.cluster.beans.DBConnectionInfo;
import com.pinus.cluster.enums.HashAlgoEnum;
import com.pinus.config.impl.XmlClusterConfigImpl;

public class XmlCinfigImplTest {

	private IClusterConfig config;

	public XmlCinfigImplTest() throws Exception {
		this.config = XmlClusterConfigImpl.getInstance();
	}

	@Test
	public void testGetIdGeneratorBatch() {
		int batch = this.config.getIdGeneratorBatch();
		System.out.println(batch);
	}

	@Test
	public void testGetZkUrl() {
		String zkUrl = this.config.getZkUrl();
		System.out.println(zkUrl);
	}

	@Test
	public void testetHashAlgo() {
		HashAlgoEnum hashAlgo = this.config.getHashAlgo();
		System.out.println(hashAlgo);
	}

	@Test
	public void testLoadMasterGlobalInfo() {
		Map<String, DBConnectionInfo> map = this.config.loadMasterGlobalInfo();
		for (Map.Entry<String, DBConnectionInfo> entry : map.entrySet()) {
			System.out.println(entry.getKey() + ":" + entry.getValue());
		}
	}

	@Test
	public void testLoadSlaveGlobalInfo() {
		Map<String, Map<Integer, DBConnectionInfo>> map = this.config.loadSlaveGlobalInfo();
		for (Map.Entry<String, Map<Integer, DBConnectionInfo>> entry : map.entrySet()) {
			System.out.println(entry.getKey() + ":" + entry.getValue());
		}
	}

	@Test
	public void testLoadMasterDbClusterInfo() {
		Map<String, DBClusterInfo> map = this.config.loadMasterDbClusterInfo();
		for (Map.Entry<String, DBClusterInfo> entry : map.entrySet()) {
			System.out.println(entry.getKey() + ":" + entry.getValue());
		}
	}

	@Test
	public void testLoadSlaveDbClusterInfo() {
		Map<String, Map<Integer, DBClusterInfo>> map = this.config.loadSlaveDbClusterInfo();
		for (Map.Entry<String, Map<Integer, DBClusterInfo>> entry : map.entrySet()) {
			System.out.println(entry.getKey() + ":" + entry.getValue());
		}
	}
}
