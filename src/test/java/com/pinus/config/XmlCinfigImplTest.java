package com.pinus.config;

import java.util.List;
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
	public void testLoadMasterDbClusterInfo() {
		Map<String, List<DBClusterInfo>> map = this.config.loadMasterDbClusterInfo();
		List<DBClusterInfo> list = map.get("klstorage");
		for (DBClusterInfo a : list) {
			System.out.println(a.getStart() + ":" + a.getEnd() + " " + a.getGlobalConnInfo());
			for (DBConnectionInfo cinfo : a.getDbConnInfos()) {
				System.out.println(a.getStart() + ":" + a.getEnd() + " " + cinfo);
			}
		}
	}

	@Test
	public void testLoadSlaveDbClusterInfo() {
		Map<String, List<List<DBClusterInfo>>> map = this.config.loadSlaveDbClusterInfo();
		List<List<DBClusterInfo>> a = map.get("user");
		for (int i = 0; i < a.size(); i++) {
			System.out.println(i);
			for (DBClusterInfo c : a.get(i)) {
				System.out.println(c.getGlobalConnInfo());
				for (DBConnectionInfo dbConnInfo : c.getDbConnInfos()) {
					System.out.println(c.getStart() + ":" + c.getEnd() + " " + dbConnInfo);
				}
			}
		}
	}
}
