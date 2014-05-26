package com.pinus.config;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.pinus.BaseTest;
import com.pinus.cluster.beans.DBClusterInfo;
import com.pinus.cluster.beans.DBClusterRegionInfo;
import com.pinus.cluster.beans.DBConnectionInfo;
import com.pinus.config.impl.XmlDBClusterConfigImpl;

public class XmlCinfigImplTest extends BaseTest {

	private IClusterConfig config;

	public XmlCinfigImplTest() throws Exception {
		this.config = XmlDBClusterConfigImpl.getInstance();
	}

	@Test
	public void testGetClusterInfo() throws Exception {
		Map<String, DBClusterInfo> map = this.config.getDBClusterInfo();
		for (Map.Entry<String, DBClusterInfo> entry : map.entrySet()) {
			System.out.println(entry.getKey());
			DBClusterInfo dbClusterInfo = entry.getValue();

			System.out.println("master global");
			System.out.println(dbClusterInfo.getMasterGlobalConnection());

			System.out.println("slave global");
			for (DBConnectionInfo connInfo : dbClusterInfo.getSlaveGlobalConnection()) {
				System.out.println(connInfo);
			}

			for (DBClusterRegionInfo region : dbClusterInfo.getDbRegions()) {
				System.out.println("master sharding");
				for (DBConnectionInfo connInfo : region.getMasterConnection()) {
					System.out.println(connInfo);
				}

				System.out.println("slave sharding");
				List<List<DBConnectionInfo>> a = region.getSlaveConnection();
				for (List<DBConnectionInfo> b : a) {
					for (DBConnectionInfo c : b) {
						System.out.println(c);
					}
				}
			}
		}
	}

}
