package org.pinus.config;

import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.pinus.BaseTest;
import org.pinus.cluster.beans.DBClusterInfo;
import org.pinus.cluster.beans.DBClusterRegionInfo;
import org.pinus.cluster.beans.DBInfo;
import org.pinus.config.IClusterConfig;
import org.pinus.config.impl.XmlClusterConfigImpl;

public class XmlCinfigImplTest extends BaseTest {

	private IClusterConfig config;

	public XmlCinfigImplTest() throws Exception {
		this.config = XmlClusterConfigImpl.getInstance();
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
			for (DBInfo connInfo : dbClusterInfo.getSlaveGlobalConnection()) {
				System.out.println(connInfo);
			}

			for (DBClusterRegionInfo region : dbClusterInfo.getDbRegions()) {
				System.out.println("master sharding");
				for (DBInfo connInfo : region.getMasterConnection()) {
					System.out.println(connInfo);
				}

				System.out.println("slave sharding");
				List<List<DBInfo>> a = region.getSlaveConnection();
				for (List<DBInfo> b : a) {
					for (DBInfo c : b) {
						System.out.println(c);
					}
				}
			}
		}
	}

}
