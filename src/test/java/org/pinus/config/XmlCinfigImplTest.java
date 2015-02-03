package org.pinus.config;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.pinus.BaseTest;
import org.pinus4j.cluster.beans.DBClusterInfo;
import org.pinus4j.cluster.beans.DBClusterRegionInfo;
import org.pinus4j.cluster.beans.DBInfo;
import org.pinus4j.cluster.config.IClusterConfig;
import org.pinus4j.cluster.config.impl.XmlClusterConfigImpl;

public class XmlCinfigImplTest extends BaseTest {

	private IClusterConfig config;

	public XmlCinfigImplTest() throws Exception {
		this.config = XmlClusterConfigImpl.getInstance();
	}

	@Test
	public void testGetClusterInfo() throws Exception {
		Collection<DBClusterInfo> dbClusterInfos = this.config.getDBClusterInfos();
		for (DBClusterInfo dbClusterInfo : dbClusterInfos) {
			System.out.println("master global");
			System.out.println(dbClusterInfo.getMasterGlobalDBInfo());

			System.out.println("slave global");
			for (DBInfo connInfo : dbClusterInfo.getSlaveGlobalDBInfo()) {
				System.out.println(connInfo);
			}

			for (DBClusterRegionInfo region : dbClusterInfo.getDbRegions()) {
				System.out.println("master sharding");
				for (DBInfo connInfo : region.getMasterDBInfos()) {
					System.out.println(connInfo);
				}

				System.out.println("slave sharding");
				List<List<DBInfo>> a = region.getSlaveDBInfos();
				for (List<DBInfo> b : a) {
					for (DBInfo c : b) {
						System.out.println(c);
					}
				}
			}
		}
	}

}
