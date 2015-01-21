/**
 * Copyright 2014 Duan Bingnan
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 *   
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pinus.cluster.beans;

import java.util.List;

/**
 * 集群区块. 一个区块中存在多个分片。
 * 
 * @author duanbn
 * 
 */
public class DBClusterRegionInfo {

	private long start;

	private long end;

	private List<DBInfo> masterConnection;

	private List<List<DBInfo>> slaveConnection;

	@Override
	public String toString() {
		return "DBClusterRegionInfo [start=" + start + ", end=" + end + ", masterConnection=" + masterConnection
				+ ", slaveConnection=" + slaveConnection + "]";
	}

	public long getStart() {
		return start;
	}

	public void setStart(long start) {
		this.start = start;
	}

	public long getEnd() {
		return end;
	}

	public void setEnd(long end) {
		this.end = end;
	}

	public List<DBInfo> getMasterConnection() {
		return masterConnection;
	}

	public void setMasterConnection(List<DBInfo> masterConnection) {
		this.masterConnection = masterConnection;
	}

	public List<List<DBInfo>> getSlaveConnection() {
		return slaveConnection;
	}

	public void setSlaveConnection(List<List<DBInfo>> slaveConnection) {
		this.slaveConnection = slaveConnection;
	}

}
