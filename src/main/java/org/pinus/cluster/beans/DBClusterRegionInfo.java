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

	private List<DBInfo> masterDBInfos;

	private List<List<DBInfo>> slaveDBInfos;

	@Override
	public String toString() {
		return "DBClusterRegionInfo [start=" + start + ", end=" + end + ", masterConnection=" + masterDBInfos
				+ ", slaveConnection=" + slaveDBInfos + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (end ^ (end >>> 32));
		result = prime * result + ((masterDBInfos == null) ? 0 : masterDBInfos.hashCode());
		result = prime * result + ((slaveDBInfos == null) ? 0 : slaveDBInfos.hashCode());
		result = prime * result + (int) (start ^ (start >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DBClusterRegionInfo other = (DBClusterRegionInfo) obj;
		if (end != other.end)
			return false;
		if (masterDBInfos == null) {
			if (other.masterDBInfos != null)
				return false;
		} else if (!masterDBInfos.equals(other.masterDBInfos))
			return false;
		if (slaveDBInfos == null) {
			if (other.slaveDBInfos != null)
				return false;
		} else if (!slaveDBInfos.equals(other.slaveDBInfos))
			return false;
		if (start != other.start)
			return false;
		return true;
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

	public List<DBInfo> getMasterDBInfos() {
		return masterDBInfos;
	}

	public void setMasterDBInfos(List<DBInfo> masterDBInfos) {
		this.masterDBInfos = masterDBInfos;
	}

	public List<List<DBInfo>> getSlaveDBInfos() {
		return slaveDBInfos;
	}

	public void setSlaveDBInfos(List<List<DBInfo>> slaveDBInfos) {
		this.slaveDBInfos = slaveDBInfos;
	}

}
