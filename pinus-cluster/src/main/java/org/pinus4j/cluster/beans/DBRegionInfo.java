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

package org.pinus4j.cluster.beans;

import java.util.List;

/**
 * 集群区块. 一个区块中存在多个分片。
 * 
 * @author duanbn
 * 
 */
public class DBRegionInfo {

	private String capacity;

	private List<Value> values;

	private List<DBInfo> masterDBInfos;

	private List<List<DBInfo>> slaveDBInfos;

	public boolean isMatch(long key) {
		for (Value value : values) {
			if (value.start <= key && value.end >= key) {
				return true;
			}
		}
		return false;
	}

	public String getCapacity() {
		return capacity;
	}

	public void setCapacity(String capacity) {
		this.capacity = capacity;
	}

	public List<Value> getValues() {
		return values;
	}

	public void setValues(List<Value> values) {
		this.values = values;
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

	public static class Value {
		public long start;
		public long end;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((capacity == null) ? 0 : capacity.hashCode());
		result = prime * result + ((masterDBInfos == null) ? 0 : masterDBInfos.hashCode());
		result = prime * result + ((slaveDBInfos == null) ? 0 : slaveDBInfos.hashCode());
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
		DBRegionInfo other = (DBRegionInfo) obj;
		if (capacity == null) {
			if (other.capacity != null)
				return false;
		} else if (!capacity.equals(other.capacity))
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
		return true;
	}

	@Override
	public String toString() {
		return "DBRegionInfo [capacity=" + capacity + ", masterDBInfos=" + masterDBInfos + ", slaveDBInfos="
				+ slaveDBInfos + "]";
	}

}
