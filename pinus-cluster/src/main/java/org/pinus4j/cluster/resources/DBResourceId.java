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

package org.pinus4j.cluster.resources;

import org.pinus4j.cluster.enums.EnumDBMasterSlave;

/**
 * database resource id implements.
 *
 * @author duanbn
 * @since 1.1.0
 */
public class DBResourceId implements IResourceId {

    private String clusterName;

    private String dbName;

    private long regionStart;

    private long regionEnd;

    private String tableName;

    private int tableIndex;

    private EnumDBMasterSlave masterSlave;

    public DBResourceId(String clusterName, String dbName, String tableName, EnumDBMasterSlave masterSlave) {
        this(clusterName, dbName, -1, -1, tableName, -1, masterSlave);
    }

    public DBResourceId(String clusterName, String dbName, long regionStart, long regionEnd, String tableName, int tableIndex,
            EnumDBMasterSlave masterSlave) {
        this.clusterName = clusterName;
        this.dbName = dbName;
        this.regionStart = regionStart;
        this.regionEnd = regionEnd;
        this.tableName = tableName;
        this.tableIndex = tableIndex;
        this.masterSlave = masterSlave;
    }

    @Override
    public String value() {
        StringBuilder value = new StringBuilder();
        value.append(this.clusterName);
        value.append(this.dbName);
        value.append(this.regionStart);
        value.append(this.regionEnd);
        value.append(this.tableName);
        value.append(this.tableIndex);
        value.append(this.masterSlave.getValue());
        return value.toString();
    }

    public String getClusterName() {
        return clusterName;
    }
    
    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }
    
    public String getDbName() {
        return dbName;
    }
    
    public void setDbName(String dbName) {
        this.dbName = dbName;
    }
    
    public long getRegionStart() {
        return regionStart;
    }
    
    public void setRegionStart(long regionStart) {
        this.regionStart = regionStart;
    }
    
    public long getRegionEnd() {
        return regionEnd;
    }
    
    public void setRegionEnd(long regionEnd) {
        this.regionEnd = regionEnd;
    }
    
    public String getTableName() {
        return tableName;
    }
    
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
    
    public int getTableIndex() {
        return tableIndex;
    }
    
    public void setTableIndex(int tableIndex) {
        this.tableIndex = tableIndex;
    }
    
    public EnumDBMasterSlave getMasterSlave() {
        return masterSlave;
    }
    
    public void setMasterSlave(EnumDBMasterSlave masterSlave) {
        this.masterSlave = masterSlave;
    }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((clusterName == null) ? 0 : clusterName.hashCode());
		result = prime * result + ((dbName == null) ? 0 : dbName.hashCode());
		result = prime * result
				+ ((masterSlave == null) ? 0 : masterSlave.hashCode());
		result = prime * result + (int) (regionEnd ^ (regionEnd >>> 32));
		result = prime * result + (int) (regionStart ^ (regionStart >>> 32));
		result = prime * result + tableIndex;
		result = prime * result
				+ ((tableName == null) ? 0 : tableName.hashCode());
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
		DBResourceId other = (DBResourceId) obj;
		if (clusterName == null) {
			if (other.clusterName != null)
				return false;
		} else if (!clusterName.equals(other.clusterName))
			return false;
		if (dbName == null) {
			if (other.dbName != null)
				return false;
		} else if (!dbName.equals(other.dbName))
			return false;
		if (masterSlave != other.masterSlave)
			return false;
		if (regionEnd != other.regionEnd)
			return false;
		if (regionStart != other.regionStart)
			return false;
		if (tableIndex != other.tableIndex)
			return false;
		if (tableName == null) {
			if (other.tableName != null)
				return false;
		} else if (!tableName.equals(other.tableName))
			return false;
		return true;
	}
}
