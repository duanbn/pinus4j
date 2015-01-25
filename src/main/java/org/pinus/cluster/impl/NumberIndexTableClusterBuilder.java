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

package org.pinus.cluster.impl;

import java.util.List;

import org.pinus.cluster.ITableCluster;
import org.pinus.cluster.ITableClusterBuilder;
import org.pinus.cluster.beans.TableNumberInfo;
import org.pinus.generator.beans.DBTable;

/**
 * builder implement for table cluster.
 *
 * @author duanbn
 * @since 1.0.0
 */
public class NumberIndexTableClusterBuilder implements ITableClusterBuilder {

	private List<DBTable> tables;

	private NumberIndexTableClusterBuilder() {
	}

	public static ITableClusterBuilder valueOf(List<DBTable> tables) {
		NumberIndexTableClusterBuilder builder = new NumberIndexTableClusterBuilder();
		builder.setTables(tables);
		return builder;
	}

	@Override
	public ITableCluster build() {
		NumberIndexTableCluster tableCluster = new NumberIndexTableCluster();

		// init
		String clusterName = null;
		TableNumberInfo tableNumberInfo = null;
		for (DBTable dbTable : tables) {
			clusterName = dbTable.getCluster();
			tableNumberInfo = tableCluster.getTableNumberInfo(clusterName);
			if (tableNumberInfo == null) {
				tableNumberInfo = TableNumberInfo.valueOf(clusterName);
				tableCluster.addTableNumberInfo(clusterName, tableNumberInfo);
			}
			tableNumberInfo.add(dbTable.getName(), dbTable.getShardingNum());
		}

		return tableCluster;
	}

	public void setTables(List<DBTable> tables) {
		this.tables = tables;
	}

}
