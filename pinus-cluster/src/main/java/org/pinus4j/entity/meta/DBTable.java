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

package org.pinus4j.entity.meta;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.pinus4j.exceptions.DBPrimaryKeyException;
import org.pinus4j.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 数据库表bean. 对数据库表的抽象.
 * 
 * @author duanbn
 */
public class DBTable implements Serializable {

    /**
     * 
     */
    private static final long   serialVersionUID = 1L;

    /**
     * 日志.
     */
    public static final Logger  LOG              = LoggerFactory.getLogger(DBTable.class);

    /**
     * 表所在的集群.
     */
    private String              cluster;

    /**
     * 表名
     */
    private String              name;

    /**
     * 表下标.
     */
    private int                 tableIndex       = -1;

    /**
     * 分片字段.
     */
    private String              shardingBy;

    /**
     * 分表数
     */
    private int                 shardingNum;

    /**
     * 是否需要被缓存
     */
    private boolean             isCache;

    private String              cacheVersion;

    /**
     * 主键
     */
    private List<DBTablePK>     primaryKeys      = new ArrayList<DBTablePK>();

    /**
     * 表中的列.
     */
    private List<DBTableColumn> columns          = new ArrayList<DBTableColumn>();

    /**
     * 包含的索引.
     */
    private List<DBTableIndex>  indexes          = new ArrayList<DBTableIndex>();

    /**
     * 判断是否是联合主键
     * 
     * @return
     */
    public boolean isUnionPrimaryKey() {
        return this.primaryKeys.size() > 1;
    }

    /**
     * 判断是否是sharding表
     * 
     * @return
     */
    public boolean isSharding() {
        return StringUtil.isNotBlank(this.shardingBy) && this.shardingNum > 0;
    }

    /**
     * 校验主键配置
     */
    public void checkPrimaryKey() {
        if (primaryKeys.isEmpty()) {
            throw new DBPrimaryKeyException("必须指定至少一个字段为主键, cluster=" + this.cluster + ", name=" + this.name);
        } else {
            int pkAICount = 0;
            for (DBTablePK primaryKey : primaryKeys) {
                if (primaryKey.isAutoIncrement()) {
                    if (++pkAICount > 1) {
                        throw new DBPrimaryKeyException("联合主键只能有一个主键自增, cluster=" + this.cluster + ", name="
                                + this.name);
                    }
                    if (DataTypeBind.getEnum(primaryKey.getType()) != DataTypeBind.INT
                            && DataTypeBind.getEnum(primaryKey.getType()) != DataTypeBind.LONG) {
                        throw new DBPrimaryKeyException("自增主键必须是int或者long类型, cluster=" + this.cluster + ", name="
                                + this.name);
                    }
                }
            }
        }
    }

    /**
     * 构造方法
     * 
     * @param name 表名
     */
    public DBTable(String name) {
        this.name = name;
    }

    public String getNameWithIndex() {
        if (tableIndex > -1) {
            return this.name + tableIndex;
        }
        return name;
    }

    public void addPrimaryKey(DBTablePK primaryKey) {
        this.primaryKeys.add(primaryKey);
    }

    public void addColumn(DBTableColumn column) {
        this.columns.add(column);
    }

    public void addIndex(DBTableIndex index) {
        this.indexes.add(index);
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<DBTablePK> getPrimaryKeys() {
        return primaryKeys;
    }

    public void setPrimaryKeys(List<DBTablePK> primaryKeys) {
        this.primaryKeys = primaryKeys;
    }

    public List<DBTableColumn> getColumns() {
        return columns;
    }

    public void setColumns(List<DBTableColumn> columns) {
        this.columns = columns;
    }

    public Map<String, DBTablePK> getPrimaryKeyMap() {
        Map<String, DBTablePK> primaryKeyMap = new HashMap<String, DBTablePK>(primaryKeys.size());
        for (DBTablePK parimaryKey : this.primaryKeys) {
            primaryKeyMap.put(parimaryKey.getField(), parimaryKey);
        }
        return primaryKeyMap;
    }

    public Map<String, DBTableColumn> getColumnMap() {
        Map<String, DBTableColumn> columnMap = new HashMap<String, DBTableColumn>(columns.size());
        for (DBTableColumn column : this.columns) {
            columnMap.put(column.getField(), column);
        }
        return columnMap;
    }

    public Map<String, DBTableIndex> getIndexMap() {
        Map<String, DBTableIndex> indexMap = new HashMap<String, DBTableIndex>(indexes.size());
        for (DBTableIndex index : this.indexes) {
            indexMap.put(index.getIndexName(), index);
        }
        return indexMap;
    }

    /**
     * 生成此表的创建SQL语句.
     * 
     * @return SQL语句.
     */
    public String[] getCreateSQL() {
        List<String> sqls = new ArrayList<String>();

        StringBuilder sql = new StringBuilder();
        // 创建表
        sql.append("CREATE TABLE `" + getNameWithIndex()).append("` (");
        for (DBTableColumn column : this.columns) {
            sql.append(_sqlFieldPhrase(column)).append(",");
        }
        sql.append(" PRIMARY KEY(" + _sqlPrimaryKey() + ")");
        sql.append(") ENGINE=InnoDB DEFAULT CHARSET=utf8;");
        sqls.add(sql.toString());

        // 创建索引
        for (DBTableIndex dbIndex : indexes) {
            sql.setLength(0);
            sql.append(_sqlCreateIndex(dbIndex));
            sqls.add(sql.toString());
        }
        return sqls.toArray(new String[sqls.size()]);
    }

    /**
     * 获取修改的sql语句.
     * 
     * @param existTable 已经存在的表结构
     * @param isDelete 多余的列是否删除.
     * @return
     */
    public String[] getAlterSQL(DBTable existTable, boolean isDelete) {
        if (!existTable.getNameWithIndex().equals(getNameWithIndex())) {
            return new String[0];
        }

        List<String> sqls = new ArrayList<String>();

        // 同步主键
        boolean isPrimaryKeyChanged = false;
        Map<String, DBTablePK> dbPrimaryKeyMap = existTable.getPrimaryKeyMap();
        DBTablePK primaryKey = null;
        if (this.primaryKeys.size() != dbPrimaryKeyMap.size()) {
            isPrimaryKeyChanged = true;
        } else {
            for (DBTablePK entityPrimaryKey : this.primaryKeys) {
                primaryKey = dbPrimaryKeyMap.get(entityPrimaryKey.getField());
                if (primaryKey == null) {
                    isPrimaryKeyChanged = true;
                    break;
                }
            }
        }
        if (isPrimaryKeyChanged) {
            String updatePkSql = "ALTER TABLE `" + this.getNameWithIndex() + "` DROP PRIMARY KEY,ADD PRIMARY KEY ("
                    + _sqlPrimaryKey() + ");";
            sqls.add(updatePkSql);
        }

        // 同步字段
        Map<String, DBTableColumn> dbColumnMap = existTable.getColumnMap();
        DBTableColumn dbColumn = null;
        for (DBTableColumn entityColumn : this.columns) {
            dbColumn = dbColumnMap.get(entityColumn.getField());
            if (dbColumn != null) {
                dbColumnMap.remove(entityColumn.getField());
            }
            if (dbColumn == null) {
                String addSql = "ALTER TABLE `" + this.getNameWithIndex() + "` ADD COLUMN "
                        + _sqlFieldPhrase(entityColumn) + ";";
                sqls.add(addSql);
            } else if (!entityColumn.equals(dbColumn)) {
                String modifySql = "ALTER TABLE `" + this.getNameWithIndex() + "` MODIFY "
                        + _sqlFieldPhrase(entityColumn) + ";";
                sqls.add(modifySql);
            }
        }

        // 同步索引
        Map<String, DBTableIndex> dbIndexMap = existTable.getIndexMap();
        DBTableIndex dbIndex = null;
        for (DBTableIndex entityIndex : this.indexes) {
            dbIndex = dbIndexMap.get(entityIndex.getIndexName());
            if (dbIndex != null) {
                dbIndexMap.remove(entityIndex.getIndexName());
            }
            if (dbIndex == null) {
                sqls.add(_sqlCreateIndex(entityIndex));
            } else if (!entityIndex.equals(dbIndex)) {
                sqls.add("DROP INDEX `" + dbIndex.getIndexName() + " ON " + this.getNameWithIndex() + "`");
                sqls.add(_sqlCreateIndex(entityIndex));
            }
        }

        // 删除多余的列
        if (isDelete) {
            for (String field : dbColumnMap.keySet()) {
                sqls.add("ALTER TABLE `" + this.getNameWithIndex() + "` DROP COLUMN `" + field + "`;");
            }
            for (DBTableIndex index : dbIndexMap.values()) {
                sqls.add("DROP INDEX `" + index.getIndexName() + "` ON `" + this.getNameWithIndex() + "`");
            }
        }

        return sqls.toArray(new String[sqls.size()]);
    }

    /**
     * 创建索引SQL语句.
     */
    private String _sqlCreateIndex(DBTableIndex index) {
        StringBuilder indexSql = new StringBuilder();

        if (index.isUnique()) {
            indexSql.append("CREATE UNIQUE INDEX");
        } else {
            indexSql.append("CREATE INDEX");
        }

        if (index.getFields() == null || index.getFields().isEmpty()) {
            throw new IllegalArgumentException("索引注解格式错误，field不能为空");
        }
        StringBuilder indexFields = new StringBuilder();
        for (String field : index.getFields()) {
            indexFields.append('`').append(field).append('`').append(",");
        }
        indexFields.deleteCharAt(indexFields.length() - 1);

        indexSql.append(" `").append(index.getIndexName()).append("` ON").append(" `").append(this.getNameWithIndex());
        indexSql.append("` (").append(indexFields.toString()).append(");");

        return indexSql.toString();
    }

    private String _sqlPrimaryKey() {
        StringBuilder primaryKey = new StringBuilder();
        for (DBTablePK pk : this.primaryKeys) {
            primaryKey.append('`').append(pk.getField()).append('`').append(',');
        }
        primaryKey.deleteCharAt(primaryKey.length() - 1);
        return primaryKey.toString();
    }

    /**
     * 生成单列的SQL语句.
     * 
     * @param column 列对象
     * @return 单列的SQL语句.
     */
    private String _sqlFieldPhrase(DBTableColumn column) {
        StringBuilder pharse = new StringBuilder();

        pharse.append('`').append(column.getField()).append("` ");
        switch (DataTypeBind.getEnum(column.getType())) {
            case UPDATETIME:
                pharse.append("timestamp");
                pharse.append(" NOT NULL");
                pharse.append(" DEFAULT " + column.getDefaultValue());
                // 修改为通过框架控制:
                // pharse.append(" DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP");
                break;
            case DATETIME:
                pharse.append("datetime");
                if (!column.isCanNull()) {
                    pharse.append(" NOT NULL");
                }
                if (column.isHasDefault())
                    pharse.append(" DEFAULT " + column.getDefaultValue());
                break;
            default:
                pharse.append(column.getType());
                if (column.getLength() > 0) {
                    pharse.append("(" + column.getLength() + ")");
                }

                if (!column.isCanNull()) {
                    pharse.append(" NOT NULL");
                }

                if (column.isAutoIncrement()) {
                    pharse.append(" AUTO_INCREMENT");
                }

                if (column.getDefaultValue() != null)
                    pharse.append(" DEFAULT " + column.getDefaultValue());
                break;
        }
        pharse.append(" COMMENT '").append(column.getComment()).append("'");

        return pharse.toString();
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public int getTableIndex() {
        return tableIndex;
    }

    public void setTableIndex(int tableIndex) {
        this.tableIndex = tableIndex;
    }

    public int getShardingNum() {
        return shardingNum;
    }

    public void setShardingNum(int shardingNum) {
        this.shardingNum = shardingNum;
    }

    public String getShardingBy() {
        return shardingBy;
    }

    public void setShardingBy(String shardingBy) {
        this.shardingBy = shardingBy;
    }

    public boolean isCache() {
        return isCache;
    }

    public void setCache(boolean isCache) {
        this.isCache = isCache;
    }

    public String getCacheVersion() {
        return cacheVersion;
    }

    public void setCacheVersion(String cacheVersion) {
        this.cacheVersion = cacheVersion;
    }

    @Override
    public String toString() {
        return "DBTable [cluster=" + cluster + ", name=" + name + ", tableIndex=" + tableIndex + ", shardingBy="
                + shardingBy + ", shardingNum=" + shardingNum + ", isCache=" + isCache + ", primaryKeys=" + primaryKeys
                + ", columns=" + columns + ", indexes=" + indexes + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((cluster == null) ? 0 : cluster.hashCode());
        result = prime * result + ((columns == null) ? 0 : columns.hashCode());
        result = prime * result + ((indexes == null) ? 0 : indexes.hashCode());
        result = prime * result + (isCache ? 1231 : 1237);
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((primaryKeys == null) ? 0 : primaryKeys.hashCode());
        result = prime * result + ((shardingBy == null) ? 0 : shardingBy.hashCode());
        result = prime * result + shardingNum;
        result = prime * result + tableIndex;
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
        DBTable other = (DBTable) obj;
        if (cluster == null) {
            if (other.cluster != null)
                return false;
        } else if (!cluster.equals(other.cluster))
            return false;
        if (columns == null) {
            if (other.columns != null)
                return false;
        } else if (!columns.equals(other.columns))
            return false;
        if (indexes == null) {
            if (other.indexes != null)
                return false;
        } else if (!indexes.equals(other.indexes))
            return false;
        if (isCache != other.isCache)
            return false;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        if (primaryKeys == null) {
            if (other.primaryKeys != null)
                return false;
        } else if (!primaryKeys.equals(other.primaryKeys))
            return false;
        if (shardingBy == null) {
            if (other.shardingBy != null)
                return false;
        } else if (!shardingBy.equals(other.shardingBy))
            return false;
        if (shardingNum != other.shardingNum)
            return false;
        if (tableIndex != other.tableIndex)
            return false;
        return true;
    }

}
