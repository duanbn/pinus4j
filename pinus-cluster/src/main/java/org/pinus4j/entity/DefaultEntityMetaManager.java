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

package org.pinus4j.entity;

import java.io.File;
import java.io.FileFilter;
import java.lang.reflect.Field;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.pinus4j.constant.Const;
import org.pinus4j.entity.annotations.DateTime;
import org.pinus4j.entity.annotations.Index;
import org.pinus4j.entity.annotations.Indexes;
import org.pinus4j.entity.annotations.PrimaryKey;
import org.pinus4j.entity.annotations.Table;
import org.pinus4j.entity.annotations.UpdateTime;
import org.pinus4j.entity.meta.DBTable;
import org.pinus4j.entity.meta.DBTableColumn;
import org.pinus4j.entity.meta.DBTableIndex;
import org.pinus4j.entity.meta.DBTablePK;
import org.pinus4j.entity.meta.DataTypeBind;
import org.pinus4j.entity.meta.EntityPK;
import org.pinus4j.entity.meta.PKName;
import org.pinus4j.entity.meta.PKValue;
import org.pinus4j.exceptions.DBOperationException;
import org.pinus4j.utils.BeansUtil;
import org.pinus4j.utils.StringUtil;

import com.google.common.collect.Lists;

/**
 * 管理加载的Entity信息.
 * 
 * @author shanwei Jul 22, 2015 1:33:30 PM
 */
public class DefaultEntityMetaManager implements IEntityMetaManager {

    /**
     * 当前线程的类装载器. 用于扫描可以生成数据表的数据对象.
     */
    private final ClassLoader                   classloader = Thread.currentThread().getContextClassLoader();

    private final static Map<Class<?>, DBTable> tableMap    = new HashMap<Class<?>, DBTable>();

    private final static List<DBTable>          tables      = new ArrayList<DBTable>();

    private volatile static IEntityMetaManager  instance;

    private DefaultEntityMetaManager() {
    }

    /**
     * 获取对象实例.
     * 
     * @return
     */
    public static IEntityMetaManager getInstance() {
        if (instance == null) {
            synchronized (DefaultEntityMetaManager.class) {
                if (instance == null) {
                    instance = new DefaultEntityMetaManager();
                }
            }
        }
        return instance;
    }

    @Override
    public PKValue getNotUnionPkValue(Object obj) {
        PKName pkName = getNotUnionPkName(obj.getClass());

        Object pkValue = BeansUtil.getProperty(obj, pkName.getValue());

        return PKValue.valueOf(pkValue);
    }

    @Override
    public EntityPK getEntityPK(Object obj) {
        PKName[] pkNames = getPkName(obj.getClass());
        List<PKValue> pkValues = Lists.newArrayList();
        Object pkValue = null;
        for (PKName pkName : pkNames) {
            pkValue = BeansUtil.getProperty(obj, pkName.getValue());
            pkValues.add(PKValue.valueOf(pkValue));
        }
        return EntityPK.valueOf(pkNames, pkValues.toArray(new PKValue[pkValues.size()]));
    }

    @Override
    public PKName getNotUnionPkName(Class<?> clazz) {
        DBTable dbTable = this.getTableMeta(clazz);
        if (dbTable.isUnionPrimaryKey()) {
            throw new IllegalStateException("不支持联合主键, class=" + clazz);
        }

        List<DBTablePK> primaryKeys = dbTable.getPrimaryKeys();

        if (primaryKeys.isEmpty()) {
            throw new IllegalStateException("找不到主键 class=" + clazz);
        }

        return primaryKeys.get(0).getPKName();
    }

    @Override
    public PKName[] getPkName(Class<?> clazz) {
        DBTable dbTable = this.getTableMeta(clazz);

        List<DBTablePK> primaryKeys = dbTable.getPrimaryKeys();

        if (primaryKeys.isEmpty()) {
            throw new IllegalStateException("找不到主键 class=" + clazz);
        }

        List<PKName> ePKList = new ArrayList<PKName>(primaryKeys.size());
        for (DBTablePK primaryKey : primaryKeys) {
            ePKList.add(primaryKey.getPKName());
        }

        return ePKList.toArray(new PKName[ePKList.size()]);
    }

    @Override
    public boolean isShardingEntity(Class<?> clazz) {
        DBTable dbTable = this.getTableMeta(clazz);

        return dbTable.isSharding();
    }

    @Override
    public Object getShardingValue(Object entity) {
        Class<?> clazz = entity.getClass();
        DBTable dbTable = this.getTableMeta(clazz);

        String shardingField = dbTable.getShardingBy();
        Object shardingValue = null;
        try {
            shardingValue = BeansUtil.getProperty(entity, shardingField);
        } catch (Exception e) {
            throw new DBOperationException("获取sharding value失败, clazz=" + clazz + " field=" + shardingField);
        }
        if (shardingValue == null) {
            throw new IllegalStateException("shardingValue is null, clazz=" + clazz + " field=" + shardingField);
        }

        return shardingValue;
    }

    @Override
    public String getClusterName(Class<?> clazz) {
        DBTable dbTable = this.getTableMeta(clazz);

        return dbTable.getCluster();
    }

    @Override
    public int getTableNum(Class<?> clazz) {
        DBTable dbTable = this.getTableMeta(clazz);

        return dbTable.getShardingNum();
    }

    @Override
    public String getTableName(Object entity, int tableIndex) {
        Class<?> entityClass = entity.getClass();
        return getTableName(entityClass, tableIndex);
    }

    @Override
    public String getTableName(Class<?> clazz, int tableIndex) {
        if (tableIndex == -1) {
            return getTableName(clazz);
        } else {
            return getTableName(clazz) + tableIndex;
        }
    }

    @Override
    public String getTableName(Class<?> clazz) {
        DBTable dbTable = this.getTableMeta(clazz);

        return dbTable.getName();
    }

    @Override
    public boolean isCache(Class<?> clazz) {
        DBTable dbTable = this.getTableMeta(clazz);

        return dbTable.isCache();
    }

    @Override
    public boolean isUnionKey(Class<?> clazz) {
        DBTable dbTable = getTableMeta(clazz);
        return dbTable.isUnionPrimaryKey();
    }

    @Override
    public DBTablePK getAutoIncrementField(Class<?> clazz) {
        DBTable dbTable = getTableMeta(clazz);

        return dbTable.getAutoIncrementField();
    }

    @Override
    public void reloadEntity(String scanPackage) {
        synchronized (this) {
            tableMap.clear();
            tables.clear();

            loadEntity(scanPackage);
        }
    }

    /**
     * 扫描包并发现使用Table注解的对象.
     */
    @Override
    public void loadEntity(String scanPackage) {

        try {
            String pkgDirName = scanPackage.replace(".", "/");
            Enumeration<URL> dirs = classloader.getResources(pkgDirName);
            URL url = null;
            DBTable dbTable = null;
            while (dirs.hasMoreElements()) {
                url = dirs.nextElement();
                String protocol = url.getProtocol();
                if (protocol.equals("file")) {
                    String filePath = URLDecoder.decode(url.getFile(), "utf-8");
                    addClassesByFile(tables, scanPackage, filePath);
                } else if (protocol.equals("jar")) {
                    JarFile jar = null;
                    jar = ((JarURLConnection) url.openConnection()).getJarFile();
                    Enumeration<JarEntry> entries = jar.entries();
                    while (entries.hasMoreElements()) {
                        JarEntry entry = entries.nextElement();
                        String name = entry.getName();
                        if (name.charAt(0) == '/') {
                            name = name.substring(1);
                        }

                        if (!name.startsWith(pkgDirName)) {
                            continue;
                        }

                        if (name.endsWith(".class") && !entry.isDirectory()) {
                            String className = name.substring(scanPackage.length() + 1, name.length() - 6).replace("/",
                                    ".");
                            Class<?> tableClass = classloader.loadClass(scanPackage + "." + className);
                            if (tableClass.getAnnotation(Table.class) != null) {
                                dbTable = converTo(tableClass);
                                tables.add(dbTable);
                                tableMap.put(tableClass, dbTable);
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void addClassesByFile(List<DBTable> tables, String packageName, String packagePath)
            throws ClassNotFoundException {
        File dir = new File(packagePath);
        if (!dir.exists() || !dir.isDirectory()) {
            return;
        }

        File[] dirfiles = dir.listFiles(new FileFilter() {
            public boolean accept(File file) {
                return file.isDirectory() || file.getName().endsWith(".class");
            }
        });

        DBTable dbTable = null;
        for (File file : dirfiles) {
            if (file.isDirectory()) {
                addClassesByFile(tables, packageName + "." + file.getName(), file.getAbsolutePath());
            } else {
                String className = file.getName().substring(0, file.getName().length() - 6);
                Class<?> tableClass = classloader.loadClass(packageName + "." + className);
                if (tableClass.getAnnotation(Table.class) != null) {
                    dbTable = converTo(tableClass);
                    tables.add(dbTable);
                    tableMap.put(tableClass, dbTable);
                }
            }
        }
    }

    /**
     * 通过翻身将class转换为DBTable对象
     */
    protected DBTable converTo(Class<?> defClass) {
        if (defClass == null) {
            throw new IllegalArgumentException("被转化的Java对象不能为空");
        }

        Class<?> clazz;
        try {
            clazz = defClass.newInstance().getClass();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // 解析DBTable
        Table annoTable = clazz.getAnnotation(Table.class);
        if (annoTable == null) {
            throw new IllegalArgumentException(clazz + "无法转化为数据库，请使用@Table注解");
        }
        // 获取表名
        String tableName = StringUtil.isBlank(annoTable.name()) ? clazz.getSimpleName() : annoTable.name();
        DBTable table = new DBTable(tableName.toLowerCase());
        // 获取集群名
        String cluster = annoTable.cluster();
        if (StringUtil.isBlank(cluster)) {
            throw new IllegalArgumentException(clazz + " @Table的cluster不能为空");
        }
        table.setCluster(cluster);

        // 获取分片字段
        String shardingBy = annoTable.shardingBy();
        table.setShardingBy(shardingBy);

        // 获取分表数
        int shardingNum = annoTable.shardingNum();
        table.setShardingNum(shardingNum);

        // 是否需要被缓存
        boolean isCache = annoTable.cache();
        table.setCache(isCache);

        // 解析DBIndex
        _parseDBIndex(table, clazz);

        // 解析DBTableColumn
        DBTablePK primaryKey = null;
        DBTableColumn column = null;
        org.pinus4j.entity.annotations.Field dbField = null;
        PrimaryKey pk = null;
        UpdateTime updateTime = null;
        DateTime datetime = null;
        for (Field f : clazz.getDeclaredFields()) {
            //
            // Datatime
            //
            datetime = f.getAnnotation(DateTime.class);
            if (datetime != null) {

                if (f.getType() != Date.class) {
                    throw new IllegalArgumentException(clazz + " " + f.getName() + " " + f.getType() + " 无法转化为日期字段");
                }

                String fieldName = f.getName();
                if (StringUtil.isNotBlank(datetime.name())) {
                    fieldName = datetime.name();
                }
                BeansUtil.putAliasField(clazz, fieldName, f);

                column = new DBTableColumn();
                column.setField(fieldName);
                column.setType(DataTypeBind.DATETIME.getDBType());
                column.setHasDefault(datetime.hasDefault());
                if (column.isHasDefault())
                    column.setDefaultValue(DataTypeBind.DATETIME.getDefaultValue());
                column.setComment(datetime.comment());

                table.addColumn(column);
            }

            //
            // UpdateTime
            //
            updateTime = f.getAnnotation(UpdateTime.class);
            if (updateTime != null) {

                if (f.getType() != java.sql.Timestamp.class) {
                    throw new IllegalArgumentException(clazz + " " + f.getName() + " " + f.getType() + " 无法转化为时间戳字段");
                }

                String fieldName = f.getName();
                if (StringUtil.isNotBlank(updateTime.name())) {
                    fieldName = updateTime.name();
                }
                BeansUtil._aliasFieldCache.put(clazz.getName() + fieldName, f);

                column = new DBTableColumn();
                column.setField(fieldName);
                column.setType(DataTypeBind.UPDATETIME.getDBType());
                column.setHasDefault(true);
                column.setDefaultValue(DataTypeBind.UPDATETIME.getDefaultValue());
                column.setComment(updateTime.comment());

                table.addColumn(column);
            }

            //
            // Field
            //
            dbField = f.getAnnotation(org.pinus4j.entity.annotations.Field.class);
            if (dbField != null) {

                if (f.getType() == java.sql.Timestamp.class) {
                    throw new IllegalArgumentException(clazz + " " + f.getName() + "应该是时间戳类型，必须使用@UpdateTime标注");
                }

                String fieldName = f.getName();
                if (StringUtil.isNotBlank(dbField.name())) {
                    fieldName = dbField.name();
                }
                BeansUtil._aliasFieldCache.put(clazz.getName() + fieldName, f);

                boolean isCanNull = dbField.isCanNull();
                int length = _getLength(f, dbField.length());
                boolean hasDefault = dbField.hasDefault();

                column = new DBTableColumn();
                column.setField(fieldName);
                column.setType(DataTypeBind.getEnum(f.getType()).getDBType());
                column.setCanNull(isCanNull);
                column.setLength(length);
                column.setHasDefault(hasDefault);
                column.setComment(dbField.comment());
                if (column.isHasDefault())
                    column.setDefaultValue(DataTypeBind.getEnum(f.getType()).getDefaultValue());

                // 如果字符串长度超过指定长度则使用text类型
                if (column.getType().equals(DataTypeBind.STRING.getDBType())
                        && column.getLength() > Const.COLUMN_TEXT_LENGTH) {
                    column.setType(DataTypeBind.TEXT.getDBType());
                    column.setHasDefault(false); // text default value gen by pinus, not db.
                    column.setLength(0);
                    column.setDefaultValue(DataTypeBind.TEXT.getDefaultValue());
                }

                // 如果字段为boolean则长度为1
                if (column.getType().equals(DataTypeBind.BOOL.getDBType())) {
                    column.setLength(1);
                }

                table.addColumn(column);
            }

            //
            // PrimaryKey
            //
            pk = f.getAnnotation(PrimaryKey.class);
            if (pk != null) {
                String fieldName = f.getName();
                if (StringUtil.isNotBlank(pk.name())) {
                    fieldName = pk.name();
                }
                BeansUtil._aliasFieldCache.put(clazz.getName() + fieldName, f);

                primaryKey = new DBTablePK();
                primaryKey.setField(fieldName);
                DataTypeBind dbType = DataTypeBind.getEnum(f.getType());
                primaryKey.setType(dbType.getDBType());
                int length = _getLength(f, pk.length());
                primaryKey.setLength(length);
                primaryKey.setComment(pk.comment());
                primaryKey.setAutoIncrement(pk.isAutoIncrement());
                table.addPrimaryKey(primaryKey);
                // primary key also is a table column
                table.addColumn(primaryKey.toTableColumn());
            }
        }

        // check primary key
        table.checkPrimaryKey();

        if (table.getColumns().isEmpty()) {
            throw new IllegalStateException(clazz + "被转化的java对象没有包含任何列属性" + defClass);
        }

        return table;
    }

    /**
     * 解析@Indexes注解.
     */
    private static void _parseDBIndex(DBTable table, Class<?> clazz) {
        Indexes annoIndexes = clazz.getAnnotation(Indexes.class);
        if (annoIndexes == null) {
            return;
        }

        Index[] indexes = annoIndexes.value();
        if (indexes == null || indexes.length <= 0) {
            throw new IllegalArgumentException("索引注解错误, " + clazz);
        }
        DBTableIndex dbIndex = null;
        for (Index index : indexes) {
            dbIndex = new DBTableIndex();
            dbIndex.setField(StringUtil.removeBlank(index.field()));
            dbIndex.setUnique(index.isUnique());
            table.addIndex(dbIndex);
        }
    }

    private static int _getLength(Field f, int annoLength) {
        int length = annoLength;
        if (length > 0) {
            return length;
        }

        DataTypeBind dbType = DataTypeBind.getEnum(f.getType());

        switch (dbType) {
            case STRING:
                length = 255;
                break;
            case BYTE:
                length = 4;
                break;
            case SHORT:
                length = 6;
                break;
            case INT:
                length = 11;
                break;
            case LONG:
                length = 20;
                break;
            default:
                break;
        }
        return length;
    }

    @Override
    public DBTable getTableMeta(Class<?> clazz) {
        DBTable dbTable = tableMap.get(clazz);

        if (dbTable == null) {
            throw new IllegalStateException("找不到实体的元信息 class=" + clazz);
        }

        return dbTable;
    }

    @Override
    public List<DBTable> getTableMetaList() {
        return tables;
    }

}
