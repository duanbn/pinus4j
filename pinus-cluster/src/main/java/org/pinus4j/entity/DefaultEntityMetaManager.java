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
import org.pinus4j.utils.ReflectUtil;
import org.pinus4j.utils.StringUtils;

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
        String tableName = StringUtils.isBlank(annoTable.name()) ? clazz.getSimpleName() : annoTable.name();
        DBTable table = new DBTable(tableName.toLowerCase());
        // 获取集群名
        String cluster = annoTable.cluster();
        if (StringUtils.isBlank(cluster)) {
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
                if (StringUtils.isNotBlank(datetime.name())) {
                    fieldName = datetime.name();
                }
                ReflectUtil.putAliasField(clazz, fieldName, f);

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
                if (StringUtils.isNotBlank(updateTime.name())) {
                    fieldName = updateTime.name();
                }
                ReflectUtil._aliasFieldCache.put(clazz.getName() + fieldName, f);

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
                if (StringUtils.isNotBlank(dbField.name())) {
                    fieldName = dbField.name();
                }
                ReflectUtil._aliasFieldCache.put(clazz.getName() + fieldName, f);

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
                if (StringUtils.isNotBlank(pk.name())) {
                    fieldName = pk.name();
                }
                ReflectUtil._aliasFieldCache.put(clazz.getName() + fieldName, f);

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
            dbIndex.setField(StringUtils.removeBlank(index.field()));
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
