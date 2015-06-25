---
layout: default
---

# 版本更新日志

### version 1.1.1

* 添加对Redis缓存的支持 (TODO)

* 引入hsqldb来完善测试 (TODO)

* 增强定制路由特性

配置中的 sharding 节点上添加自定义属性，自定义属性在 Pinus 启动时会被加载到 DBInfo 对象中，当用户在定制集群路由器时可以通过自定义属性更灵活的进行分库。

配置中的 region 节点的 capacity 属性可以设置多个范围，范围值之间使用英文半角逗号分隔。

* 自动读写分离

给所有的查询接口添加自动读写分离特性；缓存可用时优先查找缓存，存在从库时查询从库，从库未找到再查询主库。

* 新增集群查询接口

添加 findByPk、findByPkList、findBySql、findByQuery 接口，当实体对象保存在全局库中时效果等同于 findGlobalXXX 接口，当实体对象保存在分片库中时，会查询所有分片并将结果集合并。

* 修复不分bug

### version 1.1.0
* 支持事务处理

使用best efforts 1pc事务处理
    
* 支持设置本地事务的隔离级别

代码重构，添加DB资源对象，将DB对象重构为ShardingDBResource，添加GlobalDBResource

事务相关的操作交给ITransaction实现类完成

* 性能优化

优化查找数据库资源的性能

### version 1.0.2
* fix bug 
由于修改了包名而没有修改cache和router的fullpath路径
* 修改配置文件中cluster的属性
将设置router的属性从class修改为router
* 重构某些单元测试类

### version 1.0.1

1.修改包名为pinus4j

### version 0.7.1

1.代码重构

2.用户自定义分库路由规则

### version 0.7.0
1.在zookeeper中创建pinus目录，将pinus相关的信息放在此目录下.

2.添加Apache License

3.修改包名为org，将log4j替换为slf4j

4.添加简单的数据处理框架,提供单进程处理数据的方式，目前只能简单的处理单个数据对象

5.支持设置实体数据的主键值

### version 0.6.9
1.支持基于IQuery级别的二级缓存

2.优化缓存模块，添加默认缓存过期时间

* 3.fix bug
3.1.部分缓存命中是取值为null
3.2.in查询按顺序返回

4.清理过期的代码

5.支持不使用缓存的一套查询接口

6.支持实体与数据表结构同步

### version 0.6.8
1.添加基于curator framework的分布式锁实现

2.之前版本的DistributeLock被标记为过去，不建议继续使用

3.数据库连接需要优化，支持Java容器连接池

4.fix bug

5.实现0.6.0版本之后的从库实现

### version 0.6.7
1.fix bug
2.添加防止SQL注入功能
3.修复无法使用装箱的基本数据

### version 0.6.6
1.fix bug

2.添加根据查询条件获取全局表数量

3.fix Text类型没有默认值的问题.

4.允许设置多个扫描包来加载实体对象.

### version 0.6.5
1.代码优化

2.在IQuery中添加对某些field的查询.

* 完善根据sql语句进行查询, 支持同一分片数据的多表查询
3.1.此类查询无法使用缓存.

### version 0.6.4
1.每个数据实体的分片信息在进程启动时写入zookeeper中(Done)

2.开发pinus-cli工具方便对数据库的查询(Done)

3.调整项目的目录结构添加contrib目录(Done)

4.代码重构，DBCluster支持加载指定路径的配置文件 (Done)

5.添加FashionEntity对象, 继承此对象的Entity具备写入数据库的能力(Done)

  5.1.save()
  
  5.2.update()
  
  5.3.saveOrUpdate()
  
  5.4.remove()
  
6.在IQuery中添加对某些field的查询，此类查询先不走缓存(Done)

### version 0.6.3
1.去除了ISharingEntity和IGlobalEntity接口，改为使用注解代替

2.优化update查询

3.修复update时缓存脏数据的bug

### version 0.6.2

1.允许集群中不存在全局库，某些集群不需要全局信息

2.添加IShardingStorageClient可以创建集群锁

### version 0.6.1
1.添加对redis缓存的支持(TODO)

2.去除集群数据遍历器，在分布式环境下此实现遍历器性能较低切使用场景不多 (Done)

3.添加根据实体对象获取此对象所有的分库分表引用 (Done)

4.添加FatDB对象，此对象提供查询接口(Done)

5.重构DB对象，不直接引用Connection而是引用DataSource对象 (Done)

6.当使用String做Sharding时由于String做哈希之后的值做归一化(TODO)

7.修改SQL日志格式

8.遗留问题

  8.1.当使用字符串进行sharding时，由于哈希之后的值无法确定，因此可能超过最大的region范围!!!
  
    每个hash算法都有一个最大值，一个集群需要覆盖这个hash值
    
  8.2.当添加region时，已有的哈希算法如何处理？
  
    只要已有的region的库表数量不变就行

### version 0.6.0
1.重构IQuery代码，支持查询某些字段

2.添加遍历集群数据接口

2.1 FIXME 注意此接口目前是非线程安全的，因此不支持多线程调用和分布式调用，在后期版本里将支持此特性

3.添加读取单个实体集群总数接口

4.从库接口目前没有实现 (TODO)

### version 0.5.0
1.添加线性扩展特性
  引入Region的概念，每个集群可以有一个或者多个Region用来管理数据
  
### version 0.4.4
1. 去掉saveOrUpdate接口，此接口不属于基本操作，因此去掉

2. 重要修复cache相关的bug
  
### version 0.4.3
1. 配置文件使用xml代替目前的properties文件

### version 0.4.2
1. 添加IGlobalEntity和IShardingEntity接口用于标记实体对象是全局的还是分库分表的.

2. globalSave、globalUpdate、globalSaveOrUpdate、save、update、saveOrUpdate接口

3.代码重构

  3.1.IShardingStorageClient将不在继承IShardingUpdte，IShardingMasterQuery，IShardingSlaveQuery接口
  
  3.2.IShardingStorageClient会提供单独的增删改查接口来屏蔽底层操作，并且会添加更多的查询接口.
  
  3.3.IQuery接口添加查询部分字段值(缓存操作需要注意)

### version 0.4.1
1.优化了id生成器的性能

2.代码重构
  2.1.重构了ID生成器的相关代码

### version 0.4.0
1. 基于zookeeper的全局ID生成器

1.1 需要依赖zookeeper来做集群锁做同步, zookeeper连接需要在storage-config.properties里进行配置.

1.2 全局表的主键不能设置为自增

2. 所有的表的主键必须不能自增，主键由pinus框架统一生成.

### version 0.3.2
1. 添加了三个接口save、update、saveOrUpdate，支持单个参数数据对象, 但是数据对象需要实现IShardingValue接口

### version 0.3.1
1. 将Timestamp注解修改为UpdateTime

2. 修复了Timestamp自动更新失效的bug

### version 0.3.0
1. 添加更多的散列算法，支持根据字符串进行分库分表
2. 
2. 添加统计组件
3. 
2.1 统计散列分布情况

### version 0.2.1
1. 添加findOneByQuery接口

2. 代码优化

2.1 将DBGenerator和IdGenerator抽取到com.pinus.generator包中

3. 缓存方式改为针对单个实体进行缓存

4. 修复部分bug

### version 0.2.0
1. 数据库表生成工具       
1.1 根据零库中的shard_cluster表来创建分表

1.2 基于@Table注解创建分表, 基于这种策略每个分库中的分表数量是相同的
    这种策略默认的分表数量是0，如果shuardingnum=0则只在全局库中生成表
    
生成工具优先使用1.1策略创建分表，如果不存在shard_cluster表则使用1.2的策略创建

2. 查询缓存实现     
3. 
3. 实现单库单表的增删改查操作                                                          

### version 0.1.0
1. 统一的增删改查接口实现      

2. 基于哈希算法的分库分表实现   

3. 主从库查询接口实现     

4. 集群全局主键生成服务