---
layout: default
---
# 数据库水平切分模型

pinus的水平切分模型如下图所示，分为几个层级来表示，这个层级关系被配置在storage-config.xml文件中，配置文件中的节点名称为cluster
cluster可以配置多个，具体请参考入门教程中的配置文件信息。

![]({{site.baseurl}}img/sharding_arch.png)

ShardingKey 分片因子，数据的增、删、改都需要通过这个因子来定位最终会存储在哪个库的哪张表中、ShardingKey的值在路由选择时会变成一个数字，如果是字符串也会通过hash算法变成一个数字。

cluster表示一个数据库集群，每个集群在配置的时候需要指定一个集群名称。

region表示一个数据库分组，每个region需要指定一个数字的范围，这个数值表示可以接受的ShardingKey的值得范围，这里需要注意的是，这个范围不是指具体的记录数，而是可以接受的ShardingKey的值

sharding表示一个分片库

sharding table表示一个分片库中的一个分表

global表示一个全局库，在一个集群当中只能存在一个全局库，全局库中的数据不会被切分。

global table 表示全局库中的表。

了解到以上概念之后下边描述一下具体的工作机制，首先在定义数据实体对象的时候我们需要对实体对象类使用@Table进行注解，@Table注解中有shardingBy、shardingNum、cluster等属性，这些属性表示这个数据实体是属于哪个集群的以及ShardingKey的值是依据那个字段的值产生的。当我们调用save方法保存这个对象时，pinus会首先给这个对象生成一个集群中唯一的主键（参考下一节：集群中数据主键生成），然后根据shardingBy所设定的字段取出值之后创建ShardingKey对象，DBRouter实例会根据给定的ShardingKey找到对应的一个数据库资源，然后将数据对象映射成SQL语句。

# 数据扩容
pinus目前只支持静态扩容，扩容的方式是依靠添加新的region来完成，结合上述的概念，每个region是有一组库组成，当某个集群的数据需要扩容时只需要添加新的region即可。
