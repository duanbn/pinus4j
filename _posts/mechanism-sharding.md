---
layout: default
---

# 数据分片机制

## 数据库水平切分模型

Pinus 的水平切分模型分为几个层级，这个层级关系被配置在 `storage-config.xml` 文件中，配置文件中的节点名称为 cluster。cluster 可以配置多个，具体请参考 [入门教程]({% post_url 2015-02-13-induction %}) 中的配置文件信息。水平切分模型如下图所示：

![]({{site.baseurl}}/img/sharding_arch.png)

* __ShardingKey__：分片因子。数据的增、删、改都需要通过这个因子来定位最终会存储在哪个库的哪张表中，ShardingKey 的值在路由选择时会变成一个数字，如果是字符串也会通过 hash 算法变成一个数字。

* __cluster__：表示一个数据库集群，每个集群在配置的时候需要指定一个集群名称。

* __region__：表示一个数据库分组，每个 region 需要指定一个数字的范围，这个数值表示可以接受的 ShardingKey 的值的范围。这里需要注意的是，这个范围不是指具体的记录数，而是可以接受的 ShardingKey 的值。

* __sharding__：表示一个分片库。

* __sharding table__：表示一个分片库中的一个分表。

* __global__：表示一个全局库。在一个集群当中只能存在一个全局库，全局库中的数据不会被切分。

* __global table__：表示全局库中的表。

了解到以上概念之后，下边描述一下具体的工作机制。

首先在定义数据实体对象的时候，我们需要对实体对象类使用 `@Table` 进行注解，@Table 注解中有 shardingBy、shardingNum、cluster 等属性。这些属性表示该数据实体属于哪个集群以及 ShardingKey 的值依据哪个字段的值产生。当我们调用 save 方法保存这个对象时，Pinus 会首先给这个对象生成一个集群中唯一的主键（参考下一节：集群中数据主键生成），然后根据 shardingBy 所设定的字段取出值之后创建 ShardingKey 对象，DBRouter 实例会根据给定的 ShardingKey 找到对应的一个数据库资源，然后将数据对象映射成 SQL 语句。

## 数据扩容

Pinus 目前只支持静态扩容，扩容的方式是依靠添加新的 region 来完成。结合上述概念，每个 region 由一组库组成，当某个集群的数据需要扩容时只需要添加新的 region 即可。
