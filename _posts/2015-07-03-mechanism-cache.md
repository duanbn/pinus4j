---
layout: default
---

# 缓存机制

## 缓存机制

Pinus 的缓存目前基于 memcached 实现，用户可以通过实现 IPrimaryCache 和 ISecondCache 接口进行扩展。[定制开发]({% post_url 2015-02-13-custom %})

### 一级缓存

与数据库中的一条记录一一对应，增、删、改、查操作会处理缓存与数据库的一致性，全局库中数据的缓存与分片库中数据缓存的 key 值不同，但都是基于数据主键生成。

命中机制，当使用主键进行查询时会首先查询缓存，如果命中则直接返回；如果根据条件进行查询时，会首先根据查询条件从数据库中取出主键，之后从缓存中获取数据；如果部分主键没有命中缓存，则再将没有命中的主键通过 in 查询从数据库中查询，之后再将数据放入缓存中。

一级缓存的key格式

全局表 [clusterName].[tableName].[pk]

分片表 [clusterName + dbIndex].[start + end].[tableName + tableIndex].[id]

### 二级缓存

由于二级缓存的实现根据具体的缓存服务所以实现细节不同

全局表 sec.[clustername].[tablename].[sql condition hash code]
分片表 sec.[clustername].[startend].[tablename + tableIndex].[sql condition hash code]

对 IQuery 查询结果进行缓存，当增、删、改操作影响到缓存时，Pinus 会将缓存结果清除，缓存中的 key 是将 IQuery 对象通过 hash 操作生成唯一的字符串作为 key。

命中机制，将新查询的 IQuery 通过相同的 hash 操作生成 key，然后去缓存中查询，如果命中则直接返回，如果没有命中则查询数据库，将结果集进行缓存。

### 更新缓存

当执行写入、更新操作时需要多一级、二级缓存进行更新

一级缓存的更新策略，当有新数据写入的时候，记录插入到数据表之后会同时写入一级缓存、当有数据更新的时候也会实时的缓存一级缓存。

二级缓存的更新策略，因为二级缓存是对某个查询结果进行缓存，因此当查询结果涉及到的表发生变化时需要将二级缓存都清理掉。
