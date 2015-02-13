---
layout: default
---
# 配置说明
pinus在启动时候时候会读取classpath根路径的storage-config.xml文件，此文件是pinus的核心配置文件，所有的配置信息都包含在此文件中

{% highlight xml %}
<?xml version="1.0" encoding="UTF-8" ?>
<storage-config>
    <!-- text hash algo -->
    <!-- additive | rotating | oneByOne | bernstein | fnv | rs | js | pjw | 
	elf | bkdr | sdbm | djb | dek | ap | java | mix -->
    <db.cluster.hash.algo>bernstein</db.cluster.hash.algo>

    <!-- zookeeper connection -->
    <db.cluster.zk>127.0.0.1:2181</db.cluster.zk>

    <!-- id generator batch -->
    <db.cluster.generateid.batch>1</db.cluster.generateid.batch>

    <!-- db query cache expire is seconds -->
    <db.cluster.cache enabled="true">
        <primary expire="300">
            <address>127.0.0.1:11211</address>
        </primary>
        <second expire="300">
            <address>127.0.0.1:11211</address>
        </second>
    </db.cluster.cache>

    <!-- catalog is "env" or "app" -->
    <!-- env represent get connection pool from container -->
    <!-- app represent get connection pool from internal application -->
    <db-connection-pool catalog="app">
        <maxActive>10</maxActive>
	<minIdle>10</minIdle>
	<maxIdle>10</maxIdle>
	<initialSize>1</initialSize>
	<removeAbandoned>true</removeAbandoned>
	<removeAbandonedTimeout>10</removeAbandonedTimeout>
	<maxWait>1000</maxWait>
	<timeBetweenEvictionRunsMillis>10000</timeBetweenEvictionRunsMillis>
	<numTestsPerEvictionRun>10</numTestsPerEvictionRun>
	<minEvictableIdleTimeMillis>10000</minEvictableIdleTimeMillis>
    </db-connection-pool>

    <!-- cluster config -->
    <cluster name="pinus" catalog="mysql">
	<global>
	    <master>
	        <db.username></db.username>
		<db.password></db.password>
		<db.url></db.url>
	    </master>
	    <slave>
	        <db.username></db.username>
                <db.password></db.password>
	        <db.url></db.url>
	    </slave>
        </global>
	<region capacity="1-30000000">
	    <master>
	        <sharding>
		    <db.username></db.username>
		    <db.password></db.password>
		    <db.url></db.url>
		</sharding>
		<sharding>
		    <db.username></db.username>
		    <db.password></db.password>
		    <db.url></db.url>
		</sharding>
            </master>
	    <slave>
	        <sharding>
		    <db.username></db.username>
		    <db.password></db.password>
		    <db.url></db.url>
		</sharding>
		<sharding>
		    <db.username></db.username>
		    <db.password></db.password>
		    <db.url></db.url>
		</sharding>
	    </slave>
	</region>
        <region capacity="30000001-60000000">
	    <master>
	        <sharding>
		    <db.username></db.username>
		    <db.password></db.password>
		    <db.url></db.url>
		</sharding>
		<sharding>
		    <db.username></db.username>
		    <db.password></db.password>
		    <db.url></db.url>
		</sharding>
            </master>
	    <slave>
	        <sharding>
		    <db.username></db.username>
		    <db.password></db.password>
		    <db.url></db.url>
		</sharding>
		<sharding>
		    <db.username></db.username>
		    <db.password></db.password>
		    <db.url></db.url>
		</sharding>
	    </slave>
	</region>
    </cluster>
</storage-config>
{% endhighlight %}

以上是一个完整的配置文件内容，定义了一个名为pinus的集群，集群中包含一个一个全局库，两个region，第一个region包括的ShardingKey范围是1-30000000，第二个region的范围是30000001-60000000，每一个region中包含两组数据库信息，master表示主库支持读写操作，slave表示从库支持只读操作，主从同步需要依靠数据库本身的机制来实现。\\
db-connection-pool表示数据库连接池的信息，pinus使用的是apache-dbcp连接池，目前不支持替换，数据库连接池分为两种一种是应用程序，一种是容器，当使用容器的连接池时只需要将<db-connection-pool catalog="env">，同时<master>和<slave>的内容替换为容器的jni名字，示例如下

{% highlight xml %}
<storage-config>
    <!-- text hash algo -->
    <!-- additive | rotating | oneByOne | bernstein | fnv | rs | js | pjw | 
	elf | bkdr | sdbm | djb | dek | ap | java | mix -->
    <db.cluster.hash.algo>bernstein</db.cluster.hash.algo>

    <!-- zookeeper connection -->
    <db.cluster.zk>127.0.0.1:2181</db.cluster.zk>

    <!-- id generator batch -->
    <db.cluster.generateid.batch>1</db.cluster.generateid.batch>

    <!-- db query cache expire is seconds -->
    <db.cluster.cache enabled="true">
        <primary expire="300">
            <address>127.0.0.1:11211</address>
        </primary>
        <second expire="300">
            <address>127.0.0.1:11211</address>
        </second>
    </db.cluster.cache>
    
    <db-connection-pool catalog="env"></db-connection-pool>

    <cluster name="pinus" catalog="mysql">
        <global>
            <master>java:comp/env/jdbc/pinus</master>
            <slave>java:comp/env/jdbc/pinus0</slave>
        </global>
        <region capacity="1-1000000">
            <master>
	        <sharding>java:comp/env/jdbc/pinus1</sharding>
	        <sharding>java:comp/env/jdbc/pinus2</sharding>
            </master>
     	    <slave>
	        <sharding>java:comp/env/jdbc/pinus3</sharding>
                <sharding>java:comp/env/jdbc/pinus4</sharding>
	    </slave>
        </region>
        <region capacity="1000001-2000000">
            <master>
	        <sharding>java:comp/env/jdbc/pinus5</sharding>
	        <sharding>java:comp/env/jdbc/pinus6</sharding>
	    </master>
            <slave>
	        <sharding>java:comp/env/jdbc/pinus7</sharding>
	        <sharding>java:comp/env/jdbc/pinus8</sharding>
	    </slave>
        </region>
    </cluster>
</storage-config>
{% endhighlight %}

# 自动生成数据表
pinus根据配置的:数据库信息、实体对象、@Table中shardingNum的值来生成相关的数据表，生成规则如下:
  * 根据@Table中的cluster判断实体对象所在的集群
  * 根据@Table中的shardingNum来判断是全局表还是分片表，值为0或不填表示全局表
  * 根据@Table中的name表示表名，如果是分片表则创建shardingNum个表，并且下标以0开始
  * 集群中每一个库中都会生成shardingNum个数据表
以上配置完之后还需要调用api来告诉pinus创建表的规则

{% highlight java %}
IShardingStorageClient storageClient = new ShardingStorageClientImpl();
//表示如果库中不存在则创建，如果存在则同步，但是pinus只会做增量同步
storageClient.setSyncAction(EnumSyncAction.UPDATE); 
{% endhighlight %}

#使用Api开发
如果使用pinus提供的api进行开发，则需要创建ShardingStorageClientImpl对象实例，创建方法如下:\\
{% highlight java %}
IShardingStorageClient storageClient = new ShardingStorageClientImpl();
storageClient.setScanPackage("org.pinus4j"); // 需要扫描多个包时使用英文半角逗号分隔
storageClient.setSyncAction(EnumSyncAction.UPDATE);
storageClient.init();
{% endhighlight %}

当应用程序结束时需要调用storageClient.destroy()方法
#集成Spring框架
如果你的系统使用了spring框架，则需要将ShardingStorageClientImpl交给spring管理即可
{% highlight java %}
<bean id="shardingStorageClient" class="org.pinus4j.api.ShardingStorageClientImpl"
    init-method="init" destroy-method="destroy">
    <!-- 扫描多个包使用英文半角逗号分隔 -->
    <property name="scanPackage" value="org.pinus4j.entity" />
    <property name="syncAction" value="UPDATE" />
</bean>
{% endhighlight %}
## 事务处理
pinus目前支持编程式事务处理和声明式事务处理，其中声明式事务处理需要依赖spring框架

## 编程式事务
直接调用pinus提供的api
{% highlight java %}
storageClient.beginTransaction();
try {
    //do something...
    storageClient.commit();
} catch (Exceptioin e) {
    storageClient.rollback();
}
{% endhighlight %}
## 声明式事务
需要在spring配置文件中加入如下配置，

{% highlight xml %}
<bean id="userTx" class="org.pinus4j.transaction.impl.UserTransactionImpl" />
<bean id="tm"
    class="org.pinus4j.transaction.impl.BestEffortsOnePCJtaTransactionManager" />

<bean id="transactionManager"
    class="org.springframework.transaction.jta.JtaTransactionManager">
    <property name="userTransaction" ref="userTx" />
    <property name="transactionManager" ref="tm" />
</bean>

<tx:annotation-driven transaction-manager="transactionManager" />
{% endhighlight %}

以上配置表示让spring使用pinus的事务管理器来管理事务，剩下的请参考spring文档
# 日志
pinus使用slf4j日志接口来输出日志，用户可以自己选择相关的日志工具
## SQL日志
如果想要在日志文件中打印sql语句，可以将org.pinus4j.datalayer.SQLBuilder的输出级别设置为debug

{% highlight xml%}
<category name="org.pinus4j.datalayer.SQLBuilder">
    <level value="debug" />
</category>
{% endhighlight %}
## 慢日志
pinus会将响应比较慢的sql语句输出到日志中，配置方式如下

{% highlight xml%}
<category name="org.pinus4j.datalayer.SlowQueryLogger">
    <level value="warn" />
</category>
{% endhighlight %}
