-- 全局主键信息
create table global_id (
    table_name varchar(30) not null comment '表名',
    global_id bigint unsigned not null comment '全局唯一的id',
    primary key(table_name)
);

-- 数据库集群信息
create table shard_cluster (
    db_name varchar(30) not null comment '分库的数据库名称',
    db_index smallint not null comment '分库的下标',
    table_name varchar(30) not null comment '被分表的表名, 表示这个表需要进行分表',
    table_num smallint not null default 1 comment '分表的数量，表示一个库中有多少个分表'
);
