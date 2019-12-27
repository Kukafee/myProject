-- ----------------------------------------------------------------------------
-- 建表: mid_article 文章分类 特征表
-- ----------------------------------------------------------------------------
-- drop table if exists mid_article_;   -- 表名
-- create table if not exists mid_article_   -- 表名
--     (
--     mid string,
--     article string       -- 字段名
--     )
-- row format delimited
-- fields terminated by ','
-- stored as orc;
-- -- 插入数据
-- insert into
--     mid_article_        -- 表名
-- select
--     mid_app_labelp.m_id,                              -- ---------------------------------- 选库和表
--     feature_name
-- from
--     pangxk_.mid_app_labelp lateral view explode(split(article, ' ')) table_tmp  as feature_name;    -- 选库和表,特征,分隔符
-- where length(feature_name)<>0 and feature_name is not null;

-- ----------------------------------------------------------------------------
-- mapreduce.job.reduce.slowstart.completedmaps参数，默认为0.05，即map完成0.05后reduce就开始copy，
-- 如果集群资源不够，有可能导致reduce把资源全抢光，可以把这个参数调整到0.8，map完成80%后才开始reduce copy。
-- ----------------------------------------------------------------------------
-- mid_app_labelp : 34427211
-- 优化Hive执行引擎
set hive.execution.engine=tez;    -- 使用 tez 引擎
set hive.prewarm.enabled=true;    -- 告诉hive创建Tez容器
set hive.prewarm.numcontainers=10;    -- 调整Tez专用容器的数量
set mapreduce.job.reduce.slowstart.completedmaps=0.8

set TEZ_CONTAINER_MAX_JAVA_HEAP_FRACTION=0.8;     -- Tez容器的规模是YARN容器规模的倍数
set hive.auto.convert.join.nonconditionaltask.size=10;    -- 调整映射连接的规模
set hive.vectorized.execution.enabled=true;    -- 使用矢量化查询执行,须以ORC格式存储数据

set hive.groupby.skewindata=true

set mapreduce.job.reduce.slowstart.completedmaps=0.8
-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
use pangxk;
drop table if exists mid_article_;   -- 表名
create table if not exists mid_article_   -- 表名
    (
    mid string,
    article string       -- 字段名
    )
row format delimited
fields terminated by ','
stored as orc tblproperties("orc.compress"="SNAPPY");  -- -------------- 设置为 orc 存储方式
-- 插入数据   
insert into
    mid_article_        -- 表名
select
    mid_app_label2.m_id,                              -- ---------------------------------- 选库和表
    feature_name
from
    pangxk.mid_app_label2 lateral view explode(split(article, ' ')) table_tmp  as feature_name;

-- -----------------------------------------------------------------------------------
-- drop table if exists mid_article_1;   -- 表名          131 928 122
-- create table if not exists mid_article_1   -- 表名
--     (
--     mid string,
--     article string       -- 字段名
--     )
-- row format delimited
-- fields terminated by ','
-- stored as orc;
-- -- 插入数据   131 928 122
-- insert into
--     mid_article_1(mid, article)        -- 表名
-- select
--     t1.mid,
--     t2.article
-- from(select mid from mid_ttlabel) as t1
-- join(select mid, article from mid_article_) as t2
-- on t1.mid = t2.mid;
-- ------------------------------------------------------------------------------------

-- -- 去重
-- drop table if exists mid_article_2;   -- 表名
-- create table if not exists mid_article_2   -- 表名
--     (
--     mid string,
--     article string       -- 字段名
--     )
-- row format delimited
-- fields terminated by ','
-- stored as orc;
-- -- 插入数据
-- insert into mid_article_2(mid, article)
-- select distinct mid, article from mid_article_;

-- 去空
drop table if exists mid_article;   -- 表名
create table if not exists mid_article   -- 表名
    (
    mid string,
    article string       -- 字段名
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into mid_article(mid, article)
select distinct mid, article from mid_article_
where length(article)<>0 and article is not null;

-- 删除中间表 
drop table if exists mid_article_;   -- 表名
-- drop table if exists mid_article_1;   -- 表名
-- drop table if exists mid_article_2;   -- 表名

-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------



-- ----------------------------------------------------------------------------
-- ------------------------------------------------------------------------------------------
-- 创建中间表 mid_behavior_ (字段 behavior 炸裂开，打平为行)
drop table if exists mid_behavior_;   -- 表名
create table if not exists mid_behavior_   -- 表名
    (
    mid string,
    behavior string       -- 字段名
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into
    mid_behavior_        -- 表名
select
    mid_app_labelp.m_id,                              -- ---------------------------------- 选库和表
    feature_name
from
    pangxk_.mid_app_labelp lateral view explode(split(behavior, ' ')) table_tmp  as feature_name;

-- 创建中间表 mid_behavior_1 (实现join操作)
drop table if exists mid_behavior_1;   -- 表名
create table if not exists mid_behavior_1   -- 表名
    (
    mid string,
    behavior string       -- 字段名
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into
    mid_article_1(mid, behavior)        -- 表名
select
    t1.mid,
    t2.behavior
from(select mid from mid_ttlabel) as t1
join(select mid, behavior from mid_behavior_) as t2
on t1.mid = t2.mid;
-- ------------------------------------------------------------------------------------

-- 去重
drop table if exists mid_article_2;   -- 表名
create table if not exists mid_article_2   -- 表名
    (
    mid string,
    article string       -- 字段名
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into mid_article_2(mid, article)
select distinct mid, article from mid_article_;

-- 去空
drop table if exists mid_article;   -- 表名
create table if not exists mid_article   -- 表名
    (
    mid string,
    article string       -- 字段名
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into mid_article(mid, article)
select mid, article from mid_article_2
where length(article)<>0 and article is not null;

-- 删除中间表 
drop table if exists mid_article_;   -- 表名
drop table if exists mid_article_1;   -- 表名
drop table if exists mid_article_2;   -- 表名











-- ------------------------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------

drop table if exists mid_interest1_;   -- 表名
create table if not exists mid_interest1_   -- 表名
    (
    mid string,
    interest1 string       -- 字段名
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into
    mid_interest1_        -- 表名
select
    mid_app_labelp.m_id,                              -- ---------------------------------- 选库和表
    feature_name
from
    pangxk_.mid_app_labelp lateral view explode(split(interest1, ' ')) table_tmp  as feature_name;

-- ------------------------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------

drop table if exists mid_interest1_2_;   -- 表名
create table if not exists mid_interest1_2_   -- 表名
    (
    mid string,
    interest1_2 string       -- 字段名
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into
    mid_interest1_2_        -- 表名
select
    mid_app_labelp.m_id,                              -- ---------------------------------- 选库和表
    feature_name
from
    pangxk_.mid_app_labelp lateral view explode(split(interest1_2, ' ')) table_tmp  as feature_name

--------------------------------------------------------------------------------------------

----
-- 删除中间表
----































