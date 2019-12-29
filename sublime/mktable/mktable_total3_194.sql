
-- 使用表mid_app_label/mid_app_label_p
-- 		se
-- 
-- 
-- 使用mid_appid
-- 		select count(*) from mid_appid2;
-- 		select count(*) from (select distinct mid from mid_appid2);
-- 		select count(*) from (select distinct mid from databank.freq_info);
-- 

-- ----------------------------------------------------------------------------
-- 任务分析
-- ----------------------------------------------------------------------------
-- 针对总体样本的app动态表 pangxk.mid_app_label 
-- 对 文章分类, app行为, 兴趣定向(一级), 兴趣定向(一级&二级) 四个特征进行建表分析
--    article, behavior, interest1, interest1_2
-- 
-- 使用数据库
use pangxk;

-- 配置参数
set mapreduce.job.reduce.slowstart.completedmaps=0.9;

-- -- ----------------------------------------------------------------------------
-- -- 建表: mid_article 文章分类 特征表
-- -- ----------------------------------------------------------------------------
drop table if exists mid_article;   -- 表名       总体样本 mid_article    目标样本 mid_article_p
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
select
    distinct t2.mid, t1.article    
from app_article t1 
inner join mid_appid2 t2         -- mid_appid -- mid_appid2.mid 与 mid_app_label2.m_id
on(t1.appid=t2.appid);


-- drop table if exists mid_article;   -- 表名
-- create table if not exists mid_article   -- 表名
--     (
--     mid string,
--     article string       -- 字段名
--     )
-- row format delimited
-- fields terminated by ','
-- stored as orc;
-- -- 插入数据
-- insert into
--     mid_article        -- 表名
-- select
--     distinct mid_app_label2.m_id,                              -- ---------------------------------- 选库和表
--     feature_name
-- from
--     pangxk.mid_app_label2 lateral view explode(split(article, ' ')) table_tmp  as feature_name    -- 选库和表,特征,分隔符
-- where length(feature_name)<>0 and feature_name is not null;

-- -- ----------------------------------------------------------------------------
-- -- 建表: mid_behavior app行为 特征表. Time taken: 19209.452 seconds
-- -- ----------------------------------------------------------------------------
-- drop table if exists mid_behavior;   -- 表名
-- create table if not exists mid_behavior   -- 表名
--     (
--     mid string,
--     behavior string       -- 字段名
--     )
-- row format delimited
-- fields terminated by ','
-- stored as orc;
-- -- 插入数据
-- insert into mid_behavior(mid, behavior)
-- select
--     distinct t2.mid, t1.behavior
-- from app_behavior t1 
-- inner join mid_appid2 t2         -- mid_appid2
-- on(t1.appid=t2.appid);


-- drop table if exists mid_behavior;   -- 表名
-- create table if not exists mid_behavior   -- 表名
--     (
--     mid string,
--     behavior string       -- 字段名
--     )
-- row format delimited
-- fields terminated by ','
-- stored as orc;
-- -- 插入数据
-- insert into
--     mid_behavior   -- 表名
-- select
--     distinct mid_app_label2.m_id,                              -- ---------------------------------- 选库和表
--     feature_name
-- from
--     pangxk.mid_app_label2 lateral view explode(split(behavior, ' ')) table_tmp  as feature_name    -- 选库和表,特征,分隔符
-- where length(feature_name)<>0 and feature_name is not null;

-- -- ----------------------------------------------------------------------------
-- -- 建表: mid_interest1 兴趣定向(一级) 特征表  9421.205 seconds
-- -- ----------------------------------------------------------------------------
-- set mapreduce.job.reduce.slowstart.completedmaps=0.8;
-- drop table if exists mid_interest1;   -- 表名
-- create table if not exists mid_interest1   -- 表名
--     (
--     mid string,
--     interest1 string       -- 字段名
--     )
-- row format delimited
-- fields terminated by ','
-- stored as orc;
-- -- 插入数据
-- insert into mid_interest1(mid, interest1)
-- select
--     distinct t2.mid, t1.interest1
-- from app_interest1 t1 
-- inner join mid_appid2 t2
-- on(t1.appid=t2.appid);


-- drop table if exists mid_interest1;   -- 表名
-- create table if not exists mid_interest1   -- 表名
--     (
--     mid string,
--     interest1 string       -- 字段名
--     )
-- row format delimited
-- fields terminated by ','
-- stored as orc;
-- -- 插入数据
-- insert into
--     mid_interest1   -- 表名
-- select
--     distinct mid_app_label2.m_id,                              -- ---------------------------------- 选库和表
--     feature_name
-- from
--     pangxk.mid_app_label2 lateral view explode(split(interest1, ' ')) table_tmp  as feature_name    -- 选库和表,特征,分隔符
-- where length(feature_name)<>0 and feature_name is not null;

-- -- ----------------------------------------------------------------------------
-- -- 建表: mid_interest1_2 兴趣定向(一级&二级) 特征表
-- -- ----------------------------------------------------------------------------
drop table if exists mid_interest1_2;   -- 表名
create table if not exists mid_interest1_2   -- 表名
    (
    mid string,
    interest1_2 string       -- 字段名
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into mid_interest1_2(mid, interest1_2)
select
    distinct t2.mid, t1.interest1_2
from app_interest1_2 t1 
inner join mid_appid2 t2
on(t1.appid=t2.appid);


-- drop table if exists mid_interest1_2;   -- 表名
-- create table if not exists mid_interest1_2   -- 表名
--     (
--     mid string,
--     interest1_2 string       -- 字段名
--     )
-- row format delimited
-- fields terminated by ','
-- stored as orc;
-- -- 插入数据
-- insert into
--     mid_interest1_2   -- 表名
-- select
--     distinct mid_app_label2.m_id,                              -- ---------------------------------- 选库和表
--     feature_name
-- from
--     pangxk.mid_app_label2 lateral view explode(split(interest1_2, ' ')) table_tmp  as feature_name    -- 选库和表,特征,分隔符
-- where length(feature_name)<>0 and feature_name is not null;
























