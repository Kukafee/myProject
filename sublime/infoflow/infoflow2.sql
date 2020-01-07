
-- 使用数据库
use infoflow;

-- 配置参数
set mapreduce.job.reduce.slowstart.completedmaps=0.85;

-- ----------------------------------------------------------------------------
-- 创建 全量 男 文章分类 特征表 pop_article_m
-- ----------------------------------------------------------------------------
drop table if exists pop_article_m;      -- 表名
create table if not exists pop_article_m -- 表名
    (
    mid string,
    article string                     -- 字段名
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into pop_article_m(mid, article)  -- 表名, 字段名
select
    distinct t2.mid, t1.article        -- 字段名
from app_article t1                    -- 表1
inner join pop_mid_appid_m t2          -- 表2
on(t1.appid=t2.appid);


-- ----------------------------------------------------------------------------
-- 创建 全量 女 文章分类 特征表 pop_article_w
-- ----------------------------------------------------------------------------
drop table if exists pop_article_w;      -- 表名
create table if not exists pop_article_w -- 表名
    (
    mid string,
    article string                     -- 字段名
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into pop_article_w(mid, article)  -- 表名, 字段名
select
    distinct t2.mid, t1.article        -- 字段名
from app_article t1                    -- 表1
inner join pop_mid_appid_w t2          -- 表2
on(t1.appid=t2.appid);


-- ----------------------------------------------------------------------------
-- 创建 样本 男 文章分类 特征表 bsn_article_m
-- ----------------------------------------------------------------------------
drop table if exists bsn_article_m;      -- 表名
create table if not exists bsn_article_m -- 表名
    (
    mid string,
    article string                     -- 字段名
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into bsn_article_m(mid, article)  -- 表名, 字段名
select
    distinct t2.mid, t1.article        -- 字段名
from app_article t1                    -- 表1
inner join bsn_mid_appid_m t2          -- 表2
on(t1.appid=t2.appid);


-- ----------------------------------------------------------------------------
-- 创建 样本 女 文章分类 特征表 bsn_article_w
-- ----------------------------------------------------------------------------
drop table if exists bsn_article_w;      -- 表名
create table if not exists bsn_article_w -- 表名
    (
    mid string,
    article string                     -- 字段名
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into bsn_article_w(mid, article)  -- 表名, 字段名
select
    distinct t2.mid, t1.article        -- 字段名
from app_article t1                    -- 表1
inner join bsn_mid_appid_w t2          -- 表2
on(t1.appid=t2.appid);



-- ----------------------------------------------------------------------------
-- 创建 全量 男 app行为 特征表 pop_behavior_m
-- ----------------------------------------------------------------------------
drop table if exists pop_behavior_m;      -- 表名
create table if not exists pop_behavior_m -- 表名
    (
    mid string,
    behavior string                     -- 字段名
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into pop_behavior_m(mid, behavior)  -- 表名, 字段名
select
    distinct t2.mid, t1.behavior        -- 字段名
from app_behavior t1                    -- 表1
inner join pop_mid_appid_m t2          -- 表2
on(t1.appid=t2.appid);


-- ----------------------------------------------------------------------------
-- 创建 全量 女 app行为 特征表 pop_behavior_w
-- ----------------------------------------------------------------------------
drop table if exists pop_behavior_w;      -- 表名
create table if not exists pop_behavior_w -- 表名
    (
    mid string,
    behavior string                     -- 字段名
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into pop_behavior_w(mid, behavior)  -- 表名, 字段名
select
    distinct t2.mid, t1.behavior        -- 字段名
from app_behavior t1                    -- 表1
inner join pop_mid_appid_w t2          -- 表2
on(t1.appid=t2.appid);


-- ----------------------------------------------------------------------------
-- 创建 样本 男 app行为 特征表 bsn_behavior_m
-- ----------------------------------------------------------------------------
drop table if exists bsn_behavior_m;      -- 表名
create table if not exists bsn_behavior_m -- 表名
    (
    mid string,
    behavior string                     -- 字段名
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into bsn_behavior_m(mid, behavior)  -- 表名, 字段名
select
    distinct t2.mid, t1.behavior        -- 字段名
from app_behavior t1                    -- 表1
inner join bsn_mid_appid_m t2          -- 表2
on(t1.appid=t2.appid);


-- ----------------------------------------------------------------------------
-- 创建 样本 女 app行为 特征表 bsn_behavior_w
-- ----------------------------------------------------------------------------
drop table if exists bsn_behavior_w;      -- 表名
create table if not exists bsn_behavior_w -- 表名
    (
    mid string,
    behavior string                     -- 字段名
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into bsn_behavior_w(mid, behavior)  -- 表名, 字段名
select
    distinct t2.mid, t1.behavior        -- 字段名
from app_behavior t1                    -- 表1
inner join bsn_mid_appid_w t2          -- 表2
on(t1.appid=t2.appid);


-- ----------------------------------------------------------------------------
-- 创建 全量 男 兴趣定向(一级&二级) 特征表 pop_interest1_2_m
-- ----------------------------------------------------------------------------
drop table if exists pop_interest1_2_m;      -- 表名
create table if not exists pop_interest1_2_m -- 表名
    (
    mid string,
    interest1_2 string                     -- 字段名
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into pop_interest1_2_m(mid, interest1_2)  -- 表名, 字段名
select
    distinct t2.mid, t1.interest1_2        -- 字段名
from app_interest1_2 t1                    -- 表1
inner join pop_mid_appid_m t2          -- 表2
on(t1.appid=t2.appid);


-- ----------------------------------------------------------------------------
-- 创建 全量 女 兴趣定向(一级&二级) 特征表 pop_interest1_2_w
-- ----------------------------------------------------------------------------
drop table if exists pop_interest1_2_w;      -- 表名
create table if not exists pop_interest1_2_w -- 表名
    (
    mid string,
    interest1_2 string                     -- 字段名
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into pop_interest1_2_w(mid, interest1_2)  -- 表名, 字段名
select
    distinct t2.mid, t1.interest1_2        -- 字段名
from app_interest1_2 t1                    -- 表1
inner join pop_mid_appid_w t2          -- 表2
on(t1.appid=t2.appid);


-- ----------------------------------------------------------------------------
-- 创建 样本 男 兴趣定向(一级&二级) 特征表 bsn_interest1_2_m
-- ----------------------------------------------------------------------------
drop table if exists bsn_interest1_2_m;      -- 表名
create table if not exists bsn_interest1_2_m -- 表名
    (
    mid string,
    interest1_2 string                     -- 字段名
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into bsn_interest1_2_m(mid, interest1_2)  -- 表名, 字段名
select
    distinct t2.mid, t1.interest1_2        -- 字段名
from app_interest1_2 t1                    -- 表1
inner join bsn_mid_appid_m t2          -- 表2
on(t1.appid=t2.appid);


-- ----------------------------------------------------------------------------
-- 创建 样本 女 兴趣定向(一级&二级) 特征表 bsn_interest1_2_w
-- ----------------------------------------------------------------------------
drop table if exists bsn_interest1_2_w;      -- 表名
create table if not exists bsn_interest1_2_w -- 表名
    (
    mid string,
    interest1_2 string                     -- 字段名
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into bsn_interest1_2_w(mid, interest1_2)  -- 表名, 字段名
select
    distinct t2.mid, t1.interest1_2        -- 字段名
from app_interest1_2 t1                    -- 表1
inner join bsn_mid_appid_w t2          -- 表2
on(t1.appid=t2.appid);

-- ***********************************************************

































