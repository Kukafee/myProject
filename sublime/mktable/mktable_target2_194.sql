
-- ----------------------------------------------------------------------------
-- 任务分析
-- ----------------------------------------------------------------------------
-- 针对目标群体样本的app动态表 pangxk.mid_app_label_p
-- 对 文章分类, app行为, 兴趣定向(一级), 兴趣定向(一级&二级) 四个特征进行建表分析
-- 
-- 
-- 
-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
-- article                 string
-- behavior                string
-- interest1               string
-- interest1_2             string  
-- ----------------------------------------------------------------------------

use pangxk;
-- ----------------------------------------------------------------------------
-- 建表: mid_article_p 文章分类 特征表
-- ----------------------------------------------------------------------------
drop table if exists mid_article_p;   -- 表名
create table if not exists mid_article_p   -- 表名
    (
    mid string,
    article string       -- 字段名
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into
    mid_article_p        -- 表名
select
    distinct mid_app_label_p.m_id,                              -- ---------------------------------- 选库和表
    feature_name
from
    pangxk.mid_app_label_p lateral view explode(split(article, ' ')) table_tmp  as feature_name    -- 选库和表
where length(feature_name)<>0 and feature_name is not null;

-- ----------------------------------------------------------------------------
-- 建表: mid_behavior_p app行为 特征表
-- ----------------------------------------------------------------------------
drop table if exists mid_behavior_p;   -- 表名
create table if not exists mid_behavior_p   -- 表名
    (
    mid string,
    behavior string       -- 字段名
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into
    mid_behavior_p   -- 表名
select
    distinct mid_app_label_p.m_id,                              -- ---------------------------------- 选库和表
    feature_name
from
    pangxk.mid_app_label_p lateral view explode(split(behavior, ' ')) table_tmp  as feature_name    -- 选库和表,特征,分隔符
where length(feature_name)<>0 and feature_name is not null;

-- ----------------------------------------------------------------------------
-- 建表: mid_interest1_p 兴趣定向(一级) 特征表
-- ----------------------------------------------------------------------------
drop table if exists mid_interest1_p;   -- 表名
create table if not exists mid_interest1_p   -- 表名
    (
    mid string,
    interest1 string       -- 字段名
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into
    mid_interest1_p   -- 表名
select
    distinct mid_app_label_p.m_id,                              -- ---------------------------------- 选库和表
    feature_name
from
    pangxk.mid_app_label_p lateral view explode(split(interest1, ' ')) table_tmp  as feature_name    -- 选库和表,特征,分隔符
where length(feature_name)<>0 and feature_name is not null;

-- ----------------------------------------------------------------------------
-- 建表: mid_interest1_2_p 兴趣定向(一级&二级) 特征表
-- ----------------------------------------------------------------------------
drop table if exists mid_interest1_2_p;   -- 表名
create table if not exists mid_interest1_2_p   -- 表名
    (
    mid string,
    interest1_2 string       -- 字段名
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into
    mid_interest1_2_p   -- 表名
select
    distinct mid_app_label_p.m_id,                              -- ---------------------------------- 选库和表
    feature_name
from
    pangxk.mid_app_label_p lateral view explode(split(interest1_2, ' ')) table_tmp  as feature_name    -- 选库和表,特征,分隔符
where length(feature_name)<>0 and feature_name is not null;

