


use pangxk;
-- ----------------------------------------------------------------------------
-- 建表: app_article 文章分类 特征表 count(appid)=11643
-- ----------------------------------------------------------------------------
drop table if exists app_article;   -- 表名
create table if not exists app_article   -- 表名
    (
    appid string,
    article string       -- 字段名
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into
    app_article(appid, article)        -- 表名
select
    distinct app_label.app_id,                              -- ---------------------------------- 选库和表
    feature_name
from
    pangxk.app_label lateral view explode(split(article, ' ')) table_tmp  as feature_name    -- 选库和表,特征,分隔符
where length(feature_name)<>0 and feature_name is not null;


-- desc app_label;
-- app_id                  string
-- appname                 string
-- article                 string
-- behavior                string
-- interest1               string
-- interest1_2             string       

-- ----------------------------------------------------------------------------
-- 建表: app_behavior 文章分类 特征表 count(appid)=8059
-- ----------------------------------------------------------------------------
drop table if exists app_behavior;   -- 表名
create table if not exists app_behavior   -- 表名
    (
    appid string,
    behavior string       -- 字段名
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into
    app_behavior(appid, behavior)        -- 表名
select
    distinct app_label.app_id,                              -- ---------------------------------- 选库和表
    feature_name
from
    pangxk.app_label lateral view explode(split(behavior, ' ')) table_tmp  as feature_name    -- 选库和表,特征,分隔符
where length(feature_name)<>0 and feature_name is not null;

-- ----------------------------------------------------------------------------
-- 建表: app_interest1 文章分类 特征表 count(appid)=6941
-- ----------------------------------------------------------------------------
drop table if exists app_interest1;   -- 表名
create table if not exists app_interest1   -- 表名
    (
    appid string,
    interest1 string       -- 字段名
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into
    app_interest1(appid, interest1)        -- 表名
select
    distinct app_label.app_id,                              -- ---------------------------------- 选库和表
    feature_name
from
    pangxk.app_label lateral view explode(split(interest1, ' ')) table_tmp  as feature_name    -- 选库和表,特征,分隔符
where length(feature_name)<>0 and feature_name is not null;


-- ----------------------------------------------------------------------------
-- 建表: app_interest1_2 文章分类 特征表.  count(appid)=17631
-- ----------------------------------------------------------------------------
drop table if exists app_interest1_2;   -- 表名
create table if not exists app_interest1_2   -- 表名
    (
    appid string,
    interest1_2 string       -- 字段名
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into
    app_interest1_2(appid, interest1_2)        -- 表名
select
    distinct app_label.app_id,                              -- ---------------------------------- 选库和表
    feature_name
from
    pangxk.app_label lateral view explode(split(interest1_2, ' ')) table_tmp  as feature_name    -- 选库和表,特征,分隔符
where length(feature_name)<>0 and feature_name is not null;














