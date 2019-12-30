
-- use infoflow;    -- 使用数据库 infoflow
use pangxk;
-- ----------------------------------------------------------------------------
-- 总体样本建表
--     app特征
--         app_article 文章分类 特征表 count(appid)=11643
--         app_behavior 文章分类 特征表 count(appid)=8059
--         app_interest1 文章分类 特征表 count(appid)=6941
--         app_interest1_2 文章分类 特征表.  count(appid)=17631
--     mid用户特征
--         mid_article 文章分类 特征表
--         mid_behavior app行为 特征表
--         mid_interest1 兴趣定向(一级) 特征表
--         mid_interest1_2 兴趣定向(一级&二级) 特征表
-- ----------------------------------------------------------------------------

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


-- ----------------------------------------------------------------------------
-- -- 配置参数
-- ----------------------------------------------------------------------------
set mapreduce.job.reduce.slowstart.completedmaps=0.85;


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
inner join mid_appid t2         -- mid_appid.mid
on(t1.appid=t2.appid);

-- ----------------------------------------------------------------------------
-- 建表: mid_behavior app行为 特征表. Time taken: 19209.452 seconds
-- ----------------------------------------------------------------------------
drop table if exists mid_behavior;   -- 表名
create table if not exists mid_behavior   -- 表名
    (
    mid string,
    behavior string       -- 字段名
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into mid_behavior(mid, behavior)
select
    distinct t2.mid, t1.behavior
from app_behavior t1 
inner join mid_appid t2         -- mid_appid.mid
on(t1.appid=t2.appid);

-- ----------------------------------------------------------------------------
-- 建表: mid_interest1 兴趣定向(一级) 特征表  9421.205 seconds
-- ----------------------------------------------------------------------------
drop table if exists mid_interest1;   -- 表名
create table if not exists mid_interest1   -- 表名
    (
    mid string,
    interest1 string       -- 字段名
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into mid_interest1(mid, interest1)
select
    distinct t2.mid, t1.interest1
from app_interest1 t1 
inner join mid_appid t2
on(t1.appid=t2.appid);

-- ----------------------------------------------------------------------------
-- 建表: mid_interest1_2 兴趣定向(一级&二级) 特征表
-- ----------------------------------------------------------------------------
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
inner join mid_appid t2
on(t1.appid=t2.appid);




-- ----------------------------------------------------------------------------
-- 正样本建表
--     mid用户特征
--         mid_article_p 文章分类 特征表
--         mid_behavior_p app行为 特征表
--         mid_interest1_p 兴趣定向(一级) 特征表
--         mid_interest1_2_p 兴趣定向(一级&二级) 特征表
-- ----------------------------------------------------------------------------

-- ----------------------------------------------------------------------------
-- -- 配置参数
-- ----------------------------------------------------------------------------
set mapreduce.job.reduce.slowstart.completedmaps=0.5;


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
    distinct mid_app_label_p.mid, -- ------------ 选库和表
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
    distinct mid_app_label_p.mid, -- --------- 选库和表
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
    distinct mid_app_label_p.mid,  -- --------------- 选库和表
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
    distinct mid_app_label_p.mid,  -- ------------------ 选库和表
    feature_name
from
    pangxk.mid_app_label_p lateral view explode(split(interest1_2, ' ')) table_tmp  as feature_name    -- 选库和表,特征,分隔符
where length(feature_name)<>0 and feature_name is not null;







