

-- 优化Hive执行引擎
-- set hive.execution.engine=tez;    -- 使用 tez 引擎
-- set hive.prewarm.enabled=true;    -- 告诉hive创建Tez容器
-- set hive.prewarm.numcontainers=10;    -- 调整Tez专用容器的数量

-- 调整mapreduce.job.reduce.slowstart.completedmaps参数，默认为0.05，即map完成0.05后reduce就开始copy，
-- 如果集群资源不够，有可能导致reduce把资源全抢光，可以把这个参数调整到0.8，map完成80%后才开始reduce copy
set mapreduce.job.reduce.slowstart.completedmaps=0.8;
-- -- -------------------------------------------------------------------------
use pangxk;
-- --article------------------Time taken: 19266.82 seconds-----------------------------------
-- 创建 mid_article 数据表
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
    distinct t2.m_id, t1.article    
from app_article t1 
inner join mid_app_label2 t2         -- mid_appid -- mid_appid2
on(t1.appid=t2.appid);

-- mid_appid2;
-- mid                     string
-- appid                   string
-- daycount                int
-- pv                      int
-- freq                    int
-- select count(mid) from mid_appid2;     6 572 818 042
-- select count(distinct mid) from mid_appid2;   
-- --behavior-------------------------------------------------------------------
use pangxk;
set mapreduce.job.reduce.slowstart.completedmaps=0.9;
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
inner join mid_appid2 t2         -- mid_appid2
on(t1.appid=t2.appid);

-- --interest1-----------------------------------------------------------------------------
set mapreduce.job.reduce.slowstart.completedmaps=0.8;
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
inner join mid_appid2 t2
on(t1.appid=t2.appid);

-- --interest1_2 -----------------------------------------------------------------------------
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





-- -- -----------------------------------------------------------------------------
-- -- 以下是测试代码
-- -- -----------------------------------------------------------------------------
-- create table tuser(
-- name string,
-- appid int )
-- row format delimited
-- fields terminated by ',';
-- insert into table tuser values
--     ('wang', 1),
--     ('wang', 2),
--     ('wang', 3),
--     ('li', 1),
--     ('li', 3);

-- create table tlabel(
-- appid int,
-- label string)
-- row format delimited
-- fields terminated by ',';
-- insert into table tlabel values
--     (1, '娱乐'),
--     (1, '体育'),
--     (2, '教育'),
--     (2, '体操'),
--     (3, '教育');

-- select
--     distinct t1.name, t2.label
-- from tuser t1 
-- join tlabel t2
-- on(t1.appid=t2.appid);
-- -- 测试输出
-- -- li      体育
-- -- li      娱乐
-- -- li      教育
-- -- wang    体操
-- -- wang    体育
-- -- wang    娱乐
-- -- wang    教育  

-- -- 期望输出
-- 'wang'	'娱乐'
-- 'wang'	'体育'
-- 'wang'	'教育'
-- 'wang'	'体操'
-- 'li'	'娱乐'
-- 'li'	'体育'
-- 'li'	'教育'

-- select
--     distinct t1.name, t2.label
-- from tuser t1 
-- left join tlabel t2
-- on(t1.appid=t2.appid);
-- -- 测试输出
-- -- li      体育
-- -- li      娱乐
-- -- li      教育
-- -- wang    体操
-- -- wang    体育
-- -- wang    娱乐
-- -- wang    教育  

-- drop table if exists tlabel;
-- drop table if exists tuser;


-- drop table if exists mid_article_;
