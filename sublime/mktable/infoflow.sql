

-- 按照用户性别特征男女分类之后，重新计算TGI值

-- ----------------------------------------------------------------------------
-- 新建库 infoflow
-- ----------------------------------------------------------------------------
-- drop database if exists infoflow cascade;
-- create database if not exists infoflow;
use infoflow;


-- ----------------------------------------------------------------------------
-- 192.168.0.194 服务器上数据表地址 /root/pangxk/data/
-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
-- 新建年龄映射表 age_ttage : 本地年龄和头条年龄标签映射
-- ----------------------------------------------------------------------------
drop table if exists age_ttage;
create table if not exists age_ttage
    (
    age int,
    ttage string
    )
row format delimited
fields terminated by ',';
-- 加载本地数据到 age_ttage 数据表
load data local inpath '/home/pangxk/data/age_ttage.csv' overwrite into table age_ttage;


-- ----------------------------------------------------------------------------
-- 新建 app 与头条特征映射表 app_label
-- ----------------------------------------------------------------------------
drop table if exists app_label;
create table if not exists app_label
    (
    app_id int,
    appname string,
    article string,
    behavior string,
    interest1 string,
    interest1_2 string
    )
row format delimited
fields terminated by ',';
-- 加载本地数据到 app_label 数据表
load data local inpath '/home/pangxk/data/appid_table.csv' overwrite into table app_label;
-- 改变数据表 app_label appid 数据类型为string
alter table app_label change app_id app_id string;

-- ----------------------------------------------------------------------------
-- 新建 全量男年龄特征表 pop_age_m 
-- ----------------------------------------------------------------------------
drop table if exists pop_age_m;
create table if not exists pop_age_m
    (
    mid string,
    ttage string
    )
row format delimited
fields terminated by ','
stored as orc;

-- ----------------------------------------------------------------------------
-- 新建 全量女年龄特征表 pop_age_w 
-- ----------------------------------------------------------------------------
drop table if exists pop_age_w;
create table if not exists pop_age_w
    (
    mid string,
    ttage string
    )
row format delimited
fields terminated by ','
stored as orc;

-- ----------------------------------------------------------------------------
-- 新建 样本男年龄特征表 bsn_age_m
-- ----------------------------------------------------------------------------
drop table if exists bsn_age_m;
create table if not exists bsn_age_m
    (
    mid string,
    ttage string
    )
row format delimited
fields terminated by ','
stored as orc;

-- ----------------------------------------------------------------------------
-- 新建 样本女年龄特征表 bsn_age_w 
-- ----------------------------------------------------------------------------
drop table if exists bsn_age_w;
create table if not exists bsn_age_w
    (
    mid string,
    ttage string
    )
row format delimited
fields terminated by ','
stored as orc;


-- ----------------------------------------------------------------------------
-- 新建 全量男年龄特征分布表 pop_age_dist_m 
-- ----------------------------------------------------------------------------
drop table if exists pop_age_dist_m;
create table if not exists pop_age_dist_m
    (
    age string,
    uv int,  -- 特征人数
    pop_per float
    )
row format delimited
fields terminated by ','
stored as orc;


-- ----------------------------------------------------------------------------
-- 新建 全量女年龄特征分布表 pop_age_dist_w
-- ----------------------------------------------------------------------------
drop table if exists pop_age_dist_w;
create table if not exists pop_age_dist_w
    (
    age string,
    uv int,  -- 特征人数
    pop_per float
    )
row format delimited
fields terminated by ','
stored as orc;


-- ----------------------------------------------------------------------------
-- 新建 样本男年龄特征分布表 bsn_age_dist_m 
-- ----------------------------------------------------------------------------
drop table if exists bsn_age_dist_m;
create table if not exists bsn_age_dist_m
    (
    age string,
    uv int,  -- 特征人数
    bsn_per float
    )
row format delimited
fields terminated by ','
stored as orc;


-- ----------------------------------------------------------------------------
-- 新建 样本女年龄特征分布表 bsn_age_dist_w 
-- ----------------------------------------------------------------------------
drop table if exists bsn_age_dist_w;
create table if not exists bsn_age_dist_w
    (
    age string,
    uv int,  -- 特征人数
    bsn_per float
    )
row format delimited
fields terminated by ','
stored as orc;


-- 配置参数
set mapreduce.job.reduce.slowstart.completedmaps=0.85;
-- ----------------------------------------------------------------------------
-- 全量 databank.freq_info.mid, age_ttage.ttage 合并为 pop_age_m
-- 1 887 063 239 总数量
-- databank.freq_info.mid 201909 分区中数据 440 466 597
-- ----------------------------------------------------------------------------
insert into table pop_age_m (mid, ttage)
select t1.mid, t2.ttage
from (select distinct mid, age
      from databank.freq_info where (op='liantong' and m=201909 and sex=0)
     ) as t1
inner join (select age, ttage from age_ttage) as t2
on t1.age = t2.age;


-- ----------------------------------------------------------------------------
-- 全量 databank.freq_info.mid, age_ttage.ttage 合并为 pop_age_w
-- ----------------------------------------------------------------------------
insert into table pop_age_w (mid, ttage)
select t1.mid, t2.ttage
from (select distinct mid, age
      from databank.freq_info where (op='liantong' and m=201909 and sex=1)
     ) as t1
inner join (select age, ttage from age_ttage) as t2
on t1.age = t2.age;


-- ----------------------------------------------------------------------------
-- 样本 default.positive_sample.mid, age_ttage.ttage 合并为 bsn_age_m
-- ----------------------------------------------------------------------------
insert into table bsn_age_m (mid, ttage)
select t1.mid, t2.ttage
from (select distinct mid, age
      from default.positive_sample where (op='liantong' and m=201909 and sex=0)
     ) as t1
inner join (select age, ttage from age_ttage) as t2
on t1.age = t2.age;


-- ----------------------------------------------------------------------------
-- 样本 default.positive_sample.mid, age_ttage.ttage 合并为 bsn_age_w
-- ----------------------------------------------------------------------------
insert into table bsn_age_w (mid, ttage)
select t1.mid, t2.ttage
from (select distinct mid, age
      from default.positive_sample where (op='liantong' and m=201909 and sex=1)
     ) as t1
inner join (select age, ttage from age_ttage) as t2
on t1.age = t2.age;



















