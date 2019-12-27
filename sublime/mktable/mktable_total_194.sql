-- 
-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
-- 总体样本: databank.freq_info  440466597
-- mid                     string
-- sex                     int
-- age                     int
-- income                  int
-- province                string
-- city                    string
-- l_province              string
-- l_city                  string
-- l_area                  string
-- app_freq                map<string,map<string,int>>
-- label_freq              map<string,map<string,int>>
-- longitude               float
-- latitude                float
-- op                      string
-- m                       int
-- 
-- # Partition Information
-- # col_name              data_type               comment
-- 
-- op                      string
-- m                       int      
-- ----------------------------------------------------------------------------
-- 目标样本: default.positive_sample   349
-- mid                     string
-- sex                     int
-- age                     int
-- income                  int
-- province                string
-- city                    string
-- l_province              string
-- l_city                  string
-- l_area                  string
-- app_freq                map<string,map<string,int>>
-- label_freq              map<string,map<string,int>>
-- bsn                     string

-- # Partition Information
-- # col_name              data_type               comment

-- bsn                     string 
-- ----------------------------------------------------------------------------
-- 目标样本(备): default.positive_samples   349
-- mid_phone               string
-- sex                     int
-- age                     int
-- income                  int
-- province                string
-- city                    string
-- l_province              string
-- l_city                  string
-- l_area                  string
-- app_freq                map<string,map<string,int>>
-- label_freq              map<string,map<string,int>>
-- ----------------------------------------------------------------------------

-- ----------------------------------------------------------------------------
-- 建表：生成总体的特征表，包括用户静态表（mid_ttlabel）和 用户app动态表（mid_app_label）
-- ----------------------------------------------------------------------------

-- ----------------------------------------------------------------------------
-- 新建库 infoflow 或 pangxk
-- ----------------------------------------------------------------------------
drop database if exists pangxk;
-- drop database if exists pangxk cascade;    -- 强制删除
create database if not exists pangxk;
use pangxk;
-- drop database if exists infoflow;
-- -- drop database if exists infoflow cascade;    -- 强制删除
-- create database if not exists infoflow;
-- use infoflow;
select current_database();

-- ----------------------------------------------------------------------------
-- 192.168.0.194 服务器上数据表地址 /root/pangxk/data/
-- ----------------------------------------------------------------------------
-- 新建性别映射表 sex_ttsex : 本地性别标签和头条性别标签映射
-- ----------------------------------------------------------------------------
drop table if exists sex_ttsex;
create table if not exists sex_ttsex
    (
    sex int,
    ttsex string
    )
row format delimited
fields terminated by ',';
-- 加载本地数据到 sex_ttsex 数据表
load data local inpath '/root/pangxk/data/sex_ttsex.csv' overwrite into table sex_ttsex;

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
load data local inpath '/root/pangxk/data/age_ttage.csv' overwrite into table age_ttage;

-- ----------------------------------------------------------------------------
-- 新建app本地特征表 app_local
-- ----------------------------------------------------------------------------
drop table if exists app_local;
create table if not exists app_local
    (
    appid int,
    appname string,
    label_1 string,
    label_2 string
    )
row format delimited
fields terminated by ',';
-- 加载本地数据到 app_local 数据表
load data local inpath '/root/pangxk/data/app_local.csv' overwrite into table app_local;

-- ----------------------------------------------------------------------------
-- 新建app头条特征表 app_label
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
load data local inpath '/root/pangxk/data/appid_table.csv' overwrite into table app_label;
-- 改变数据表 app_label appid 数据类型为string
alter table app_label change app_id app_id string;

-- ----------------------------------------------------------------------------
-- 新建 mid_address 数据表(ORC格式) : 本地mid和本地地域标签映射
-- ----------------------------------------------------------------------------
drop table if exists mid_address;
create table if not exists mid_address
    (
    id string,
    province string,
    city string,
    l_province string,
    l_city string,
    l_area string
    )   -- 直接使用本地地址标签作为头条地址标签
row format delimited
fields terminated by ','
stored as orc;
-- 插入来自databank.freq_info 的数据
insert into mid_address
select
    distinct mid,   -- 使用 distinct 只对 mid 字段去重复 distinct
    province,
    city,
    l_province,
    l_city,
    l_area
from databank.freq_info;

-- ----------------------------------------------------------------------------
-- 新建 mid_ttlabel_ 数据表(ORC格式) 
-- ----------------------------------------------------------------------------
drop table if exists mid_ttlabel_;
create table if not exists mid_ttlabel_
    (
    mid string,
    ttsex string, 
    ttage string
    )
row format delimited
fields terminated by ','
stored as orc;

-- ----------------------------------------------------------------------------
-- 新建 mid_ttlabel 数据表(ORC格式) 
-- ----------------------------------------------------------------------------
drop table if exists mid_ttlabel;
create table if not exists mid_ttlabel
    (
    mid string,
    ttsex string, 
    ttage string,
    province string,
    city string,
    l_province string,
    l_city string,
    l_area string
    )
row format delimited
fields terminated by ','
stored as orc;

-- ----------------------------------------------------------------------------
-- 总体 databank.freq_info.mid, sex_ttsex.ttsex, age_ttage.ttage 合并成 mid_ttlabel_ 
-- ----------------------------------------------------------------------------
insert into table mid_ttlabel_ (mid, ttsex, ttage)
select t1.mid, t2.ttsex, t3.ttage
from (select distinct mid, sex, age from databank.freq_info) as t1
join (select sex, ttsex from sex_ttsex) as t2
join (select age, ttage from age_ttage) as t3
on t1.sex = t2.sex and t1.age = t3.age;

-- ----------------------------------------------------------------------------
-- 数据表 mid_ttlabel_ 与 mid_address 合并成 mid_ttlabel 
-- ----------------------------------------------------------------------------
insert into table mid_ttlabel (mid, ttsex, ttage, province, city, l_province, l_city, l_area)
select distinct t1.mid, t1.ttsex, t1.ttage, t2.province, t2.city, t2.l_province, t2.l_city, t2.l_area
from (select distinct mid, ttsex, ttage from mid_ttlabel_) as t1
join (select distinct id, province, city, l_province, l_city, l_area from mid_address) as t2
on t1.mid = t2.id;

-- ----------------------------------------------------------------------------
-- 删除中间数据表 mid_address 和 mid_ttlabel_
-- ----------------------------------------------------------------------------
drop table if exists mid_address;
drop table if exists mid_ttlabel_;

-- ----------------------------------------------------------------------------
-- 新建 mid_appid 数据表(ORC格式)
-- ----------------------------------------------------------------------------
drop table if exists mid_appid;
create table if not exists mid_appid
    (
    mid string,
    appid string,
    daycount int,
    pv int,
    freq int)
row format delimited
fields terminated by ','
collection items terminated by '-'
map keys terminated by ':'
stored as orc;
-- 插入数据
insert into mid_appid
select distinct mid, key as appid, value['daycount'] as daycount, value['pv'] as pv, value['freq'] as freq
from databank.freq_info lateral view explode(app_freq) temp;

-- ----------------------------------------------------------------------------
-- 新建 mid_app_label 数据表(ORC格式)
-- ----------------------------------------------------------------------------
drop table if exists mid_app_label;
create table if not exists mid_app_label
    (
    m_id string,
    appid string,
    appname string,
    daycount int,
    pv int,
    freq int,
    article string,
    behavior string,
    interest1 string,
    interest1_2 string
    )
partitioned by (m int comment 'month')
row format delimited
fields terminated by ','
stored as orc;
-- 数据表 mid_appid 与 app_label 合成到 mid_app_label
insert into table mid_app_label partition(m=201909)
(m_id, appid, appname, daycount, pv, freq, article, behavior, interest1, interest1_2)
select t2.mid, t2.appid, t1.appname, t2.daycount, t2.pv, t2.freq, t1.article, t1.behavior, t1.interest1, t1.interest1_2
from (select app_id, appname, article, behavior, interest1, interest1_2 from app_label) as t1
join (select mid, appid, daycount, pv, freq from mid_appid) as t2
on t1.app_id = t2.appid;

-- ----------------------------------------------------------------------------
-- 删除中间数据表 mid_appid
-- ----------------------------------------------------------------------------
drop table if exists mid_appid;

-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------



