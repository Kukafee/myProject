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
-- 新建库 infoflow
-- ----------------------------------------------------------------------------
-- drop database if exists pangxk cascade;
-- create database if not exists pangxk;
-- use pangxk;
drop database if exists infoflow;
-- drop database if exists infoflow cascade;    -- 强制删除数据库 infoflow
create database if not exists infoflow;    -- 创建数据库 infoflow
use infoflow;    -- 使用数据库
select current_database();    -- 显示当前使用的数据库


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
load data local inpath '/home/pangxk/data/sex_ttsex.csv' overwrite into table sex_ttsex;

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
load data local inpath '/home/pangxk/data/app_local.csv' overwrite into table app_local;

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
-- 新建 总体用户静态表 mid_ttlabel 数据表(ORC格式) 
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
-- ？总体 databank.freq_info.mid, sex_ttsex.ttsex, age_ttage.ttage 合并为 mid_ttlabel 数据表(ORC格式)
-- ----------------------------------------------------------------------------
insert into table mid_ttlabel (mid, ttsex, ttage, province, city, l_province, l_city, l_area)
select t1.mid, t2.ttsex, t3.ttage, t1.province, t1.city, t1.l_province, t1.l_city, t1.l_area
from (select distinct mid, sex, age, province, city, l_province, l_city, l_area
      from databank.freq_info
     ) as t1
inner join (select sex, ttsex from sex_ttsex) as t2
inner join (select age, ttage from age_ttage) as t3
on t1.sex = t2.sex and t1.age = t3.age;


-- ----------------------------------------------------------------------------
-- 新建 正样本用户静态表 mid_ttlabel_p 数据表(ORC格式) 
-- ----------------------------------------------------------------------------
drop table if exists mid_ttlabel_p;
create table if not exists mid_ttlabel_p
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
-- ？正样本 default.positive_sample.mid, sex_ttsex.ttsex, age_ttage.ttage 合并为 mid_ttlabel_p 数据表(ORC格式) 
-- ----------------------------------------------------------------------------
insert into table mid_ttlabel_p (mid, ttsex, ttage, province, city, l_province, l_city, l_area)
select t1.mid, t2.ttsex, t3.ttage, t1.province, t1.city, t1.l_province, t1.l_city, t1.l_area
from (select distinct mid, sex, age, province, city, l_province, l_city, l_area
      from default.positive_sample
     ) as t1
inner join (select sex, ttsex from sex_ttsex) as t2
inner join (select age, ttage from age_ttage) as t3
on t1.sex = t2.sex and t1.age = t3.age;



-- ----------------------------------------------------------------------------
-- 新建总体 用户与使用的appID映射表 mid_appid 数据表(ORC格式)
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
-- 新建目标群体 用户与使用的appID映射表 mid_appid_p 数据表(ORC格式)
-- ----------------------------------------------------------------------------
drop table if exists mid_appid_p;
create table if not exists mid_appid_p
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
insert into mid_appid_p
select distinct mid, key as appid, value['daycount'] as daycount, value['pv'] as pv, value['freq'] as freq
from default.positive_sample lateral view explode(app_freq) temp;


-- -- ----------------------------------------------------------------------------
-- -- ？新建总体用户 动态表 mid_app_label 数据表(ORC格式) [暂]
-- -- ----------------------------------------------------------------------------
-- drop table if exists mid_app_label;
-- create table if not exists mid_app_label
--     (
--     mid string,
--     appid string,
--     appname string,
--     daycount int,
--     pv int,
--     freq int,
--     article string,
--     behavior string,
--     interest1 string,
--     interest1_2 string
--     )
-- partitioned by (m int comment 'month')
-- row format delimited
-- fields terminated by ','
-- stored as orc;
-- -- 数据表 mid_appid 与 app_label 合成到 mid_app_label
-- insert into table mid_app_label partition(m=201909)
-- (mid, appid, appname, daycount, pv, freq, article, behavior, interest1, interest1_2)
-- select t2.mid, t2.appid, t1.appname, t2.daycount, t2.pv, t2.freq, t1.article, t1.behavior, t1.interest1, t1.interest1_2
-- from (select app_id, appname, article, behavior, interest1, interest1_2 from app_label) as t1
-- join (select mid, appid, daycount, pv, freq from mid_appid) as t2
-- on t1.app_id = t2.appid;


-- ----------------------------------------------------------------------------
-- ？新建目标群体 动态表 mid_app_label_p 数据表(ORC格式)
-- ----------------------------------------------------------------------------
drop table if exists mid_app_label_p;
create table if not exists mid_app_label_p
    (
    mid string,
    appid string,
    appname string,
    daycount int,
    pv int,
    freq int,
    article string,
    behavior string,
    interest1 string,
    interest1_2 string)
partitioned by (
m int comment 'month',
bsn string)
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into table mid_app_label_p partition(m=201909, bsn='acadsoc')
(mid, appid, appname, daycount, pv, freq, article, behavior, interest1, interest1_2)
select t2.mid, t2.appid, t1.appname, t2.daycount, t2.pv, t2.freq, t1.article, t1.behavior, t1.interest1, t1.interest1_2
from (select app_id, appname, article, behavior, interest1, interest1_2 from app_label) as t1
join (select mid, appid, daycount, pv, freq from mid_appid_p) as t2
on t1.app_id = t2.appid;


-- ----------------------------------------------------------------------------
-- 删除中间数据表
-- ----------------------------------------------------------------------------

drop table if exists mid_appid_p;

