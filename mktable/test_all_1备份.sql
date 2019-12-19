-- /Users/edz/pang/code/sublime/test.sql
-- 0.创建数据库pangxk
drop database if exists pangxk cascade;
create database if not exists pangxk;

use pangxk;
select current_database();

-- 创建 positive_sample 数据表(正样本数据表)
-- drop table if exists positive_sample;
-- create table if not exists positive_sample
-- like wangl.positive_sample;

-- 创建 sex_ttsex 数据表(本地性别标签和头条性别标签映射)
drop table if exists sex_ttsex;
create table if not exists sex_ttsex(
	sex int,
	ttsex string)
row format delimited
fields terminated by ',';
-- 加载本地数据到 sex_ttsex 数据表
load data local inpath '/home/wangl/pangxk/sex_ttsex.csv' overwrite into table sex_ttsex; 

-- 创建 age_ttage 数据表(本地年龄和头条年龄标签映射)
drop table if exists age_ttage;
create table if not exists age_ttage(
	age int,
	ttage string)
row format delimited
fields terminated by ',';
-- 加载本地数据到 age_ttage 数据表
load data local inpath '/home/wangl/pangxk/age_ttage.csv' overwrite into table age_ttage; 

-- 创建 mid_address 数据表(本地mid和本地区域标签映射)
drop table if exists mid_address;
create table if not exists mid_address(
id string,
province string,
city string,
l_province string,
l_city string,
l_area string)   -- 直接使用本地地址标签作为头条地址标签
row format delimited
fields terminated by ',';
-- 给 mid_address 数据表插入来自databank.freq_info 的数据
insert into mid_address
select 
mid, 
-- -- 使用concat 连接各字段, 并使用'-'作为分隔符,注意！分割符要与建表时的字段分隔符不同，此处属于字段内分割
-- concat(province, '-', city, '-', l_province, '-', l_city, '-', l_area) as address
province, city, l_province, l_city, l_area
from databank.freq_info;

-- 创建 mid_ttlabel_ 数据表(本地年龄和头条年龄标签映射)
drop table if exists mid_ttlabel_;
create table if not exists mid_ttlabel_(
mid string,
ttsex string, 
ttage string
)
-- partitioned by (
-- op string comment 'operator',
-- m int comment 'month')
row format delimited
fields terminated by ',';
-- -- 给 mid_ttlabel_ 数据表插入来自databank.freq_info 的地址数据
-- insert into mid_ttlabel_(mid, ttaddress)
-- select 
-- mid, 
-- concat(province, city, l_province, l_city, l_area) as address
-- from databank.freq_info;

-- 创建 mid_ttlabel 数据表(本地年龄和头条年龄标签映射)备用
drop table if exists mid_ttlabel;
create table if not exists mid_ttlabel(
mid string,
ttsex string, 
ttage string,
province string,
city string,
l_province string,
l_city string,
l_area string
)
-- partitioned by (
-- op string comment 'operator',
-- m int comment 'month')
row format delimited
fields terminated by ',';

-- -- 数据表mid_ttlabel与数据表sex_ttsex, age_ttage, mid_address 合并
-- insert into table mid_ttlabel (mid, id, ttsex, ttage, ttaddress)
-- select databank.freq_info.mid, databank.freq_info.mid, sex_ttsex.ttsex, age_ttage.ttage, mid_address.ttaddress
-- from (select mid, id, sex, age from databank.freq_info) as t1
-- join (select sex, ttsex from sex_ttsex) as t2
-- join (select age, ttage from age_ttage) as t3
-- join (select id, ttaddress from mid_address) as t4 -- 需要将mid更改为id，不能与mid_ttlabel中的mid同名
-- on t1.sex = t2.sex and t1.age = t3.age and t1.mid = t4.id;

-- 将数据databank.freq_info.mid,sex_ttsex.ttsex, age_ttage.ttage 合并成 mid_ttlabel_
insert into table mid_ttlabel_ (mid, ttsex, ttage)
select mid, ttsex, ttage
from (select mid, sex, age from databank.freq_info) as t1
join (select sex, ttsex from sex_ttsex) as t2
join (select age, ttage from age_ttage) as t3
on t1.sex = t2.sex and t1.age = t3.age;

-- 将数据表mid_ttlabel_, mid_address 合并成 mid_ttlabel
insert into table mid_ttlabel (mid, ttsex, ttage, province, city, l_province, l_city, l_area)
select mid, ttsex, ttage, province, city, l_province, l_city, l_area
from (select mid, ttsex, ttage from mid_ttlabel_) as t1
join (select id, province, city, l_province, l_city, l_area from mid_address) as t2
on t1.mid = t2.id;

-- 删除中间数据表 mid_ttlabel_
drop table if exists mid_ttlabel_;

------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------

-- 创建 app_label 数据表(本地年龄和头条年龄标签映射)
drop table if exists app_label;
create table if not exists app_label(
app_id int,
appname string,
article string,
behavior string,
interest1 string,
interest1_2 string)
row format delimited
fields terminated by ',';
-- 加载本地数据到 app_label 数据表
load data local inpath '/home/wangl/pangxk/app_label.csv' overwrite into table app_label;

-- 检查 app_label 表数据
select * from app_label limit 10; 

-- 创建 mid_appid 数据表
drop table if exists mid_appid;
create table if not exists mid_appid(
mid string,
appid string,
daycount int,
pv int,
freq int)
-- partitioned by (
-- op string comment 'operator',
-- m int comment 'month')
row format delimited
fields terminated by ','
collection items terminated by '-'
map keys terminated by ':';
-- 插入数据
insert into mid_appid
select mid, key as appid, value['daycount'] as daycount, value['pv'] as pv, value['freq'] as freq
from databank.freq_info lateral view explode(app_freq) temp;


-- P_792591E8AAMGQMJY      551     2       3       2
-- P_792591E8AAMGQMJY      232     1       6       2
-- P_792591E8AAMGQMJY      476     23      78      1
-- P_792591E8AAMGQMJY      630     1       1       3
-- P_792591E8AAMGQMJY      479     1       1       3
-- P_792591E8AAMGQMJY      996     1       1       3 
-- 1.作用于单列 select distinct name from A
-- 作用于多列s elect distinct name, id from A

-- 改变数据表 app_label appid 字段名和类型
alter table app_label change app_id app_id string;

-- 创建 mid_app_label 数据表
drop table if exists mid_app_label;
create table if not exists mid_app_label(
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
-- partitioned by (
-- op string comment 'operator',
-- m int comment 'month')
row format delimited
fields terminated by ',';

-- 将数据表 mid_appid, app_label 合并成 mid_app_label
insert into table mid_app_label
(m_id, appid, appname, daycount, pv, freq, article, behavior, interest1, interest1_2)
select mid, appid, appname, daycount, pv, freq, article, behavior, interest1, interest1_2
from (select mid, appid, daycount, pv, freq from mid_appid) as t1 --
join (select app_id, appname, article, behavior, interest1, interest1_2 from app_label) as t2
on t1.appid = t2.app_id;

-- 创建 pers_feature 数据表
drop table if exists pers_feature;
create table if not exists pers_feature( -- 17 个字段特征
mid string,
ttsex string,
ttage string,
province string,
city string,
l_province string,
l_city string,
l_area string,
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
-- partitioned by (
-- op string comment 'operator',
-- m int comment 'month')
row format delimited
fields terminated by ',';

-- 改变数据表 mid_app_label mid 字段名为 m_id ,便于join操作
-- alter table mid_app_label change mid m_id string;

-- 将数据表 mid_ttlabel, mid_app_label 合并成 pers_feature
insert into table pers_feature
(mid, ttsex, ttage, province, city, l_province, l_city, l_area,
appid, appname, daycount, pv, freq, article, behavior, interest1, interest1_2)
select
mid, ttsex, ttage, province, city, l_province, l_city, l_area,
appid, appname, daycount, pv, freq, article, behavior, interest1, interest1_2
from (select mid, ttsex, ttage, province, city, l_province, l_city, l_area from mid_ttlabel) as t1
join (select m_id, appid, appname, daycount, pv, freq, article, behavior, interest1, interest1_2 from mid_app_label) as t2
on t1.mid = t2.m_id;





