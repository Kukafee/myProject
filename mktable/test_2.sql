
-- /Users/edz/pang/code/sublime/mktable/test_2.sql
-- -------------------------------------------------
-- 去重复 使用了 distinct 
-- 创建并生成了 分区表 mid_app_labelp
-- -------------------------------------------------

-- 0.创建并使用数据库pangxk_
drop database if exists pangxk_ cascade;
create database if not exists pangxk_;
use pangxk_;
select current_database();

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

-- 创建 app_local 数据表(本地性别标签和头条性别标签映射)
drop table if exists app_local;
create table if not exists app_local(
appid int,
appname string,
label_1 string,
label_2 string)
row format delimited
fields terminated by ',';
-- 加载本地数据到 sex_ttsex 数据表
load data local inpath '/home/wangl/pangxk/app_local.csv' overwrite into table app_local; 

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
load data local inpath '/home/wangl/pangxk/appid_table.csv' overwrite into table app_label;
-- 改变数据表 app_label appid 数据类型
alter table app_label change app_id app_id string;

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
distinct mid,   -- 使用 distinct 只对 mid 字段去重复 distinct 
-- -- 使用concat 连接各字段, 并使用'-'作为分隔符,注意！分割符要与建表时的字段分隔符不同，此处属于字段内分割
-- concat(province, '-', city, '-', l_province, '-', l_city, '-', l_area) as address
province, city, l_province, l_city, l_area
from databank.freq_info;

-- 创建 mid_ttlabel_ 数据表(过度表)
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

-- 创建 mid_ttlabel 数据表()
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


-- 将数据databank.freq_info.mid,sex_ttsex.ttsex, age_ttage.ttage 合并成 mid_ttlabel_
insert into table mid_ttlabel_ (mid, ttsex, ttage)
select mid, ttsex, ttage
from (select distinct mid, sex, age from databank.freq_info) as t1
join (select sex, ttsex from sex_ttsex) as t2
join (select age, ttage from age_ttage) as t3
on t1.sex = t2.sex and t1.age = t3.age;

-- 将数据表mid_ttlabel_, mid_address 合并成 mid_ttlabel
insert into table mid_ttlabel (mid, ttsex, ttage, province, city, l_province, l_city, l_area)
select distinct mid, ttsex, ttage, province, city, l_province, l_city, l_area
from (select distinct mid, ttsex, ttage from mid_ttlabel_) as t1
join (select distinct id, province, city, l_province, l_city, l_area from mid_address) as t2
on t1.mid = t2.id;

-- 删除中间数据表 mid_ttlabel_
drop table if exists mid_ttlabel_;


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
select distinct mid, key as appid, value['daycount'] as daycount, value['pv'] as pv, value['freq'] as freq
from databank.freq_info lateral view explode(app_freq) temp;


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
interest1_2 string)
-- partitioned by (
-- op string comment 'operator',
-- m int comment 'month')
row format delimited
fields terminated by ',';

-- 将数据表 mid_appid, app_label 合并成 mid_app_label
insert into table mid_app_label
(m_id, appid, appname, daycount, pv, freq, article, behavior, interest1, interest1_2)
select distinct mid, appid, appname, daycount, pv, freq, article, behavior, interest1, interest1_2
from (select distinct mid, appid, daycount, pv, freq from mid_appid) as t1 --
join (select distinct app_id, appname, article, behavior, interest1, interest1_2 from app_label) as t2
on t1.appid = t2.app_id;

-- 创建 mid_app_labelp 分区数据表
drop table if exists mid_app_labelp;
create table if not exists mid_app_labelp(
m_id string,
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
m int comment 'month')
row format delimited
fields terminated by ',';
-- 把 mid_app_label 数据拷贝至 分区表 mid_app_labelp
insert into mid_app_labelp partition(m=201909)
select
m_id, appid, appname, daycount, pv, freq, article, behavior, interest1, interest1_2
from mid_app_label;


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
interest1_2 string)
-- partitioned by (
-- op string comment 'operator',
-- m int comment 'month')
row format delimited
fields terminated by ',';

-- 将数据表 mid_ttlabel, mid_app_label 合并成 pers_feature
insert into table pers_feature
(mid, ttsex, ttage, province, city, l_province, l_city, l_area,
appid, appname, daycount, pv, freq, article, behavior, interest1, interest1_2)
select
distinct mid, ttsex, ttage, province, city, l_province, l_city, l_area,
appid, appname, daycount, pv, freq, article, behavior, interest1, interest1_2
from (select distinct mid, ttsex, ttage, province, city, l_province, l_city, l_area from mid_ttlabel) as t1
join (select distinct m_id, appid, appname, daycount, pv, freq, article, behavior, interest1, interest1_2 from mid_app_label) as t2
on t1.mid = t2.m_id;






















-- distinct

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
-- op                      string
-- m                       int    


-- select
-- distinct province as province,
-- -- mid,   -- 使用 distinct 只对 mid 字段去重复 distinct 
-- -- -- 使用concat 连接各字段, 并使用'-'作为分隔符,注意！分割符要与建表时的字段分隔符不同，此处属于字段内分割
-- -- concat(province, '-', city, '-', l_province, '-', l_city, '-', l_area) as address
-- city, l_province, l_city, l_area
-- from databank.freq_info limit 10;

-- drop table if exists test;
-- create table if not exists test1(
-- mid string)
-- row format delimited
-- fields terminated by ',';
-- insert into table test values
-- ('kukafee', 'hebei', 22),
-- ('xietao','sichuan', 23),
-- ('wangyong','anhui', 22),
-- ('kukafee','hebei', 23),
-- ('kukafee','hebei', 22),
-- ('shuji','hubei', 23);

-- select distinct mid, age from test; 



-- SELECT * FROM(
-- select * from customer where user=(
-- 	SELECT source_user from customer WHERE user='admin') UNION ALL select * from customer where user=(
-- 	select source_user from customer where user=(
-- 		SELECT source_user from customer WHERE user='admin')) union ALL select * from customer where user=(
-- 	select source_user from customer where user=(
-- 		select source_user from customer where user=(
-- 			SELECT source_user from customer WHERE user='admin'))) UNION ALL select * from customer where source_user=(/*我的上线的上线的user*/
-- 	select user from customer where user=(
-- 		select source_user from customer where user=(
-- 			SELECT source_user from customer WHERE user='admin'))) union all select * from customer where source_user=(/*我的上线的上线的上线user*/
-- 	select user from customer where user=(
-- 	select source_user from customer where user=(
-- 		select source_user from customer where user=(
-- 			SELECT source_user from customer WHERE user='admin'))))) as alias group by user;

















