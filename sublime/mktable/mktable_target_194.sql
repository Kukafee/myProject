-- 
-- ----------------------------------------------------------------------------
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

-- ----------------------------------------------------------------------------
-- 建表：生成目标群体的特征表，包括用户静态表（mid_ttlabel）和 用户app动态表（mid_app_label）
-- ----------------------------------------------------------------------------

-- ----------------------------------------------------------------------------
-- 选中数据库
-- ----------------------------------------------------------------------------
use pangxk;
-- use pangxk_;
select current_database();

-- ----------------------------------------------------------------------------
-- 新建目标群体 mid_address_p 数据表(ORC格式)
-- ----------------------------------------------------------------------------

drop table if exists mid_address_p;
create table if not exists mid_address_p
    (
    id string,
    province string,
    city string,
    l_province string,
    l_city string,
    l_area string
    )
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into mid_address_p
select
    distinct mid,
    province,
    city,
    l_province,
    l_city,
    l_area
from default.positive_sample;

-- ----------------------------------------------------------------------------
-- 新建目标群体 mid_ttlabel_p_ 数据表(ORC格式)
-- ----------------------------------------------------------------------------
drop table if exists mid_ttlabel_p_;
create table if not exists mid_ttlabel_p_
    (
    mid string,
    ttsex string,
    ttage string
    )
row format delimited
fields terminated by ','
stored as orc;

-- ----------------------------------------------------------------------------
-- 新建目标群体 mid_ttlabel_p 数据表(ORC格式)
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
-- 目标群体 default.positive_sample.mid, sex_ttsex.ttsex, age_ttage.ttage 合并成 mid_ttlabel_p_ 
-- ----------------------------------------------------------------------------
insert into table mid_ttlabel_p_ (mid, ttsex, ttage)
select t1.mid, t2.ttsex, t3.ttage
from (select distinct mid, sex, age from default.positive_sample) as t1
join (select sex, ttsex from sex_ttsex) as t2
join (select age, ttage from age_ttage) as t3
on t1.sex = t2.sex and t1.age = t3.age;

-- ----------------------------------------------------------------------------
-- 数据表 mid_ttlabel_p_ 与 mid_address_p 合并成 mid_ttlabel_p 
-- ----------------------------------------------------------------------------
insert into table mid_ttlabel_p (mid, ttsex, ttage, province, city, l_province, l_city, l_area)
select distinct t1.mid, t1.ttsex, t1.ttage, t2.province, t2.city, t2.l_province, t2.l_city, t2.l_area
from (select distinct mid, ttsex, ttage from mid_ttlabel_p_) as t1
join (select distinct id, province, city, l_province, l_city, l_area from mid_address_p) as t2
on t1.mid = t2.id;

-- ----------------------------------------------------------------------------
-- 删除中间数据表 mid_ttlabel_p_ 和 mid_address_p
-- ----------------------------------------------------------------------------
drop table if exists mid_ttlabel_p_;
drop table if exists mid_address_p;

-- ----------------------------------------------------------------------------
-- 新建目标群体 mid_appid_p 数据表(ORC格式)
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

-- ----------------------------------------------------------------------------
-- 新建目标群体 mid_app_label_p 数据表(ORC格式)
-- ----------------------------------------------------------------------------
drop table if exists mid_app_label_p;
create table if not exists mid_app_label_p
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
    interest1_2 string)
partitioned by (
m int comment 'month',
bsn string)
row format delimited
fields terminated by ','
stored as orc;
-- 插入数据
insert into table mid_app_label_p partition(m=201909, bsn='acadsoc')
(m_id, appid, appname, daycount, pv, freq, article, behavior, interest1, interest1_2)
select t2.mid, t2.appid, t1.appname, t2.daycount, t2.pv, t2.freq, t1.article, t1.behavior, t1.interest1, t1.interest1_2
from (select app_id, appname, article, behavior, interest1, interest1_2 from app_label) as t1
join (select mid, appid, daycount, pv, freq from mid_appid_p) as t2
on t1.app_id = t2.appid;

-- ----------------------------------------------------------------------------
-- 删除中间数据表 mid_appid_p
-- ----------------------------------------------------------------------------
drop table if exists mid_appid_p;

-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------




















