
-- ----------------------------------------------------------
-- '/Users/edz/pang/code/sublime/tgi/tgi.sql'
-- ----------------------------------------------------------


-- wangl.positive_sample
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



-- # Partition Information
-- # col_name              data_type               comment

-- op                      string
-- m                       int   
-- ----------------------------------------------------------                   
-- 

--- ------mid_ttlabel;
-- mid                     string
-- ttsex                   string
-- ttage                   string
-- province                string
-- city                    string
-- l_province              string
-- l_city                  string
-- l_area                  string 


-- 总体样本
select count(mid) as all    -- 
from pangxk_.mid_ttlabel    

select count(mid) as     -- 
from pangxk_.mid_ttlabel
group by ttsex

select count(mid)    -- 
from pangxk_.mid_ttlabel
group by ttage

select count(mid)    -- 
from pangxk_.mid_ttlabel
group by province    

select count(mid)    -- 
from pangxk_.mid_ttlabel
group by city 

select count(mid)    -- 
from pangxk_.mid_ttlabel
group by l_province

select count(mid)    -- 
from pangxk_.mid_ttlabel 
group by l_city

select count(mid)
from pangxk_.mid_ttlabel
group by l_area

-- 目标样本 （直接join）




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


-- insert overwrite table mid_app_labelp partition(m=201909)
-- select
-- m_id, appid, appname, daycount, pv, freq, article, behavior, interest1, interest1_2
-- from mid_app_label;













































