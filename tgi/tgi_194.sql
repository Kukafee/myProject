-- 
-- /Users/edz/pang/code/sublime/tgi/tgi_194.sql
-- select count(distinct(mid)) from pangxk.mid_ttlabel;
-- 总体样本个数 440466597
-- select count(distinct(mid)) from pangxk.mid_ttlabel_p;
-- 目标群体样本个数 349
-- ----------------------------------------------------------------------------
-- 分析总体表 mid_ttlabel 与 目标群体表 mid_ttlabel_p
-- ----------------------------------------------------------------------------
use pangxk;
-- ----------------------------------------------------------------------------
-- 创建并生成用户年龄TGI表 tgi_age
-- ----------------------------------------------------------------------------
drop table if exists tgi_age;   -- ---1
create table if not exists tgi_age(   -- ---1
    age string,   -- ---1
    tgi_value double)
row format delimited
fields terminated by ',';
-- 给用户年龄TGI表插入数据
insert into
    tgi_age   -- ---1
select
    rate_total.feature,
    -- rate_target.feature_target / rate_total.feature_total tgi_value
    cast(rate_target.feature_target / rate_total.feature_total as decimal(6,3)) as tgi_value  -- 保留6位有效数字且3位小数
from -- -
    (select
    table_target.feature as feature,
    table_target.feature_count / 349 as feature_target   -- 目标群体样本个数 349
    from
    (select ttage feature, count(distinct(mid)) feature_count   -- ---1
    from pangxk.mid_ttlabel_p
    group by ttage) as table_target) as rate_target   -- ---1
join -- -
    (select
    table_total.feature as feature,
    table_total.feature_count / 440466597 as feature_total   -- 总体样本个数 2560647
    from
    (select ttage feature, count(distinct(mid)) feature_count   -- ---1
    from pangxk.mid_ttlabel2
    group by ttage) as table_total)as rate_total  -- ---1 
on -- -
    rate_target.feature=rate_total.feature
order by tgi_value desc;   -- ****降序

-- ----------------------------------------------------------------------------
-- 创建用户性别TGI表 tgi_sex
-- ----------------------------------------------------------------------------
drop table if exists tgi_sex;
create table if not exists tgi_sex(
    sex string,
    tgi_value double)
row format delimited
fields terminated by ',';
-- 给用户性别TGI表插入数据
insert into
    tgi_sex
select
    rate_total.feature,
    -- rate_target.feature_target / rate_total.feature_total tgi_value   -- ****
    cast(rate_target.feature_target / rate_total.feature_total as decimal(6,3)) as tgi_value
    -- select cast(ROUND(9.1234999999,5) as money)
    -- select cast(round(9.1234999999,5)) 
from -- -
    (select
    table_target.feature as feature,
    table_target.feature_count / 349 as feature_target   -- 目标群体样本个数2560647
    from
    (select ttsex feature, count(distinct(mid)) feature_count   -- ---1
    from pangxk.mid_ttlabel_p
    group by ttsex) as table_target) as rate_target   -- ---1
join -- -
    (select
    table_total.feature as feature,
    table_total.feature_count / 2560647 as feature_total   -- 总体样本个数2560647
    from
    (select ttsex feature, count(distinct(mid)) feature_count   -- ---1
    from pangxk.mid_ttlabel2
    group by ttsex) as table_total)as rate_total  -- ---1 
on -- -
    rate_target.feature=rate_total.feature
order by tgi_value desc;   -- ****降序

-- ----------------------------------------------------------------------------
-- 创建用户归属地省TGI表 tgi_province   33
-- ----------------------------------------------------------------------------
drop table if exists tgi_province;   -- ---1
create table if not exists tgi_province(   -- ---1
    province string,   -- ---1
    tgi_value double)
row format delimited
fields terminated by ',';
-- 给用户归属地省TGI表插入数据
insert into
    tgi_province   -- ---1
select
    rate_total.feature,
    -- rate_target.feature_target / rate_total.feature_total tgi_value
    cast(rate_target.feature_target / rate_total.feature_total as decimal(6,3)) as tgi_value
from -- -
    (select
    table_target.feature as feature,
    table_target.feature_count / 349 as feature_target   -- 目标群体样本个数2560647
    from
    (select province feature, count(distinct(mid)) feature_count   -- ---1
    from pangxk.mid_ttlabel_p
    group by province) as table_target) as rate_target   -- ---1
join -- -
    (select
    table_total.feature as feature,
    table_total.feature_count / 2560647 as feature_total   -- 总体样本个数2560647
    from
    (select province feature, count(distinct(mid)) feature_count   -- ---1
    from pangxk.mid_ttlabel2
    group by province) as table_total)as rate_total  -- ---1 
on -- -
    rate_target.feature=rate_total.feature
order by tgi_value desc;

-- ----------------------------------------------------------------------------
-- 创建用户归属地市TGI表 tgi_city   361
-- ----------------------------------------------------------------------------
drop table if exists tgi_city;   -- ---1
create table if not exists tgi_city(   -- ---1
    city string,   -- ---1
    tgi_value double)
row format delimited
fields terminated by ',';
-- 给用户归属地市TGI表插入数据
insert into
    tgi_city   -- ---1
select
    rate_total.feature,
    -- rate_target.feature_target / rate_total.feature_total tgi_value
    cast(rate_target.feature_target / rate_total.feature_total as decimal(6,3)) as tgi_value
from -- -
    (select
    table_target.feature as feature,
    table_target.feature_count / 349 as feature_target   -- 目标群体样本个数2560647
    from
    (select city feature, count(distinct(mid)) feature_count   -- ---1
    from pangxk.mid_ttlabel_p
    group by city) as table_target) as rate_target   -- ---1
join -- -
    (select
    table_total.feature as feature,
    table_total.feature_count / 2560647 as feature_total   -- 总体样本个数2560647
    from
    (select city feature, count(distinct(mid)) feature_count   -- ---1
    from pangxk.mid_ttlabel2
    group by city) as table_total)as rate_total  -- ---1 
on -- -
    rate_target.feature=rate_total.feature
order by tgi_value desc;

-- ----------------------------------------------------------------------------
-- 创建用户常住地省TGI表 tgi_l_province 32
-- ---------------------------------------------------------------------------- 
drop table if exists tgi_l_province;   -- ---1
create table if not exists tgi_l_province(   -- ---1
    l_province string,   -- ---1
    tgi_value double)
row format delimited
fields terminated by ',';
-- 给用户常住地省TGI表插入数据
insert into
    tgi_l_province   -- ---1
select
    rate_total.feature,
    -- rate_target.feature_target / rate_total.feature_total tgi_value
    cast(rate_target.feature_target / rate_total.feature_total as decimal(6,3)) as tgi_value
from -- -
    (select
    table_target.feature as feature,
    table_target.feature_count / 349 as feature_target   -- 目标群体样本个数2560647
    from
    (select l_province feature, count(distinct(mid)) feature_count   -- ---1
    from pangxk.mid_ttlabel_p
    group by l_province) as table_target) as rate_target   -- ---1
join -- -
    (select
    table_total.feature as feature,
    table_total.feature_count / 2560647 as feature_total   -- 总体样本个数2560647
    from
    (select l_province feature, count(distinct(mid)) feature_count   -- ---1
    from pangxk.mid_ttlabel2
    group by l_province) as table_total)as rate_total  -- ---1 
on -- -
    rate_target.feature=rate_total.feature
order by tgi_value desc;

-- ----------------------------------------------------------------------------
-- 创建用户常住地市TGI表  tgi_l_city  384
-- ---------------------------------------------------------------------------- 
drop table if exists tgi_l_city;   -- ---1
create table if not exists tgi_l_city(   -- ---1
    l_city string,   -- ---1
    tgi_value double)
row format delimited
fields terminated by ',';
-- 给用户常住地市TGI表插入数据
insert into
    tgi_l_city   -- ---1
select
    rate_total.feature,
    -- rate_target.feature_target / rate_total.feature_total tgi_value
    cast(rate_target.feature_target / rate_total.feature_total as decimal(6,3)) as tgi_value
from -- -
    (select
    table_target.feature as feature,
    table_target.feature_count / 349 as feature_target   -- 目标群体样本个数2560647
    from
    (select l_city feature, count(distinct(mid)) feature_count   -- ---1
    from pangxk.mid_ttlabel_p
    group by l_city) as table_target) as rate_target   -- ---1
join -- -
    (select
    table_total.feature as feature,
    table_total.feature_count / 2560647 as feature_total   -- 总体样本个数2560647
    from
    (select l_city feature, count(distinct(mid)) feature_count   -- ---1
    from pangxk.mid_ttlabel2
    group by l_city) as table_total)as rate_total  -- ---1 
on -- -
    rate_target.feature=rate_total.feature
order by tgi_value desc;

-- ----------------------------------------------------------------------------
-- 创建用户常住地区TGI表  tgi_l_area  2805
-- ---------------------------------------------------------------------------- 


-- ---------------------------------------------------------------------------- 
-- ---------------------------------------------------------------------------- 




-- ----------------------------------------------------------------------------
-- 分析 mid_app_label 表
-- ----------------------------------------------------------------------------
-- 统计：
-- mid_app_label
-- m_id         string       
-- appid        string    2402   
-- appname      string    2124   
-- daycount     int       30       
-- pv           int       66568
-- freq         int       3
-- * article                 string    128    TT种类 45个      
-- * behavior                string    76     TT种类 按分类：移动应用13   移动游戏9    
-- * interest1               string    68     TT种类 18个    
-- * interest1_2             string    152    TT种类 128个     
-- ----------------------------------------------------------------------------

-- select count(distinct(m_id)) from mid_app_labelp;    1177914 ?
-- select count(m_id) from mid_app_labelp;              34427211?
-- select count(distinct(appid)) from mid_app_labelp;   2402 ?
-- select count(appid) from mid_app_labelp;             34427211?

-- select count(m_id) from mid_app_labelp where mid_app_labelp.article










