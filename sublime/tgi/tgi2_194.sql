-- 
-- /Users/edz/pang/code/sublime/tgi/tgi_194.sql
-- select count(distinct(mid)) from pangxk.mid_ttlabel;
-- 总体样本个数 440466597
-- select count(distinct(mid)) from pangxk.mid_ttlabel_p;
-- 目标群体样本个数 349
-- ----------------------------------------------------------------------------
-- 分析总体表 mid_ttlabel 与 目标群体表 mid_ttlabel_p
-- ----------------------------------------------------------------------------
-- 选中数据库
use pangxk;

-- ----------------------------------------------------------------------------
-- 统计数据
-- ----------------------------------------------------------------------------
-- 静态特征总体 select count(distinct mid) from mid_ttlabel2     440 466 597  
-- 静态特征目标 select count(distinct mid) from mid_ttlabel_p    349   
-- 
-- app特征
-- -- tgi_article 特征      
--        总体 select count(mid) from mid_article;    4 389 569 631   
--        总体 select count(distinct mid) from mid_article;    
--        目标 select count(mid) from mid_article_p;
--        目标 select count(distinct mid) from mid_article_p;        
--    tgi_behavior 特征
--        总体 select count(mid) from mid_behavior;
--        总体 select count(distinct mid) from mid_behavior;
--        目标 select count(mid) from mid_behavior_p;     
--        目标 select count(distinct mid) from mid_behavior_p;            
--    tgi_interest1 特征
--        总体 select count(mid) from mid_interest1;
--        总体 select count(distinct mid) from mid_interest1;
--        目标 select count(mid) from mid_interest1_p;
--        目标 select count(distinct mid) from mid_interest1_p;
--    tgi_interest1_2 特征
--        总体 select count(mid) from mid_interest1_2;
--        总体 select count(distinct mid) from mid_interest1_2;
--        目标 select count(mid) from mid_interest1_2;
--        目标 select count(distinct mid) from mid_interest1_2;
-- 
-- 
-- -- ----------------------------------------------------------------------------
-- -- 创建并生成用户年龄TGI表 tgi_age
-- -- ----------------------------------------------------------------------------
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
    cast(rate_target.feature_target / rate_total.feature_total as decimal(12,6)) as tgi_value  -- 保留6位有效数字且3位小数
from -- -
    (select
    table_target.feature as feature,
    table_target.feature_count / 349 as feature_target   -- 目标群体样本个数
    from
    (select ttage feature, count(distinct(mid)) feature_count   -- ---1
    from pangxk.mid_ttlabel_p
    group by ttage) as table_target) as rate_target   -- ---1
join -- -
    (select
    table_total.feature as feature,
    table_total.feature_count / 440466597 as feature_total   -- 总体样本个数 
    from
    (select ttage feature, count(distinct(mid)) feature_count   -- ---1
    from pangxk.mid_ttlabel2
    group by ttage) as table_total)as rate_total  -- ---1 
on -- -
    rate_target.feature=rate_total.feature
order by tgi_value desc;   -- ****降序

-- -- ----------------------------------------------------------------------------
-- -- 创建用户性别TGI表 tgi_sex
-- -- ----------------------------------------------------------------------------
-- drop table if exists tgi_sex;
-- create table if not exists tgi_sex(
--     sex string,
--     tgi_value double)
-- row format delimited
-- fields terminated by ',';
-- -- 给用户性别TGI表插入数据
-- insert into
--     tgi_sex
-- select
--     rate_total.feature,
--     -- rate_target.feature_target / rate_total.feature_total tgi_value   -- ****
--     cast(rate_target.feature_target / rate_total.feature_total as decimal(12,6)) as tgi_value
--     -- select cast(ROUND(9.1234999999,5) as money)
--     -- select cast(round(9.1234999999,5)) 
-- from -- -
--     (select
--     table_target.feature as feature,
--     table_target.feature_count / 349 as feature_target   -- 目标群体样本个数
--     from
--     (select ttsex feature, count(distinct(mid)) feature_count   -- ---1
--     from pangxk.mid_ttlabel_p
--     group by ttsex) as table_target) as rate_target   -- ---1
-- join -- -
--     (select
--     table_total.feature as feature,
--     table_total.feature_count / 440466597 as feature_total   -- 总体样本个数
--     from
--     (select ttsex feature, count(distinct(mid)) feature_count   -- ---1
--     from pangxk.mid_ttlabel2
--     group by ttsex) as table_total)as rate_total  -- ---1 
-- on -- -
--     rate_target.feature=rate_total.feature
-- order by tgi_value desc;   -- ****降序

-- -- ----------------------------------------------------------------------------
-- -- 创建用户归属地省TGI表 tgi_province   33
-- -- ----------------------------------------------------------------------------
-- drop table if exists tgi_province;   -- ---1
-- create table if not exists tgi_province(   -- ---1
--     province string,   -- ---1
--     tgi_value double)
-- row format delimited
-- fields terminated by ',';
-- -- 给用户归属地省TGI表插入数据
-- insert into
--     tgi_province   -- ---1
-- select
--     rate_total.feature,
--     -- rate_target.feature_target / rate_total.feature_total tgi_value
--     cast(rate_target.feature_target / rate_total.feature_total as decimal(12,6)) as tgi_value
-- from -- -
--     (select
--     table_target.feature as feature,
--     table_target.feature_count / 349 as feature_target   -- 目标群体样本个数
--     from
--     (select province feature, count(distinct(mid)) feature_count   -- ---1
--     from pangxk.mid_ttlabel_p
--     group by province) as table_target) as rate_target   -- ---1
-- join -- -
--     (select
--     table_total.feature as feature,
--     table_total.feature_count / 440466597 as feature_total   -- 总体样本个数
--     from
--     (select province feature, count(distinct(mid)) feature_count   -- ---1
--     from pangxk.mid_ttlabel2
--     group by province) as table_total)as rate_total  -- ---1 
-- on -- -
--     rate_target.feature=rate_total.feature
-- order by tgi_value desc;

-- -- ----------------------------------------------------------------------------
-- -- 创建用户归属地市TGI表 tgi_city   361
-- -- ----------------------------------------------------------------------------
-- drop table if exists tgi_city;   -- ---1
-- create table if not exists tgi_city(   -- ---1
--     city string,   -- ---1
--     tgi_value double)
-- row format delimited
-- fields terminated by ',';
-- -- 给用户归属地市TGI表插入数据
-- insert into
--     tgi_city   -- ---1
-- select
--     rate_total.feature,
--     -- rate_target.feature_target / rate_total.feature_total tgi_value
--     cast(rate_target.feature_target / rate_total.feature_total as decimal(12,6)) as tgi_value
-- from -- -
--     (select
--     table_target.feature as feature,
--     table_target.feature_count / 349 as feature_target   -- 目标群体样本个数
--     from
--     (select city feature, count(distinct(mid)) feature_count   -- ---1
--     from pangxk.mid_ttlabel_p
--     group by city) as table_target) as rate_target   -- ---1
-- join -- -
--     (select
--     table_total.feature as feature,
--     table_total.feature_count / 440466597 as feature_total   -- 总体样本个数
--     from
--     (select city feature, count(distinct(mid)) feature_count   -- ---1
--     from pangxk.mid_ttlabel2
--     group by city) as table_total)as rate_total  -- ---1 
-- on -- -
--     rate_target.feature=rate_total.feature
-- order by tgi_value desc;

-- -- ----------------------------------------------------------------------------
-- -- 创建用户常住地省TGI表 tgi_l_province 32
-- -- ---------------------------------------------------------------------------- 
-- drop table if exists tgi_l_province;   -- ---1
-- create table if not exists tgi_l_province(   -- ---1
--     l_province string,   -- ---1
--     tgi_value double)
-- row format delimited
-- fields terminated by ',';
-- -- 给用户常住地省TGI表插入数据
-- insert into
--     tgi_l_province   -- ---1
-- select
--     rate_total.feature,
--     -- rate_target.feature_target / rate_total.feature_total tgi_value
--     cast(rate_target.feature_target / rate_total.feature_total as decimal(12,6)) as tgi_value
-- from -- -
--     (select
--     table_target.feature as feature,
--     table_target.feature_count / 349 as feature_target   -- 目标群体样本个数
--     from
--     (select l_province feature, count(distinct(mid)) feature_count   -- ---1
--     from pangxk.mid_ttlabel_p
--     group by l_province) as table_target) as rate_target   -- ---1
-- join -- -
--     (select
--     table_total.feature as feature,
--     table_total.feature_count / 440466597 as feature_total   -- 总体样本个数
--     from
--     (select l_province feature, count(distinct(mid)) feature_count   -- ---1
--     from pangxk.mid_ttlabel2
--     group by l_province) as table_total)as rate_total  -- ---1 
-- on -- -
--     rate_target.feature=rate_total.feature
-- order by tgi_value desc;

-- -- ----------------------------------------------------------------------------
-- -- 创建用户常住地市TGI表  tgi_l_city  384
-- -- ---------------------------------------------------------------------------- 
-- drop table if exists tgi_l_city;   -- ---1
-- create table if not exists tgi_l_city(   -- ---1
--     l_city string,   -- ---1
--     tgi_value double)
-- row format delimited
-- fields terminated by ',';
-- -- 给用户常住地市TGI表插入数据
-- insert into
--     tgi_l_city   -- ---1
-- select
--     rate_total.feature,
--     -- rate_target.feature_target / rate_total.feature_total tgi_value
--     cast(rate_target.feature_target / rate_total.feature_total as decimal(12,6)) as tgi_value
-- from -- -
--     (select
--     table_target.feature as feature,
--     table_target.feature_count / 349 as feature_target   -- 目标群体样本个数
--     from
--     (select l_city feature, count(distinct(mid)) feature_count   -- ---1
--     from pangxk.mid_ttlabel_p
--     group by l_city) as table_target) as rate_target   -- ---1
-- join -- -
--     (select
--     table_total.feature as feature,
--     table_total.feature_count / 440466597 as feature_total   -- 总体样本个数
--     from
--     (select l_city feature, count(distinct(mid)) feature_count   -- ---1
--     from pangxk.mid_ttlabel2
--     group by l_city) as table_total)as rate_total  -- ---1 
-- on -- -
--     rate_target.feature=rate_total.feature
-- order by tgi_value desc;

-- -- 大兴安岭        55.843098
-- -- 临夏回族自治州  34.235235
-- -- 克拉玛依        31.911451
-- -- 天津    22.851723
-- -- 黑河    19.23837
-- -- 阿拉善盟        16.255563
-- -- 铜陵    15.581836
-- -- 乌鲁木齐        15.171625
-- -- 巴音郭楞        14.999251
-- -- 秦皇岛  14.516438   
-- -- ----------------------------------------------------------------------------
-- -- 创建用户常住地区TGI表  tgi_l_area  2805
-- -- ---------------------------------------------------------------------------- 


-- -- ---------------------------------------------------------------------------- 
-- -- ---------------------------------------------------------------------------- 




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


-- 建表
	-- tgi_article                 
	-- tgi_behavior                
	-- tgi_interest1
	-- tgi_interest1_2     
-- 数量统计
    -- 样本群体总量
    -- 目标群体数量
    
     
-- 初步分析，应该使用去重的数值进行计算
-- app特征
-- -- tgi_article 特征      
--        总体 select count(mid) from mid_article;    4 389 569 631      4389569631 
--        总体 select count(distinct mid) from mid_article;  卡死！ 
--            select count(*) from (select distinct mid from mid_article);-- 优化去重聚合
-- 
--        目标 select count(mid) from mid_article_p;  10369
--        目标 select count(distinct mid) from mid_article_p;   342
-- ---------------------------------------------------------------------------------------
--    tgi_behavior 特征
--        总体 select count(mid) from mid_behavior;  2585354237   2585354237
--        总体 select count(distinct mid) from mid_behavior;    
--            select count(mid) from (select distinct(mid)from mid_behavior);-- 优化去重聚合         

--        目标 select count(mid) from mid_behavior_p;   5228      
--        目标 select count(distinct mid) from mid_behavior_p;  342    
-- ---------------------------------------------------------------------------------------
--    tgi_interest1 特征
--        总体 select count(mid) from mid_interest1;1931106137
--        总体 select count(distinct mid) from mid_interest1;
--            select count(*) from (select distinct mid from mid_interest1);-- 优化去重聚合

--        目标 select count(mid) from mid_interest1_p;4558
--        目标 select count(distinct mid) from mid_interest1_p;342
-- ---------------------------------------------------------------------------------------
--    tgi_interest1_2 特征
--        总体 select count(mid) from mid_interest1_2;
--        总体 select count(distinct mid) from mid_interest1_2;
--            select count(*) from (select distinct mid from mid_interest1_2);-- 优化去重聚合

--        目标 select count(mid) from mid_interest1_2_p;19588
--        目标 select count(distinct mid) from mid_interest1_2_p;342
-- 
-- 
-- -- ----------------------------------------------------------------------------   
-- ----------------------------------------------------------------------------

-- ----------------------------------------------------------------------------
-- 创建 app 的 文章分类标签 TGI表  tgi_article  45个
-- ---------------------------------------------------------------------------- 
drop table if exists tgi_article;   -- --------------------------------表名
create table if not exists tgi_article(   -- --------------------------表名
    articleClassification string,   -- --------------------------------标签名
    tgi_value double)
row format delimited
fields terminated by ',';
-- 插入数据
insert into
    tgi_article   -- ---------------------------------------------------表名
select
    rate_total.feature,
    -- rate_target.feature_target / rate_total.feature_total tgi_value
    cast(rate_target.feature_target / rate_total.feature_total as decimal(12,6)) as tgi_value
from -- -
    (select
    table_target.feature as feature,
    table_target.feature_count / 10369 as feature_target
    from
    (select article feature, count(mid) feature_count   -- -----字段名
    from pangxk.mid_article_p    -- -------------------------------------选择数据库和表
    group by article) as table_target) as rate_target   -- ---------------字段名
join -- -
    (select
    table_total.feature as feature,
    table_total.feature_count / 4171909188 as feature_total
    from
    (select article feature, count(mid) feature_count   -- ------字段名
    from pangxk.mid_article    -- ---------------------------------------选择数据库和表
    group by article) as table_total)as rate_total   -- -------------------字段名
on -- -
    rate_target.feature=rate_total.feature
order by tgi_value desc;

-- ----------------------------------------------------------------------------
-- 创建 app行为 标签 TGI表  tgi_behavior  1+1+19+9
-- ---------------------------------------------------------------------------- 
drop table if exists tgi_behavior;   -- --------------------------------表名
create table if not exists tgi_behavior(   -- --------------------------表名
    APPbehavior string,   -- --------------------------------标签名
    tgi_value double)
row format delimited
fields terminated by ',';
-- 插入数据
insert into
    tgi_behavior   -- ---------------------------------------------------表名
select
    rate_total.feature,
    -- rate_target.feature_target / rate_total.feature_total tgi_value
    cast(rate_target.feature_target / rate_total.feature_total as decimal(12,6)) as tgi_value
from -- -
    (select
    table_target.feature as feature,
    table_target.feature_count / 5228 as feature_target
    from
    (select behavior feature, count(mid) feature_count   -- -----字段名
    from pangxk.mid_behavior_p    -- -------------------------------------选择数据库和表
    group by behavior) as table_target) as rate_target   -- ---------------字段名
join -- -
    (select
    table_total.feature as feature,
    table_total.feature_count / 2724552423 as feature_total
    from
    (select behavior feature, count(mid) feature_count   -- ------字段名
    from pangxk.mid_behavior    -- --------------------------- ------------选择数据库和表
    group by behavior) as table_total)as rate_total   -- -------------------字段名
on -- -
    rate_target.feature=rate_total.feature
order by tgi_value desc;

-- ----------------------------------------------------------------------------
-- 创建 兴趣定向(一级) 标签 TGI表  tgi_interest1  18
-- ---------------------------------------------------------------------------- 
drop table if exists tgi_interest1;   -- --------------------------------表名
create table if not exists tgi_interest1(   -- --------------------------表名
    interest1 string,   -- --------------------------------标签名
    tgi_value double)
row format delimited
fields terminated by ',';
-- 插入数据
insert into
    tgi_interest1   -- ---------------------------------------------------表名
select
    rate_total.feature,
    cast(rate_target.feature_target / rate_total.feature_total as decimal(12,6)) as tgi_value
from
    (select
    table_target.feature as feature,
    table_target.feature_count / 4558 as feature_target   --
    from
    (select interest1 feature, count(mid) feature_count   -- -----字段名
    from pangxk.mid_interest1_p    -- ----------------------------
    group by interest1) as table_target) as rate_target   -- ---------------字段名
join
    (select
    table_total.feature as feature,
    table_total.feature_count / 1931106137 as feature_total   -- 
    from
    (select interest1 feature, count(mid) feature_count   -- ------字段名
    from pangxk.mid_interest1    -- ------------------------------
    group by interest1) as table_total)as rate_total   -- -------------------字段名
on -- -
    rate_target.feature=rate_total.feature
order by tgi_value desc;

-- ----------------------------------------------------------------------------
-- 创建 兴趣定向(一级&二级) 标签 TGI表  tgi_interest1_2  18
-- ---------------------------------------------------------------------------- 
drop table if exists tgi_interest1_2;   -- --------------------------------表名
create table if not exists tgi_interest1_2(   -- --------------------------表名
    interest1_2 string,   -- ----------------------------------------------标签名
    tgi_value double)
row format delimited
fields terminated by ',';
-- 插入数据
insert into
    tgi_interest1_2   -- ---------------------------------------------------表名
select
    rate_total.feature,
    -- rate_target.feature_target / rate_total.feature_total tgi_value
    cast(rate_target.feature_target / rate_total.feature_total as decimal(12,6)) as tgi_value
from -- -
    (select
    table_target.feature as feature,
    table_target.feature_count / 19588 as feature_target   -- 
    from
    (select interest1_2 feature, count(mid) feature_count   -- -----字段名
    from pangxk.mid_interest1_2_p    -- -------------------------------------选择数据库和表
    group by interest1_2) as table_target) as rate_target   -- ---------------字段名
join -- -
    (select
    table_total.feature as feature,
    table_total.feature_count / 5527776442 as feature_total   -- 
    from
    (select interest1_2 feature, count(mid) feature_count   -- ------字段名
    from pangxk.mid_interest1_2    -- ---------------------------------------选择数据库和表
    group by interest1_2) as table_total)as rate_total   -- -------------------字段名
on -- -
    rate_target.feature=rate_total.feature
order by tgi_value desc;


-- ----------------------------------------------------------------------------
-- 删除中间数据表 pangxk 数据库中 
           -- mid_article   mid_article_p   mid_behavior   mid_behavior_p   
           -- mid_interest1   mid_interest1_p   mid_interest1_2   mid_interest1_2_p
-- ----------------------------------------------------------------------------
drop table if exists mid_article;
drop table if exists mid_article_p;
drop table if exists mid_behavior;
drop table if exists mid_behavior_p;
drop table if exists mid_interest1;
drop table if exists mid_interest1_p;
drop table if exists mid_interest1_2;
drop table if exists mid_interest1_2_p;


-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
-- 重复使用的数据表
--      正样本数据表 positive_sample
--      总体样本数据表 databank.freq_info
--      年龄标签映射表 age_ttage
--      性别标签映射表 sex_ttsex
--      
-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
-- 生成 性别TGI表
INSERT OVERWRITE local DIRECTORY '/root/pangxk/tgi/tgivalues/tgi_sex.csv'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
select * from pangxk.tgi_sex;
-- 生成 年龄TGI表
insert overwrite local directory '/root/pangxk/tgi/tgivalues/tgi_age.csv'
row format delimited fields terminated by ','
select * from pangxk.tgi_age;
-- 生成 归属地省TGI表
insert overwrite local directory '/root/pangxk/tgi/tgivalues/tgi_province.csv'
row format delimited fields terminated by ','
select * from pangxk.tgi_province;
-- 生成 归属地市TGI表
insert overwrite local directory '/root/pangxk/tgi/tgivalues/tgi_city.csv'
row format delimited fields terminated by ','
select * from pangxk.tgi_city;
-- 生成 常住地省TGI表
insert overwrite local directory '/root/pangxk/tgi/tgivalues/tgi_l_province.csv'
row format delimited fields terminated by ','
select * from pangxk.tgi_l_province;
-- 生成 常住地市TGI表
insert overwrite local directory '/root/pangxk/tgi/tgivalues/tgi_l_city.csv'
row format delimited fields terminated by ','
select * from pangxk.tgi_l_city;

-- 生成 文章分类特征TGI表
insert overwrite local directory '/root/pangxk/tgi/tgivalues/tgi_article.csv'
row format delimited fields terminated by ','
select * from pangxk.tgi_article;
-- 生成 app行为特征TGI表
insert overwrite local directory '/root/pangxk/tgi/tgivalues/tgi_behavior.csv'
row format delimited fields terminated by ','
select * from pangxk.tgi_behavior;
-- 生成 兴趣定向(一级)特征TGI表
insert overwrite local directory '/root/pangxk/tgi/tgivalues/tgi_interest1.csv'
row format delimited fields terminated by ','
select * from pangxk.tgi_interest1;
-- 生成 兴趣定向(一级&二级)特征TGI表
insert overwrite local directory '/root/pangxk/tgi/tgivalues/tgi_interest1_2.csv'
row format delimited fields terminated by ','
select * from pangxk.tgi_interest1_2;









