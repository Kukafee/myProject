

use infoflow;    -- 使用数据库
select current_database();    -- 显示当前使用的数据库

-- select count(distinct(mid)) from pangxk.mid_ttlabel;
-- 总体样本个数 440466597
-- select count(distinct(mid)) from pangxk.mid_ttlabel_p;
-- 目标群体样本个数 349

-- -----------------------------------------------------------------------------
-- 性别特征 4
--     '男'   '女'   '未知'   ''
-- -----------------------------------------------------------------------------
-- 年龄特征 7
--     '18-23'   '24-30'   '31-40'   '41-49'  '50+'   '未知'   ''
-- -----------------------------------------------------------------------------
-- 归属地 省
--     
-- 
-- -----------------------------------------------------------------------------
-- 归属地 市
-- 
-- 
-- -----------------------------------------------------------------------------
-- 常住地 省
-- 
-- 
-- -----------------------------------------------------------------------------
-- 常住地 市
-- 
-- 
-- -----------------------------------------------------------------------------
-- 常住地 区
-- -----------------------------------------------------------------------------
-- 文章分类 45
--     娱乐.   社会.   汽车.   国际.   历史_   体育.   健康.*   军事_   情感.   时尚.   育儿.*   财经.   搞笑.   数码.   探索.   旅游.   
--     星座.   故事.   科技.   美食.   文化.   家居.   电影.   宠物.   游戏.   瘦身.*   奇葩.   教育.   房产.   科学.   摄影.   养生.*   
--     美文_   收藏.   动漫.   漫画.   小窍门.   设计.   本地.   法制_   政务.   商业.   职场.   辟谣.   毕业生.
--         [缺失] 历史_  军事_  美文_   法制_    [多]Huggies"    原因: app名字中包含','  好奇,Huggies
-----------------------------------------------------------------------------
-- app行为 13+9
--     移动应用13
--         网上购物.
--         阅读学习.
--         新闻资讯.
--         影音图像.
--         旅游出行.
--         常用工具.
--         办公软件.
--         社交网络.
--         美化手机.
--         金融理财.
--         性能优化.
--         生活服务.
--         通信聊天.
--     移动游戏9.
--         休闲时间.
--         儿童益智.
--         网络游戏.
--         动作射击.
--         跑酷竞速.
--         体育格斗.
--         扑克棋牌.
--         经营策略.
--         角色扮演.
--                在app行为中包含"移动游戏" 或者 "移动应用" 说明包括该类目中所有项目
--                在app行为标签中，除了"移动应用"外，包含全部，
--                [多]  健康/养生/瘦身/育儿
-- -----------------------------------------------------------------------------
-- 兴趣定向(一级) 18
--     游戏
--     家装百货
--     金融理财
--     教育培训
--     旅游出行
--     服饰箱包
--     汽车
--     美容化妆
--     房产
--     餐饮美食
--     母婴儿童
--     科技数码
--     体育运动
--     生活服务
--     医疗健康
--     法律服务
--     文化娱乐
--     商务服务
-- -----------------------------------------------------------------------------
-- 兴趣定向(二级) 128
-- 
-- 
-- -----------------------------------------------------------------------------








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
    cast(rate_target.feature_target / rate_total.feature_total as decimal(12,6)) as tgi_value  -- 保留6位有效数字且3位小数
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
-- 删除中间数据表
-- ----------------------------------------------------------------------------

drop table if exists mid_article;
drop table if exists mid_behavior;
drop table if exists mid_interest1;
drop table if exists mid_interest1_2;
drop table if exists mid_article_p;
drop table if exists mid_behavior_p;
drop table if exists mid_interest1_p;
drop table if exists mid_interest1_2_p;

