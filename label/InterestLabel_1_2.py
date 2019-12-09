
# 兴趣定向 一级&二级标签映射函数
def Interest_1_2(keys):

    labelDic = {
        '教育培训_K12教育':['教育培训_学前教育', '教育培训_中小学教育', '教育培训_学历教育'],
        '教育培训_早教辅导': ['教育培训_学前教育'],
        '教育培训_高等教育': ['教育培训_大学教育', '教育培训_学历教育', '教育培训_公务员考试', '教育培训_出国留学'],
        '教育培训_语言学习': ['教育培训_语言培训', '教育培训_出国留学'],
        '教育培训_出国留学': ['教育培训_语言培训', '教育培训_出国留学'],
        '教育培训_公开课': ['教育培训_职业教育', '教育培训_语言培训', '教育培训_IT培训', '教育培训_公务员考试', '教育培训_技能培训', '教育培训_拓展训练', '文化娱乐_书籍杂志', '文化娱乐_艺术展览'],
        '教育培训_继续教育': ['教育培训_职业教育', '教育培训_技能培训', '教育培训_IT培训'],

        '新闻资讯_趣味新闻':['文化娱乐_书籍杂志'],
        '新闻资讯_垂直新闻':['文化娱乐_书籍杂志'],
        '新闻资讯_综合新闻':['文化娱乐_书籍杂志'],
        '新闻资讯_财经新闻':['金融理财_股票基金', '金融理财_保险', '金融理财_期货外汇', '金融理财_银行理财', '金融理财_互联网金融'],
        '新闻资讯_体育资讯':['体育运动_球类运动', '体育运动_休闲运动', '体育运动_跑步骑行', '体育运动_水上运动', '体育运动_极限运动', '体育运动_户外运动', '体育运动_运动装备'],
        '新闻资讯_报纸':['文化娱乐_书籍杂志'],
        '新闻资讯_男性时尚':['文化娱乐_桌游', '文化娱乐_酒吧KTV', '文化娱乐_电影电视'],
        '新闻资讯_科学文化':['文化娱乐_艺术展览', '文化娱乐_爱好收藏', '文化娱乐_书籍杂志'],
        '新闻资讯_影视娱乐':['文化娱乐_电影电视', '文化娱乐_艺术展览', '文化娱乐_演出', '文化娱乐_动漫周边'],
        '新闻资讯_商业资讯':['金融理财_股票基金', '金融理财_保险', '金融理财_期货外汇', '金融理财_银行理财', '金融理财_互联网金融', '金融理财_贵金属', '商务服务_招商加盟', '商务服务_营销广告'],
        '新闻资讯_杂志':['文化娱乐_书籍杂志'],
        '新闻资讯_家电资讯':['家装百货_家用电器', '家装百货_家电维修'],
        '新闻资讯_法律资讯':['法律服务_司法鉴定', '法律服务_律师服务', '法律服务_公证'],
        '新闻资讯_创业资讯':['商务服务_求职招聘', '商务服务_营销广告', '商务服务_招商加盟', '商务服务_展会服务', '商务服务_物流配送'],
        '新闻资讯_互联网资讯':['金融理财_互联网金融', '科技数码_手机及配件', '科技数码_电脑周边'],

        '通讯社交_社区论坛':['文化娱乐_电影电视', '文化娱乐_演出'],
        '通讯社交_家校沟通':['教育培训_学前教育', '教育培训_中小学教育'],
        '通讯社交_知识百科': ['家装百货_家电维修', '文化娱乐_爱好收藏', '文化娱乐_书籍杂志'],
        '通讯社交_即时通讯': ['文化娱乐_酒吧KTV', '文化娱乐_爱好收藏'],
        '通讯社交_匿名社交': ['文化娱乐_酒吧KTV', '文化娱乐_爱好收藏'],
        '通讯社交_健身社区': ['医疗健康_保健品', '体育运动_休闲运动', '体育运动_跑步骑行', '体育运动_户外运动', '体育运动_运动装备'],
        '通讯社交_职场社交': ['商务服务_求职招聘', '科技数码_办公设备', '教育培训_职业教育'],
        '通讯社交_LGBT社交': ['服饰箱包_时尚女装', '服饰箱包_女鞋', '服饰箱包_内衣', '服饰箱包_珠宝配饰', '服饰箱包_箱包皮具', '美容化妆_减肥瘦身', '美容化妆_美容整形', '美容化妆_美发护发', '美容化妆_化妆护肤', '生活服务_美容美发'],
        '通讯社交_婚恋社交': ['医疗健康_成人用品', '生活服务_家政服务'],
        '通讯社交_微博博客': ['文化娱乐_动漫周边', '文化娱乐_电影电视', '文化娱乐_艺术展览', '文化娱乐_演出'],

        '图片摄影_图片编辑':['科技数码_数码摄像'],
        '图片摄影_影像剪辑':['科技数码_数码摄像', '文化娱乐_电影电视', '生活服务_摄影照相'],
        '图片摄影_相册图库':['科技数码_数码摄像', '生活服务_摄影照相'],
        '图片摄影_摄影社区':['科技数码_数码摄像', '生活服务_摄影照相'],
        '图片摄影_照相机':['科技数码_数码摄像', '生活服务_摄影照相'],

        '实用工具_生活工具':['家装百货_家电维修', '家装百货_日用百货', '生活服务_家政服务'],
        '实用工具_办公工具':['商务服务_办公文教', '科技数码_办公设备', '房产_写字楼'],
        '实用工具_壁纸主题':['科技数码_手机及配件', '生活服务_摄影照相'],
        '实用工具_分类信息':['科技数码_手机及配件'],
        '实用工具_词典翻译':['商务服务_办公文教', '教育培训_语言培训', '教育培训_出国留学', '教育培训_大学教育'],
        '实用工具_应用商店':['科技数码_手机及配件', '科技数码_电脑周边'],
        '实用工具_手机工具':['科技数码_手机及配件'],
        '实用工具_地图导航':['旅游出行_本地周边游', '旅游出行_国内游', '旅游出行_港澳台游', '旅游出行_境外游', '旅游出行_户外探险', '旅游出行_主题旅游', '旅游出行_交通票务'],

        '汽车服务_违章查询':['商务服务_安全安保','汽车_车辆养护'],
        '汽车服务_加油油耗':['汽车_低档车','汽车_中档车', '汽车_高档车', '汽车_二手车', '汽车_租车', '汽车_车辆养护'],
        '汽车服务_驾考摇号': ['汽车_低档车','汽车_中档车', '汽车_高档车', '汽车_二手车', '汽车_车辆养护'],
        '汽车服务_行车辅助': ['汽车_低档车','汽车_中档车', '汽车_高档车', '汽车_二手车', '汽车_车辆养护'],
        '汽车服务_汽车保险': ['汽车_低档车','汽车_中档车', '汽车_高档车', '汽车_二手车', '汽车_车辆养护', '金融理财_保险'],
        '汽车服务_汽车买卖': ['汽车_低档车','汽车_中档车', '汽车_高档车', '汽车_二手车', '汽车_车辆养护', '商务服务_招商加盟', '商务服务_展会服务', '商务服务_营销广告', '商务服务_物流配送'],
        '汽车服务_汽车养护': ['汽车_低档车','汽车_中档车', '汽车_高档车', '汽车_二手车', '汽车_车辆养护'],
        '汽车服务_汽车资讯': ['汽车_低档车','汽车_中档车', '汽车_高档车', '汽车_二手车', '汽车_车辆养护'],

        '通勤出行_租车':['汽车_租车', '旅游出行_本地周边游', '旅游出行_国内游', '旅游出行_户外探险'],
        '通勤出行_拼车':['汽车_租车', '旅游出行_本地周边游', '旅游出行_国内游', '旅游出行_户外探险'],
        '通勤出行_共享单车':['旅游出行_本地周边游', '旅游出行_户外探险'],
        '通勤出行_公共交通': ['旅游出行_本地周边游', '旅游出行_户外探险', '汽车_租车'],
        '通勤出行_打车': ['旅游出行_本地周边游', '汽车_租车'],
        '通勤出行_代驾': ['旅游出行_本地周边游', '汽车_租车'],

        '房产租售_房产租赁':['家装百货_家装建材', '房产_租房', '房产_二手房', '房产_普通住宅', '房产_别墅豪宅', '房产_写字楼', '生活服务_家政服务'],
        '房产租售_房产买卖':['家装百货_家装建材', '房产_租房', '房产_二手房', '房产_普通住宅', '房产_别墅豪宅', '房产_写字楼', '生活服务_家政服务'],
        '房产租售_房产金融':['房产_租房', '房产_二手房', '房产_普通住宅', '房产_别墅豪宅', '房产_写字楼', '商务服务_营销广告', '商务服务_展会服务', '金融理财_股票基金'],
        '房产租售_综合租售':['房产_租房', '房产_二手房', '房产_写字楼', '汽车_租车', '商务服务_营销广告'],
        '房产租售_家居家装':['生活服务_家政服务', '家装百货_家居家纺', '家装百货_家电维修', '家装百货_日用百货', '家装百货_家装建材'],
        '房产租售_房产运营':['房产_租房', '房产_二手房', '房产_普通住宅', '房产_别墅豪宅', '房产_写字楼', '商务服务_营销广告', '商务服务_展会服务'],
        '房产租售_房产开发':['房产_租房', '房产_二手房', '房产_普通住宅', '房产_别墅豪宅', '房产_写字楼', '商务服务_营销广告', '商务服务_展会服务'],

        '阅读漫画_在线阅读':['教育培训_学前教育', '教育培训_中小学教育', '文化娱乐_动漫周边', '母婴儿童_儿童玩具', '科技数码_手机及配件'],
        '阅读漫画_动漫漫画':['教育培训_学前教育', '教育培训_中小学教育', '文化娱乐_动漫周边', '母婴儿童_儿童玩具'],
        '阅读漫画_少儿读物':['教育培训_学前教育', '教育培训_中小学教育', '母婴儿童_儿童玩具'],

        '幼儿园_幼儿园教育':['母婴儿童_写真拍照', '母婴儿童_儿童玩具', '母婴儿童_宝宝食品', '母婴儿童_宝宝用品', '教育培训_学前教育', '游戏_儿童益智'],
        '幼儿园_幼儿园游戏':['母婴儿童_写真拍照', '母婴儿童_儿童玩具', '母婴儿童_宝宝食品', '母婴儿童_宝宝用品', '教育培训_学前教育', '游戏_儿童益智'],

        '初中_初中教育':['游戏_儿童益智', '教育培训_中小学教育', '教育培训_出国留学'],
        '小学_小学教育':['游戏_儿童益智', '教育培训_中小学教育'],
        '中小学_5-12岁':['游戏_儿童益智', '教育培训_中小学教育'],
        '幼儿园_幼儿园阅读': ['游戏_儿童益智', '教育培训_中小学教育', '母婴儿童_儿童玩具'],

        '3c_电脑':['科技数码_电脑周边', '科技数码_游戏机', '科技数码_办公设备'],
        '3c_手机':['科技数码_手机及配件', '科技数码_数码摄像'],
        '3c_其他':['科技数码_电脑周边', '科技数码_游戏机', '科技数码_办公设备', '科技数码_手机及配件'],


        '智能硬件_智能安防':['家装百货_家用电器', '科技数码_数码摄像', '商务服务_安全安保'],
        '智能硬件_智能家居':['家装百货_家用电器', '房产_普通住宅', '房产_别墅豪宅', '家装百货_家居家纺', '家装百货_家用电器'],
        '智能硬件_穿戴设备':['科技数码_手机及配件', '服饰箱包_手表'],

        '宗教占卜_宗教占卜':['游戏_扑克棋牌', '文化娱乐_爱好收藏'],

        '视频直播_垂直直播':['文化娱乐_电影电视', '文化娱乐_艺术展览', '文化娱乐_酒吧KTV', '文化娱乐_演出', '科技数码_数码摄像'],
        '视频直播_垂直视频':['文化娱乐_电影电视', '文化娱乐_艺术展览', '文化娱乐_酒吧KTV', '文化娱乐_演出', '科技数码_数码摄像'],
        '视频直播_游戏直播':['游戏_跑酷竞速', '游戏_网络游戏', '游戏_动作射击', '游戏_塔防守卫'],
        '视频直播_电视视频':['文化娱乐_电影电视', '文化娱乐_艺术展览', '文化娱乐_酒吧KTV', '文化娱乐_演出', '科技数码_数码摄像'],
        '视频直播_综合视频':['文化娱乐_电影电视', '文化娱乐_艺术展览', '文化娱乐_酒吧KTV', '文化娱乐_演出', '科技数码_数码摄像'],
        '视频直播_播放器':['文化娱乐_电影电视', '科技数码_数码摄像', '科技数码_手机及配件'],
        '视频直播_综合直播':['文化娱乐_电影电视', '文化娱乐_艺术展览', '文化娱乐_酒吧KTV', '文化娱乐_演出', '科技数码_数码摄像'],
        '视频直播_体育直播':['体育运动_球类运动', '体育运动_户外运动', '科技数码_数码摄像'],
        '视频直播_短视频':['文化娱乐_电影电视', '文化娱乐_艺术展览', '文化娱乐_酒吧KTV', '文化娱乐_演出', '科技数码_数码摄像'],

        '网络购物_二手交易':['商务服务_物流配送', '商务服务_营销广告', '商务服务_招商加盟'],
        '网络购物_水果生鲜':['商务服务_物流配送', '餐饮美食_水果蔬菜', '餐饮美食_其他生鲜'],
        '网络购物_垂直电商':['商务服务_物流配送', '商务服务_营销广告', '商务服务_招商加盟'],
        '网络购物_综合电商':['商务服务_物流配送', '商务服务_营销广告', '商务服务_招商加盟'],
        '网络购物_商家用户':['商务服务_物流配送', '商务服务_营销广告', '商务服务_展会服务', '商务服务_招商加盟'],
        '网络购物_跨境海淘':['商务服务_物流配送', '旅游出行_港澳台游', '旅游出行_境外游'],
        '网络购物_奢侈品':['商务服务_物流配送', '金融理财_股票基金', '旅游出行_主题旅游', '汽车_高档车', '美容化妆_美容整形', '美容化妆_美发护发', '美容化妆_化妆护肤', '房产_别墅豪宅', '医疗健康_保健品'],
        '网络购物_嘻哈潮牌':['文化娱乐_动漫周边', '文化娱乐_电影电视', '文化娱乐_酒吧KTV'],
        '网络购物_艺术收藏':['文化娱乐_爱好收藏', '文化娱乐_艺术展览'],
        '网络购物_优惠特卖':['生活服务_娱乐设施', '房产_租房', '汽车_低档车'],
        '网络购物_商超便利':['生活服务_娱乐设施'],
        '网络购物_网购导流':['家装百货_日用百货', '科技数码_手机及配件', '商务服务_物流配送'],
        '网络购物_珠宝首饰':['生活服务_美容美发', '美容化妆_化妆护肤', '美容化妆_减肥瘦身', '服饰箱包_时尚女装', '服饰箱包_女鞋', '服饰箱包_内衣', '服饰箱包_珠宝配饰'],
        '网络购物_成人用品':['旅游出行_主题旅游', '旅游出行_酒店住宿', '医疗健康_保健品', '医疗健康_成人用品'],
        '网络购物_宠物用品':['生活服务_宠物用品', '家装百货_日用百货'],

        '金融服务_传统银行':['金融理财_股票基金', '金融理财_保险', '金融理财_银行理财', '金融理财_贵金属'],
        '金融服务_借贷':['金融理财_期货外汇', '金融理财_互联网金融', '金融理财_股票基金', '汽车_租车'],
        '金融服务_支付工具':['金融理财_互联网金融', '金融理财_股票基金'],
        '金融服务_记账':['金融理财_互联网金融', '金融理财_股票基金'],
        '金融服务_综合金融':['金融理财_互联网金融', '金融理财_股票基金', '金融理财_银行理财', '金融理财_保险', '金融理财_期货外汇'],
        '金融服务_理财':['金融理财_银行理财', '金融理财_股票基金', '金融理财_期货外汇', '金融理财_保险', '金融理财_互联网金融'],
        '金融服务_保险': ['金融理财_保险', '金融理财_银行理财', '金融理财_股票基金', '金融理财_期货外汇'],
        '金融服务_博彩彩票': ['金融理财_股票基金', '金融理财_彩票'],
        '金融服务_分期消费':['金融理财_互联网金融', '科技数码_手机及配件'],
        '金融服务_众筹':['金融理财_互联网金融', '科技数码_手机及配件'],

        '旅行出游_综合预定':['旅游出行_本地周边游', '旅游出行_国内游', '旅游出行_港澳台游', '旅游出行_境外游', '旅游出行_主题旅游', '旅游出行_酒店住宿', '旅游出行_交通票务', '生活服务_摄影照相'],
        '旅行出游_行程管理':['旅游出行_本地周边游', '旅游出行_国内游', '旅游出行_港澳台游', '旅游出行_境外游', '旅游出行_主题旅游', '旅游出行_酒店住宿', '旅游出行_交通票务', '生活服务_摄影照相'],
        '旅行出游_商务酒店':['旅游出行_本地周边游', '旅游出行_国内游', '旅游出行_港澳台游', '旅游出行_境外游', '旅游出行_主题旅游', '旅游出行_酒店住宿', '旅游出行_交通票务', '生活服务_摄影照相'],
        '旅行出游_非廉价航空':['旅游出行_本地周边游', '旅游出行_国内游', '旅游出行_港澳台游', '旅游出行_境外游', '旅游出行_主题旅游', '旅游出行_酒店住宿', '旅游出行_交通票务', '生活服务_摄影照相'],
        '旅行出游_民宿短租':['旅游出行_本地周边游', '旅游出行_国内游', '旅游出行_交通票务', '生活服务_摄影照相', '文化娱乐_艺术展览'],
        '旅行出游_火车高铁':['旅游出行_本地周边游', '旅游出行_国内游', '旅游出行_港澳台游', '旅游出行_境外游', '旅游出行_主题旅游', '旅游出行_酒店住宿', '旅游出行_交通票务', '生活服务_摄影照相'],
        '旅行出游_行程攻略':['旅游出行_本地周边游', '旅游出行_国内游', '旅游出行_港澳台游', '旅游出行_境外游', '旅游出行_主题旅游', '旅游出行_酒店住宿', '旅游出行_交通票务', '生活服务_摄影照相'],
        '旅行出游_廉价航空':['旅游出行_本地周边游', '旅游出行_国内游', '旅游出行_酒店住宿', '旅游出行_交通票务', '生活服务_摄影照相'],
        '旅行出游_经济酒店':['旅游出行_本地周边游', '旅游出行_国内游', '旅游出行_酒店住宿', '旅游出行_交通票务', '生活服务_摄影照相'],

        '求职招聘_实习招聘':['教育培训_职业教育', '教育培训_IT培训', '教育培训_技能培训', '商务服务_求职招聘'],
        '求职招聘_招聘求职':['教育培训_大学教育', '教育培训_职业教育', '教育培训_IT培训', '教育培训_技能培训', '商务服务_求职招聘'],
        '求职招聘_社保缴费':['教育培训_职业教育', '教育培训_公务员考试', '教育培训_技能培训', '金融理财_股票基金', '金融理财_银行理财'],
        '求职招聘_校园招聘':['教育培训_大学教育', '教育培训_职业教育', '教育培训_IT培训', '教育培训_技能培训', '商务服务_求职招聘', '教育培训_公务员考试'],
        '求职招聘_兼职招聘':['教育培训_IT培训', '教育培训_技能培训', '商务服务_求职招聘', '教育培训_公务员考试'],
        '求职招聘_人事资讯':['商务服务_求职招聘', '教育培训_职业教育', '教育培训_技能培训', '教育培训_拓展训练'],
        '求职招聘_人事管理':['商务服务_求职招聘', '教育培训_职业教育', '教育培训_技能培训', '教育培训_拓展训练'],
        '求职招聘_职场社交':['商务服务_求职招聘', '教育培训_职业教育', '教育培训_技能培训'],

        '健康美容_运动健身':['医疗健康_保健品', '体育运动_球类运动', '体育运动_休闲运动', '体育运动_跑步骑行', '体育运动_户外运动', '体育运动_运动装备'],
        '健康美容_健康管理':['医疗健康_保健品', '医疗健康_药品', '医疗健康_医疗诊疗', '体育运动_休闲运动', '体育运动_跑步骑行', '体育运动_户外运动', '体育运动_运动装备', '体育运动_球类运动', '餐饮美食_水果蔬菜', '美容化妆_减肥瘦身'],
        '健康美容_减肥瘦身':['美容化妆_减肥瘦身', '美容化妆_美容整形', '美容化妆_美发护发', '美容化妆_化妆护肤'],
        '健康美容_美容美妆':['美容化妆_减肥瘦身', '美容化妆_美容整形', '美容化妆_美发护发', '美容化妆_化妆护肤'],

        '医疗健康_医药服务':['医疗健康_医疗诊疗', '医疗健康_药品', '医疗健康_医疗器械'],
        '医疗健康_预约挂号':['医疗健康_医疗诊疗', '医疗健康_药品', '医疗健康_医疗器械'],
        '医疗健康_医疗资讯':['医疗健康_医疗诊疗', '医疗健康_药品', '医疗健康_医疗器械'],
        '医疗健康_问诊咨询':['医疗健康_医疗诊疗', '医疗健康_药品', '医疗健康_医疗器械'],
        '医疗健康_养生保健':['医疗健康_医疗诊疗', '医疗健康_药品', '医疗健康_医疗器械', '医疗健康_保健品'],
        '医疗健康_医疗管理':['医疗健康_医疗诊疗', '医疗健康_药品', '医疗健康_医疗器械', '医疗健康_保健品'],

        '母婴亲子_母婴健康':['母婴儿童_孕妇用品', '母婴儿童_胎教相关', '母婴儿童_宝宝用品', '母婴儿童_宝宝食品', '母婴儿童_写真拍照', '母婴儿童_儿童玩具', '教育培训_学前教育', '服饰箱包_童装童鞋'],
        '母婴亲子_母婴社区':['母婴儿童_孕妇用品', '母婴儿童_胎教相关', '母婴儿童_宝宝用品', '母婴儿童_宝宝食品', '母婴儿童_写真拍照', '母婴儿童_儿童玩具', '教育培训_学前教育', '服饰箱包_童装童鞋'],
        '母婴亲子_母婴工具':['母婴儿童_孕妇用品', '母婴儿童_胎教相关', '母婴儿童_宝宝用品', '母婴儿童_宝宝食品', '母婴儿童_写真拍照', '母婴儿童_儿童玩具', '教育培训_学前教育', '服饰箱包_童装童鞋'],
        '母婴亲子_母婴购物': ['母婴儿童_孕妇用品', '母婴儿童_胎教相关', '母婴儿童_宝宝用品', '母婴儿童_宝宝食品', '母婴儿童_写真拍照', '母婴儿童_儿童玩具', '教育培训_学前教育', '服饰箱包_童装童鞋'],

        '垂直行业_母婴':['母婴儿童_孕妇用品', '母婴儿童_胎教相关', '母婴儿童_宝宝用品', '母婴儿童_宝宝食品', '母婴儿童_写真拍照', '母婴儿童_儿童玩具', '教育培训_学前教育', '服饰箱包_童装童鞋'],
        '智能硬件_健康监测':['美容化妆_减肥瘦身', '医疗健康_保健品', '医疗健康_医疗器械'],

        '餐饮娱乐_优惠团购':['文化娱乐_酒吧KTV', '餐饮美食_餐馆'],
        '餐饮娱乐_点评推荐':['文化娱乐_酒吧KTV', '餐饮美食_餐馆', '家装百货_日用百货'],
        '餐饮娱乐_娱乐票务':['文化娱乐_演出', '文化娱乐_艺术展览'],
        '餐饮娱乐_外卖订餐':['餐饮美食_餐馆'],
        '餐饮娱乐_美食菜谱':['餐饮美食_餐馆'],

        '游戏娱乐_游戏娱乐':['科技数码_游戏机', '游戏_休闲时间', '游戏_跑酷竞速', '游戏_宝石消除', '游戏_网络游戏', '游戏_动作射击', '游戏_扑克棋牌', '游戏_儿童益智', '游戏_塔防守卫', '游戏_体育格斗', '游戏_角色扮演', '游戏_经营策略'],
        '游戏娱乐_游戏攻略':['科技数码_游戏机', '游戏_休闲时间', '游戏_跑酷竞速', '游戏_宝石消除', '游戏_网络游戏', '游戏_动作射击', '游戏_扑克棋牌', '游戏_塔防守卫', '游戏_体育格斗', '游戏_角色扮演', '游戏_经营策略'],
        '游戏娱乐_网易系游戏':['科技数码_游戏机', '游戏_休闲时间', '游戏_跑酷竞速', '游戏_宝石消除', '游戏_网络游戏', '游戏_动作射击', '游戏_扑克棋牌', '游戏_塔防守卫', '游戏_体育格斗', '游戏_角色扮演','游戏_经营策略'],
        '游戏娱乐_腾讯系游戏':['科技数码_游戏机', '游戏_休闲时间', '游戏_跑酷竞速', '游戏_宝石消除', '游戏_网络游戏', '游戏_动作射击', '游戏_扑克棋牌', '游戏_塔防守卫', '游戏_体育格斗', '游戏_角色扮演','游戏_经营策略'],
        '手机游戏_冒险游戏':['科技数码_游戏机', '游戏_跑酷竞速', '游戏_网络游戏', '游戏_动作射击', '游戏_体育格斗', '游戏_角色扮演',],
        '手机游戏_射击游戏':['科技数码_游戏机', '游戏_休闲时间', '游戏_网络游戏', '游戏_动作射击'],
        '手机游戏_游戏助手':['科技数码_游戏机', '游戏_休闲时间', '游戏_跑酷竞速', '游戏_宝石消除', '游戏_网络游戏', '游戏_动作射击', '游戏_扑克棋牌', '游戏_儿童益智', '游戏_塔防守卫', '游戏_体育格斗', '游戏_角色扮演','游戏_经营策略'],
        '手机游戏_休闲游戏':['科技数码_游戏机', '游戏_休闲时间', '游戏_儿童益智', '游戏_角色扮演','游戏_经营策略'],
        '手机游戏_模拟经营':['科技数码_游戏机', '游戏_休闲时间', '游戏_跑酷竞速', '游戏_宝石消除', '游戏_网络游戏', '游戏_扑克棋牌', '游戏_儿童益智', '游戏_塔防守卫', '游戏_体育格斗', '游戏_角色扮演'],
        '手机游戏_卡牌游戏':['游戏_扑克棋牌'],
        '手机游戏_角色扮演':['科技数码_游戏机', '游戏_角色扮演'],
        '手机游戏_赛车游戏':['科技数码_游戏机', '游戏_跑酷竞速', '游戏_网络游戏'],
        '手机游戏_跑酷游戏':['游戏_跑酷竞速'],
        '手机游戏_棋牌游戏':['游戏_休闲时间', '游戏_扑克棋牌',],
        '手机游戏_智力游戏':['科技数码_游戏机', '游戏_休闲时间', '游戏_儿童益智'],
        '手机游戏_策略游戏':['科技数码_游戏机', '游戏_经营策略'],
        '手机游戏_飞行游戏':['科技数码_游戏机', '游戏_网络游戏'],
        '手机游戏_桌面游戏':['文化娱乐_桌游', '科技数码_游戏机', '游戏_休闲时间'],
        '手机游戏_音乐游戏':['科技数码_游戏机', '游戏_休闲时间'],
        '手机游戏_动作游戏':['科技数码_游戏机', '游戏_跑酷竞速', '游戏_网络游戏', '游戏_动作射击', '游戏_体育格斗'],
        '手机游戏_小程序':['科技数码_游戏机', '游戏', '游戏_休闲时间', '游戏_跑酷竞速', '游戏_宝石消除', '游戏_网络游戏', '游戏_动作射击', '游戏_扑克棋牌', '游戏_儿童益智', '游戏_塔防守卫', '游戏_体育格斗', '游戏_角色扮演','游戏_经营策略'],

        '快递物流_物流运输':['商务服务_物流配送'],
        '快递物流_快递服务':['商务服务_物流配送'],

        '音乐广播_综合音乐':['文化娱乐_酒吧KTV', '文化娱乐_艺术展览', '文化娱乐_演出'],
        '音乐广播_在线电台':['文化娱乐_酒吧KTV', '文化娱乐_演出'],
        '音乐广播_手机铃声':['文化娱乐_酒吧KTV', '文化娱乐_艺术展览', '文化娱乐_演出'],
        '音乐广播_在线K歌':['文化娱乐_酒吧KTV', '文化娱乐_艺术展览', '文化娱乐_演出'],
        '音乐广播_音乐工具':['文化娱乐_酒吧KTV', '文化娱乐_艺术展览', '文化娱乐_演出'],

        '生活服务_家政服务':['生活服务_家政服务'],
        '生活服务_婚庆服务': ['生活服务_家政服务', '生活服务_鲜花礼品', '生活服务_摄影照相'],

        '社会公益_社会公益':['文化娱乐_爱好收藏'],

        '理财_证券':['金融理财_股票基金', '金融理财_保险', '金融理财_银行理财', '金融理财_期货外汇'],
        '理财_综合理财':['金融理财_股票基金', '金融理财_保险', '金融理财_银行理财', '金融理财_期货外汇', '金融理财_互联网金融'],
        '理财_互联网操盘':['金融理财_股票基金', '金融理财_期货外汇', '金融理财_互联网金融'],
        '理财_五大行':['金融理财_银行理财', '金融理财_期货外汇'],
        '理财_大型商业银行':['金融理财_股票基金', '金融理财_保险', '金融理财_银行理财', '金融理财_期货外汇', '金融理财_互联网金融'],
        '理财_信息门户':['金融理财_股票基金', '金融理财_保险', '金融理财_银行理财', '金融理财_期货外汇', '金融理财_互联网金融', '金融理财_彩票', '金融理财_贵金属'],
        '理财_记账/信用卡管理':['金融理财_银行理财', '金融理财_期货外汇'],
        '理财_p2p':['金融理财_股票基金', '金融理财_银行理财', '金融理财_互联网金融'],
        '理财_nan':['金融理财_股票基金', '金融理财_保险', '金融理财_银行理财', '金融理财_期货外汇', '金融理财_互联网金融', '金融理财_彩票', '金融理财_贵金属'],
        '理财_传统券商':['金融理财_股票基金', '金融理财_保险', '金融理财_银行理财', '金融理财_期货外汇', '金融理财_贵金属'],
        '理财_城商行':['金融理财_股票基金', '金融理财_保险', '金融理财_银行理财', '金融理财_期货外汇', '金融理财_互联网金融', '金融理财_贵金属'],
        '理财_传统保险':['金融理财_保险', '金融理财_银行理财'],
        '理财_智能投顾':['金融理财_股票基金', '金融理财_保险', '金融理财_银行理财', '金融理财_期货外汇', '金融理财_互联网金融', '金融理财_贵金属'],
        '理财_互联网保险':['金融理财_保险', '金融理财_银行理财', '金融理财_互联网金融'],
        '理财_虚拟货币':['金融理财_股票基金', '金融理财_保险', '金融理财_银行理财', '金融理财_期货外汇', '金融理财_互联网金融', '金融理财_贵金属'],
        '理财_农商行':['金融理财_股票基金', '金融理财_保险', '金融理财_银行理财', '金融理财_期货外汇', '金融理财_互联网金融', '金融理财_贵金属'],
        '理财_证券社区':['金融理财_股票基金', '金融理财_银行理财'],

        '借贷_网购分期':['金融理财_银行理财'],
        '借贷_线上小额':['金融理财_银行理财'],
        '借贷_教育分期':['金融理财_银行理财', '教育培训_IT培训', '教育培训_出国留学', '教育培训_技能培训'],
        '借贷_旅游分期':['金融理财_银行理财', '旅游出行_国内游', '旅游出行_港澳台游', '旅游出行_境外游', '旅游出行_主题旅游', '旅游出行_酒店住宿', '旅游出行_交通票务'],
        '借贷_银行信用卡':['金融理财_银行理财', '金融理财_互联网金融'],
        '借贷_信用卡代偿':['金融理财_银行理财', '金融理财_互联网金融'],
        '借贷_线上大额':['金融理财_银行理财', '金融理财_互联网金融', '金融理财_期货外汇'],
        '借贷_信用卡管理':['金融理财_银行理财', '金融理财_互联网金融', '金融理财_期货外汇'],
        '借贷_汽车分期':['金融理财_银行理财', '金融理财_互联网金融', '汽车_中档车', '汽车_高档车', '汽车_低档车'],
        '借贷_医美分期':['金融理财_银行理财', '美容化妆_美容整形', '美容化妆_减肥瘦身', '美容化妆_美发护发', '美容化妆_化妆护肤'],
        '借贷_租房分期':['金融理财_银行理财', '房产_租房', '房产_二手房', '房产_写字楼'],
        '借贷_nan':['金融理财_银行理财'],
        '借贷_农业分期':['金融理财_银行理财', '商务服务_农林牧渔', '商务服务_机械器材'],
    }
    # print(len(labelDic))
    # print(type(labelDic))
    # return labelDic.get(keys, default=[])
    labelstring = ''
    for lable in labelDic[keys]:
        labelstring += (lable + ',')
    return labelstring

if __name__ == '__main__':
    # 测试
    label_list = Interest_1_2('新闻资讯_垂直新闻')
    print(label_list)