
# APP行为 按分类 标签映射函数
def APPbehaviorClassifi(keys):

    labelDic = {
        '教育培训_K12教育':['移动应用_阅读学习'],
        '新闻资讯_趣味新闻':['移动应用_新闻资讯'],
        '新闻资讯_垂直新闻':['移动应用_新闻资讯'],
        '新闻资讯_综合新闻':['移动应用_新闻资讯'],
        '新闻资讯_财经新闻':['移动应用_新闻资讯'],
        '新闻资讯_体育资讯':['移动应用_新闻资讯', '移动游戏_体育格斗'],
        '新闻资讯_报纸':['移动应用_新闻资讯', '移动应用_阅读学习'],
        '新闻资讯_男性时尚':['移动游戏', '移动应用_影音图像'],
        '新闻资讯_科学文化':['移动应用_阅读学习', '移动游戏_休闲时间'],
        '新闻资讯_影视娱乐':['移动应用_影音图像', '移动游戏_休闲时间'],
        '新闻资讯_商业资讯':['移动应用_金融理财', '移动应用_新闻资讯'],
        '新闻资讯_杂志':['移动应用_新闻资讯', '移动应用_阅读学习'],
        '新闻资讯_家电资讯':['移动应用_网上购物', '移动应用_生活服务'],
        '新闻资讯_法律资讯':['移动应用_新闻资讯', '移动应用_阅读学习'],
        '新闻资讯_创业资讯':['移动应用_金融理财', '移动应用_新闻资讯'],
        '新闻资讯_互联网资讯':['移动应用_新闻资讯', '移动应用_网上购物'],
        '通讯社交_社区论坛':['移动应用_社交网络', '移动应用_通信聊天', '移动应用_生活服务'],
        '通讯社交_家校沟通':['移动游戏_儿童益智', '移动应用_阅读学习', '移动应用_生活服务'],
        '图片摄影_图片编辑':['移动应用_影音图像', '移动应用_美化手机'],
        '图片摄影_影像剪辑':['移动应用_影音图像', '移动应用_美化手机'],
        '图片摄影_相册图库':['移动应用_影音图像', '移动应用_美化手机'],
        '图片摄影_摄影社区':['移动应用_影音图像', '移动应用_美化手机'],
        '图片摄影_照相机':['移动应用_影音图像', '移动应用_美化手机', '移动应用_常用工具'],
        '实用工具_生活工具':['移动应用_常用工具', '移动应用_生活服务'],
        '实用工具_办公工具':['移动应用_常用工具', '移动应用_办公软件'],
        '实用工具_壁纸主题':['移动应用_常用工具', '移动应用_美化手机'],
        '实用工具_分类信息':['移动应用_生活服务', '移动应用_常用工具'],
        '实用工具_词典翻译':['移动应用_阅读学习', '移动应用_常用工具'],
        '实用工具_应用商店':['移动应用_常用工具', '移动应用_性能优化'],
        '实用工具_手机工具':['移动应用_常用工具', '移动应用_性能优化', '移动应用_美化手机'],
        '实用工具_地图导航':['移动应用_常用工具', '移动应用_旅游出行'],
        '金融服务_保险':['移动应用_金融理财', '移动应用_新闻资讯'],
        '金融服务_博彩彩票':['移动应用_金融理财', '移动应用_新闻资讯'],
        '汽车服务_违章查询':['移动应用_旅游出行','移动应用_生活服务'],
        '汽车服务_加油油耗':['移动应用_旅游出行','移动应用_生活服务'],
        '汽车服务_汽车内容':['移动应用_旅游出行','移动应用_生活服务'],
        '通勤出行_租车':['移动应用_旅游出行', '移动应用_生活服务'],
        '通勤出行_拼车':['移动应用_旅游出行', '移动应用_生活服务'],
        '通勤出行_共享单车':['移动应用_旅游出行', '移动应用_生活服务'],
        '房产租售_房产租赁':['移动应用_生活服务', '移动应用_常用工具'],
        '房产租售_房产买卖':['移动应用_生活服务', '移动应用_常用工具'],
        '房产租售_房产金融':['移动应用_金融理财', '移动应用_常用工具'],
        '房产租售_综合租售':['移动应用_生活服务', '移动应用_常用工具', '移动应用_金融理财'],
        '房产租售_家居家装':['移动应用_生活服务', '移动应用_常用工具'],
        '房产租售_房产运营':['移动应用_生活服务', '移动应用_常用工具', '移动应用_金融理财'],
        '房产租售_房产开发':['移动应用_生活服务', '移动应用_常用工具', '移动应用_金融理财'],
        '阅读漫画_在线阅读':['移动应用_阅读学习', '移动应用_常用工具'],
        '阅读漫画_动漫漫画':['移动应用_常用工具', '移动应用_阅读学习'],
        '阅读漫画_少儿读物':['移动应用_常用工具', '移动应用_阅读学习', '移动游戏_儿童益智'],
        '幼儿园_幼儿园教育':['移动应用_常用工具', '移动应用_阅读学习', '移动游戏_儿童益智'],
        '幼儿园_幼儿园游戏':['移动应用_常用工具', '移动应用_阅读学习', '移动游戏_儿童益智'],
        '初中_初中教育':['移动应用_常用工具', '移动应用_阅读学习', '移动游戏_儿童益智'],
        '小学_小学教育':['移动应用_常用工具', '移动应用_阅读学习', '移动游戏_儿童益智'],
        '中小学_5-12岁':['移动应用_常用工具', '移动应用_阅读学习', '移动游戏_儿童益智'],
        '3c_电脑':['移动应用_常用工具', '移动应用_办公软件', '移动游戏_休闲时间'],
        '3c_手机':['移动应用_常用工具', '移动应用_办公软件', '移动游戏_休闲时间'],
        '3c_其他':['移动应用_常用工具', '移动游戏_休闲时间'],
        '幼儿园_幼儿园阅读':['移动应用_常用工具', '移动应用_阅读学习', '移动游戏_儿童益智'],
        '智能硬件_智能安防':['移动应用_常用工具', '移动应用_生活服务', '移动应用_性能优化'],
        '智能硬件_智能家居':['移动应用_常用工具', '移动应用_生活服务', '移动应用_性能优化'],
        '智能硬件_穿戴设备':['移动应用_常用工具', '移动应用_生活服务', '移动应用_性能优化'],
        '宗教占卜_宗教占卜':['移动应用_常用工具', '移动应用_生活服务', '移动游戏_休闲时间'],
        '视频直播_垂直直播':['移动游戏_休闲时间', '移动应用_社交网络'],
        '视频直播_垂直视频':['移动游戏_休闲时间', '移动应用_社交网络'],
        '视频直播_游戏直播':['移动游戏', '移动应用_社交网络'],
        '视频直播_电视视频':['移动应用_生活服务', '移动应用_性能优化'],
        '视频直播_综合视频':['移动应用_生活服务', '移动应用_性能优化'],
        '视频直播_播放器':['移动应用_生活服务', '移动应用_常用工具', '移动应用_影音图像'],
        '视频直播_综合直播':['移动游戏_休闲时间', '移动应用_社交网络'],
        '视频直播_体育直播':['移动游戏_休闲时间', '移动游戏_体育格斗'],
        '视频直播_短视频':['移动应用_常用工具', '移动应用_影音图像'],
        '视频直播_VR视频':['移动游戏_休闲时间', '移动应用_社交网络'],
        '网络购物_二手交易':['移动应用_生活服务', '移动应用_网上购物'],
        '网络购物_水果生鲜':['移动应用_网上购物', '移动应用_生活服务'],
        '网络购物_垂直电商':['移动应用_网上购物', '移动应用_生活服务'],
        '网络购物_综合电商':['移动应用_网上购物', '移动应用_生活服务'],
        '网络购物_商家用户':['移动应用_网上购物', '移动应用_金融理财'],
        '网络购物_跨境海淘':['移动应用_网上购物', '移动应用_金融理财'],
        '网络购物_奢侈品':['移动应用_网上购物', '移动应用_金融理财'],
        '网络购物_嘻哈潮牌':['移动应用_网上购物', '移动应用_金融理财'],
        '网络购物_艺术收藏':['移动应用_网上购物', '移动应用_金融理财'],
        '网络购物_优惠特卖':['移动应用_网上购物', '移动应用_生活服务', '移动应用_金融理财'],
        '网络购物_商超便利':['移动应用_网上购物', '移动应用_生活服务', '移动应用_金融理财'],
        '网络购物_网购导流':['移动应用_网上购物', '移动应用_生活服务', '移动应用_金融理财'],
        '网络购物_珠宝首饰':['移动应用_网上购物', '移动应用_金融理财'],
        '网络购物_成人用品':['移动应用_网上购物', '移动应用_生活服务'],
        '网络购物_宠物用品':['移动应用_网上购物', '移动应用_生活服务'],
        '金融服务_传统银行':['移动应用_金融理财', '移动应用_常用工具'],
        '金融服务_借贷':['移动应用_金融理财', '移动应用_常用工具'],
        '金融服务_支付工具':['移动应用_金融理财', '移动应用_常用工具'],
        '金融服务_记账':['移动应用_金融理财', '移动应用_常用工具'],
        '金融服务_综合金融':['移动应用_金融理财', '移动应用_常用工具'],
        '金融服务_理财':['移动应用_金融理财', '移动应用_常用工具'],
        '金融服务_分期消费':['移动应用_金融理财', '移动应用_常用工具', '移动应用_生活服务'],
        '金融服务_众筹':['移动应用_金融理财', '移动应用_常用工具', '移动应用_生活服务'],
        '旅行出游_综合预定':['移动应用_旅游出行', '移动应用_生活服务'],
        '旅行出游_行程管理':['移动应用_旅游出行', '移动应用_金融理财'],
        '旅行出游_商务酒店':['移动应用_旅游出行', '移动应用_金融理财'],
        '旅行出游_非廉价航空':['移动应用_旅游出行', '移动应用_金融理财'],
        '旅行出游_民宿短租':['移动应用_旅游出行', '移动应用_生活服务'],
        '旅行出游_火车高铁':['移动应用_旅游出行', '移动应用_金融理财', '移动应用_金融理财'],
        '旅行出游_行程攻略':['移动应用_旅游出行', '移动应用_金融理财'],
        '旅行出游_廉价航空':['移动应用_旅游出行', '移动应用_金融理财'],
        '旅行出游_经济酒店':['移动应用_旅游出行', '移动应用_金融理财'],
        '生活服务_婚庆服务':['移动应用_生活服务', '移动应用_旅游出行'],
        '教育培训_高等教育':['移动应用_阅读学习'],
        '教育培训_语言学习':['移动应用_阅读学习'],
        '教育培训_出国留学':['移动应用_阅读学习'],
        '教育培训_公开课':['移动应用_阅读学习'],
        '教育培训_继续教育':['移动应用_阅读学习'],
        '求职招聘_实习招聘':['移动应用_阅读学习'],
        '求职招聘_招聘求职':['移动应用_阅读学习'],
        '求职招聘_社保缴费':['移动应用_阅读学习'],
        '求职招聘_校园招聘':['移动应用_阅读学习'],
        '求职招聘_兼职招聘':['移动应用_阅读学习'],
        '求职招聘_人事资讯':['移动应用_新闻资讯'],
        '求职招聘_人事管理':['移动应用_常用工具', '移动应用_办公软件'],
        '求职招聘_职场社交':['移动应用_通信聊天', '移动应用_生活服务', '移动应用_社交网络'],
        '教育培训_早教辅导':['移动应用_阅读学习', '移动应用_生活服务'],
        '通讯社交_知识百科':['移动应用_通信聊天', '移动应用_阅读学习', '移动应用_社交网络'],
        '通讯社交_即时通讯':['移动应用_通信聊天', '移动应用_社交网络'],
        '通讯社交_匿名社交':['移动应用_通信聊天', '移动应用_社交网络'],
        '通讯社交_健身社区':['移动应用_生活服务', '移动应用_社交网络'],
        '通讯社交_职场社交':['移动应用_生活服务', '移动应用_社交网络'],
        '通讯社交_LGBT社交':['移动应用_通信聊天', '移动应用_社交网络'],
        '健康美容_运动健身':['移动应用_生活服务', '移动应用_常用工具'],
        '健康美容_健康管理':['移动应用_生活服务', '移动应用_常用工具'],
        '健康美容_减肥瘦身':['移动应用_生活服务', '移动应用_常用工具'],
        '健康美容_美容美妆':['移动应用_常用工具', '移动应用_生活服务'],
        '医疗健康_医药服务':['移动应用_生活服务'],
        '医疗健康_预约挂号':['移动应用_生活服务'],
        '医疗健康_医疗资讯':['移动应用_生活服务'],
        '医疗健康_问诊咨询':['移动应用_生活服务'],
        '医疗健康_养生保健':['移动应用_生活服务'],
        '医疗健康_医疗管理':['移动应用_生活服务'],
        '母婴亲子_母婴健康':['移动应用_阅读学习', '移动应用_生活服务'],
        '母婴亲子_母婴社区':['移动应用_阅读学习', '移动应用_生活服务'],
        '母婴亲子_母婴工具':['移动应用_常用工具', '移动应用_生活服务'],
        '母婴亲子_月子中心':['移动应用_阅读学习', '移动应用_生活服务'],
        '垂直行业_母婴':['移动应用_常用工具', '移动应用_生活服务'],
        '智能硬件_健康监测':['移动应用_常用工具', '移动应用_生活服务'],
        '母婴亲子_母婴购物':['移动应用_常用工具', '移动应用_生活服务'],
        '通讯社交_婚恋社交':['移动应用_社交网络', '移动应用_通信聊天'],
        '通讯社交_微博博客':['移动应用_社交网络', '移动应用_通信聊天'],
        '餐饮娱乐_优惠团购':['移动应用_网上购物', '移动应用_生活服务'],
        '餐饮娱乐_点评推荐':['移动应用_网上购物', '移动应用_生活服务'],
        '餐饮娱乐_娱乐票务':['移动应用_影音图像'],
        '餐饮娱乐_外卖订餐':['移动应用_生活服务', '移动应用_网上购物'],
        '餐饮娱乐_美食菜谱':['移动应用_常用工具', '移动应用_生活服务'],
        '通勤出行_公共交通':['移动应用_旅游出行', '移动应用_生活服务'],
        '通勤出行_打车':['移动应用_旅游出行', '移动应用_生活服务'],
        '通勤出行_代驾':['移动应用_旅游出行', '移动应用_生活服务'],
        '游戏娱乐_游戏娱乐':['移动游戏'],
        '游戏娱乐_游戏攻略':['移动游戏'],
        '游戏娱乐_网易系游戏':['移动游戏'],
        '游戏娱乐_腾讯系游戏':['移动游戏'],
        '手机游戏_冒险游戏':['移动游戏_动作射击', '移动游戏_跑酷竞速', '移动游戏_体育格斗', '移动游戏_网络游戏'],
        '手机游戏_射击游戏':['移动游戏_动作射击', '移动游戏_网络游戏'],
        '手机游戏_游戏助手':['移动游戏'],
        '手机游戏_休闲游戏':['移动游戏_休闲时间', '移动游戏_角色扮演'],
        '手机游戏_模拟经营':['移动游戏'],
        '手机游戏_卡牌游戏':['移动游戏_扑克棋牌', '移动游戏_休闲时间'],
        '手机游戏_角色扮演':['移动游戏_角色扮演'],
        '手机游戏_赛车游戏':['移动游戏_网络游戏', '移动游戏_跑酷竞速'],
        '手机游戏_跑酷游戏':['移动游戏_跑酷竞速'],
        '手机游戏_棋牌游戏':['移动游戏_扑克棋牌'],
        '手机游戏_智力游戏':['移动游戏_儿童益智', '移动游戏_休闲时间'],
        '手机游戏_策略游戏':['移动游戏_经营策略'],
        '手机游戏_飞行游戏':['移动游戏_网络游戏', '移动游戏_跑酷竞速'],
        '手机游戏_桌面游戏':['移动游戏'],
        '手机游戏_音乐游戏':['移动游戏_休闲时间', '移动游戏_网络游戏'],
        '手机游戏_动作游戏':['移动游戏_动作射击', '移动游戏_网络游戏', '移动游戏_跑酷竞速'],
        '手机游戏_小程序':['移动游戏'],
        '快递物流_物流运输':['移动应用_网上购物', '移动应用_生活服务'],
        '快递物流_快递服务':['移动应用_网上购物', '移动应用_生活服务'],
        '汽车服务_驾考摇号':['移动应用_常用工具', '移动应用_生活服务'],
        '汽车服务_行车辅助':['移动应用_常用工具', '移动应用_生活服务'],
        '汽车服务_汽车保险':['移动应用_金融理财', '移动应用_生活服务'],
        '汽车服务_汽车买卖':['移动应用_金融理财', '移动应用_网上购物'],
        '汽车服务_汽车养护':['移动应用_生活服务', '移动应用_性能优化'],
        '汽车服务_汽车资讯':['移动应用_新闻资讯', '移动应用_旅游出行'],
        '音乐广播_综合音乐':['移动应用_影音图像', '移动应用_常用工具'],
        '音乐广播_在线电台':['移动应用_影音图像', '移动应用_社交网络', '移动应用_通信聊天'],
        '音乐广播_手机铃声':['移动应用_影音图像', '移动应用_常用工具'],
        '音乐广播_在线K歌':['移动应用_影音图像', '移动应用_常用工具', '移动应用_社交网络'],
        '音乐广播_音乐工具':['移动应用_影音图像', '移动应用_常用工具'],
        '生活服务_家政服务':['移动应用_生活服务'],
        '社会公益_社会公益':['移动应用_社交网络', '移动应用_新闻资讯'],
        '理财_证券':['移动应用_金融理财', '移动应用_新闻资讯'],
        '理财_综合理财':['移动应用_金融理财', '移动应用_新闻资讯'],
        '理财_互联网操盘':['移动应用_金融理财', '移动应用_新闻资讯'],
        '理财_五大行':['移动应用_金融理财', '移动应用_新闻资讯'],
        '理财_大型商业银行':['移动应用_金融理财', '移动应用_新闻资讯'],
        '理财_信息门户':['移动应用_金融理财', '移动应用_新闻资讯'],
        '理财_外汇':['移动应用_金融理财', '移动应用_新闻资讯'],
        '理财_贵金属':['移动应用_金融理财', '移动应用_新闻资讯'],
        '理财_记账/信用卡管理':['移动应用_金融理财', '移动应用_新闻资讯'],
        '理财_p2p':['移动应用_金融理财', '移动应用_新闻资讯'],
        '理财_nan':['移动应用_金融理财', '移动应用_新闻资讯'],
        '理财_传统券商':['移动应用_金融理财', '移动应用_新闻资讯'],
        '理财_城商行':['移动应用_金融理财', '移动应用_新闻资讯'],
        '理财_传统保险':['移动应用_金融理财', '移动应用_新闻资讯'],
        '理财_智能投顾':['移动应用_金融理财', '移动应用_新闻资讯'],
        '理财_互联网保险':['移动应用_金融理财', '移动应用_新闻资讯'],
        '理财_虚拟货币':['移动应用_金融理财', '移动应用_新闻资讯'],
        '理财_农商行':['移动应用_金融理财', '移动应用_新闻资讯'],
        '理财_证券社区':['移动应用_金融理财', '移动应用_新闻资讯'],
        '借贷_网购分期':['移动应用_金融理财', '移动应用_网上购物'],
        '借贷_线上小额':['移动应用_金融理财', '移动应用_网上购物'],
        '借贷_教育分期':['移动应用_金融理财', '移动应用_网上购物', '移动应用_阅读学习'],
        '借贷_旅游分期':['移动应用_金融理财', '移动应用_网上购物', '移动应用_旅游出行'],
        '借贷_银行信用卡':['移动应用_金融理财', '移动应用_网上购物'],
        '借贷_信用卡代偿':['移动应用_金融理财', '移动应用_网上购物'],
        '借贷_线上大额':['移动应用_金融理财', '移动应用_网上购物'],
        '借贷_信用卡管理':['移动应用_金融理财', '移动应用_新闻资讯'],
        '借贷_汽车分期':['移动应用_金融理财', '移动应用_网上购物'],
        '借贷_医美分期':['移动应用_金融理财', '移动应用_网上购物', '移动应用_生活服务'],
        '借贷_租房分期':['移动应用_金融理财', '移动应用_网上购物', '移动应用_生活服务'],
        '借贷_nan':['移动应用_金融理财', '移动应用_网上购物', '移动应用_生活服务'],
        '借贷_农业分期':['移动应用_金融理财', '移动应用_网上购物', '移动应用_生活服务'],
        '借贷_第三方信用卡':['移动应用_金融理财', '移动应用_网上购物']
    }
    # print(len(labelDic))
    # print(type(labelDic))
    # return labelDic.get(keys, default=[])
    labelstring = ''
    for lable in labelDic[keys]:
        labelstring += (lable + ' ')
    return labelstring

if __name__ == '__main__':
    # 测试
    label_list = APPbehaviorClassifi('新闻资讯_体育资讯')
    print(label_list)
