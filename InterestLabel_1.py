
# 兴趣定向 标签映射函数
def Interest_1(keys):

    labelDic = {
        '教育培训_K12教育':['教育培训'],
        '教育培训_早教辅导': ['教育培训'],
        '教育培训_高等教育': ['教育培训'],
        '教育培训_语言学习': ['教育培训'],
        '教育培训_出国留学': ['教育培训'],
        '教育培训_公开课': ['教育培训', '文化娱乐'],
        '教育培训_继续教育': ['教育培训'],

        '新闻资讯_趣味新闻':['文化娱乐'],
        '新闻资讯_垂直新闻':['文化娱乐'],
        '新闻资讯_综合新闻':['文化娱乐'],
        '新闻资讯_财经新闻':['金融理财'],
        '新闻资讯_体育资讯':['体育运动'],
        '新闻资讯_报纸':['文化娱乐'],
        '新闻资讯_男性时尚':['文化娱乐'],
        '新闻资讯_科学文化':['文化娱乐'],
        '新闻资讯_影视娱乐':['文化娱乐'],
        '新闻资讯_商业资讯':['金融理财', '商务服务'],
        '新闻资讯_杂志':['文化娱乐'],
        '新闻资讯_家电资讯':['家装百货'],
        '新闻资讯_法律资讯':['法律服务'],
        '新闻资讯_创业资讯':['商务服务', '金融理财'],
        '新闻资讯_互联网资讯':['金融理财', '科技数码'],

        '通讯社交_社区论坛':['文化娱乐'],
        '通讯社交_家校沟通':['教育培训'],
        '通讯社交_知识百科': ['家装百货', '文化娱乐'],
        '通讯社交_即时通讯': ['文化娱乐'],
        '通讯社交_匿名社交': ['文化娱乐'],
        '通讯社交_健身社区': ['医疗健康', '体育运动'],
        '通讯社交_职场社交': ['商务服务', '科技数码', '教育培训'],
        '通讯社交_LGBT社交': ['美容化妆', '服饰箱包', '生活服务'],
        '通讯社交_婚恋社交': ['医疗健康', '生活服务'],
        '通讯社交_微博博客': ['文化娱乐'],

        '图片摄影_图片编辑':['科技数码'],
        '图片摄影_影像剪辑':['科技数码', '文化娱乐', '生活服务'],
        '图片摄影_相册图库':['科技数码', '生活服务'],
        '图片摄影_摄影社区':['科技数码', '生活服务'],
        '图片摄影_照相机':['科技数码', '生活服务'],

        '实用工具_生活工具':['家装百货', '生活服务'],
        '实用工具_办公工具':['商务服务', '科技数码', '房产'],
        '实用工具_壁纸主题':['科技数码', '生活服务'],
        '实用工具_分类信息':['科技数码'],
        '实用工具_词典翻译':['教育培训', '商务服务'],
        '实用工具_应用商店':['科技数码'],
        '实用工具_手机工具':['科技数码'],
        '实用工具_地图导航':['旅游出行'],

        '汽车服务_违章查询':['汽车','商务服务'],
        '汽车服务_加油油耗':['汽车'],
        '汽车服务_驾考摇号': ['汽车'],
        '汽车服务_行车辅助': ['汽车'],
        '汽车服务_汽车保险': ['汽车', '金融理财'],
        '汽车服务_汽车买卖': ['汽车', '商务服务'],
        '汽车服务_汽车养护': ['汽车'],
        '汽车服务_汽车资讯': ['汽车'],

        '通勤出行_租车':['汽车', '旅游出行'],
        '通勤出行_拼车':['汽车', '旅游出行'],
        '通勤出行_共享单车':['旅游出行'],
        '通勤出行_公共交通': ['汽车', '旅游出行'],
        '通勤出行_打车': ['汽车', '旅游出行'],
        '通勤出行_代驾': ['汽车', '旅游出行'],

        '房产租售_房产租赁':['生活服务', '房产', '家装百货'],
        '房产租售_房产买卖':['生活服务', '房产', '家装百货'],
        '房产租售_房产金融':['金融理财', '商务服务', '房产'],
        '房产租售_综合租售':['商务服务', '房产', '汽车'],
        '房产租售_家居家装':['家装百货', '生活服务'],
        '房产租售_房产运营':['商务服务', '房产'],
        '房产租售_房产开发':['商务服务', '房产'],

        '阅读漫画_在线阅读':['母婴儿童', '教育培训', '科技数码', '文化娱乐'],
        '阅读漫画_动漫漫画':['母婴儿童', '教育培训', '文化娱乐'],
        '阅读漫画_少儿读物':['母婴儿童', '教育培训'],

        '幼儿园_幼儿园教育':['游戏', '教育培训', '母婴儿童'],
        '幼儿园_幼儿园游戏':['游戏', '教育培训', '母婴儿童'],

        '初中_初中教育':['游戏', '教育培训'],
        '小学_小学教育':['游戏', '教育培训'],
        '中小学_5-12岁':['游戏', '教育培训'],
        '幼儿园_幼儿园阅读': ['游戏', '教育培训', '母婴儿童'],

        '3c_电脑':['科技数码'],
        '3c_手机':['科技数码'],
        '3c_其他':['科技数码'],


        '智能硬件_智能安防':['商务服务', '科技数码', '家装百货'],
        '智能硬件_智能家居':['家装百货', '房产', '家装百货'],
        '智能硬件_穿戴设备':['科技数码', '服饰箱包'],

        '宗教占卜_宗教占卜':['文化娱乐', '游戏'],

        '视频直播_垂直直播':['科技数码', '文化娱乐'],
        '视频直播_垂直视频':['科技数码', '文化娱乐'],
        '视频直播_游戏直播':['游戏'],
        '视频直播_电视视频':['科技数码', '文化娱乐'],
        '视频直播_综合视频':['科技数码', '文化娱乐'],
        '视频直播_播放器':['科技数码', '文化娱乐'],
        '视频直播_综合直播':['科技数码', '文化娱乐'],
        '视频直播_体育直播':['体育运动', '科技数码'],
        '视频直播_短视频':['科技数码', '文化娱乐'],

        '网络购物_二手交易':['商务服务'],
        '网络购物_水果生鲜':['商务服务', '餐饮美食'],
        '网络购物_垂直电商':['商务服务'],
        '网络购物_综合电商':['商务服务'],
        '网络购物_商家用户':['商务服务'],
        '网络购物_跨境海淘':['商务服务', '旅游出行'],
        '网络购物_奢侈品':['医疗健康', '房产', '美容化妆', '汽车', '旅游出行', '金融理财', '商务服务'],
        '网络购物_嘻哈潮牌':['文化娱乐'],
        '网络购物_艺术收藏':['文化娱乐'],
        '网络购物_优惠特卖':['生活服务', '房产', '汽车'],
        '网络购物_商超便利':['生活服务'],
        '网络购物_网购导流':['家装百货', '科技数码', '商务服务'],
        '网络购物_珠宝首饰':['生活服务', '美容化妆', '服饰箱包'],
        '网络购物_成人用品':['旅游出行', '医疗健康'],
        '网络购物_宠物用品':['家装百货', '生活服务'],

        '金融服务_传统银行':['金融理财'],
        '金融服务_借贷':['金融理财', '汽车'],
        '金融服务_支付工具':['金融理财'],
        '金融服务_记账':['金融理财'],
        '金融服务_综合金融':['金融理财'],
        '金融服务_理财':['金融理财'],
        '金融服务_保险': ['金融理财'],
        '金融服务_博彩彩票': ['金融理财'],
        '金融服务_分期消费':['金融理财', '科技数码'],
        '金融服务_众筹':['金融理财', '科技数码'],

        '旅行出游_综合预定':['旅游出行', '生活服务'],
        '旅行出游_行程管理':['旅游出行', '生活服务'],
        '旅行出游_商务酒店':['旅游出行', '生活服务'],
        '旅行出游_非廉价航空':['旅游出行', '生活服务'],
        '旅行出游_民宿短租':['旅游出行', '生活服务', '文化娱乐'],
        '旅行出游_火车高铁':['旅游出行', '生活服务'],
        '旅行出游_行程攻略':['旅游出行', '生活服务'],
        '旅行出游_廉价航空':['旅游出行', '生活服务'],
        '旅行出游_经济酒店':['旅游出行', '生活服务'],

        '求职招聘_实习招聘':['教育培训', '商务服务'],
        '求职招聘_招聘求职':['教育培训', '商务服务'],
        '求职招聘_社保缴费':['教育培训', '金融理财'],
        '求职招聘_校园招聘':['教育培训', '商务服务'],
        '求职招聘_兼职招聘':['教育培训', '商务服务'],
        '求职招聘_人事资讯':['教育培训', '商务服务'],
        '求职招聘_人事管理':['教育培训', '商务服务'],
        '求职招聘_职场社交':['教育培训', '商务服务'],

        '健康美容_运动健身':['医疗健康', '体育运动'],
        '健康美容_健康管理':['医疗健康', '体育运动', '餐饮美食', '美容化妆'],
        '健康美容_减肥瘦身':['健康', '瘦身', '养生'],
        '健康美容_美容美妆':['健康', '瘦身', '养生'],

        '医疗健康_医药服务':['健康', '养生'],
        '医疗健康_预约挂号':['健康', '养生'],
        '医疗健康_医疗资讯':['健康', '养生'],
        '医疗健康_问诊咨询':['健康', '养生'],
        '医疗健康_养生保健':['健康', '养生'],
        '医疗健康_医疗管理':['健康', '政务', '商业'],

        '母婴亲子_母婴健康':['健康', '育儿', '瘦身', '养生'],
        '母婴亲子_母婴社区':['健康', '育儿', '瘦身', '养生'],
        '母婴亲子_母婴工具':['健康', '育儿', '瘦身', '养生'],
        '母婴亲子_母婴购物': ['健康', '育儿', '瘦身', '养生'],

        '垂直行业_母婴':['健康', '育儿', '瘦身', '养生'],
        '智能硬件_健康监测':['健康', '瘦身', '养生', '科技'],

        '餐饮娱乐_优惠团购':['美食', '小窍门', '本地'],
        '餐饮娱乐_点评推荐':['美食', '小窍门', '本地'],
        '餐饮娱乐_娱乐票务':['娱乐', '时尚', '星座'],
        '餐饮娱乐_外卖订餐':['美食', '本地', '探索'],
        '餐饮娱乐_美食菜谱':['美食', '本地'],

        '游戏娱乐_游戏娱乐':['娱乐', '游戏', '星座'],
        '游戏娱乐_游戏攻略':['娱乐', '游戏'],
        '游戏娱乐_网易系游戏':['娱乐', '游戏'],
        '游戏娱乐_腾讯系游戏':['娱乐', '游戏'],
        '手机游戏_冒险游戏':['娱乐', '游戏'],
        '手机游戏_射击游戏':['娱乐', '游戏', '军事'],
        '手机游戏_游戏助手':['娱乐', '游戏'],
        '手机游戏_休闲游戏':['娱乐', '游戏'],
        '手机游戏_模拟经营':['娱乐', '游戏', '政务', '商业'],
        '手机游戏_卡牌游戏':['娱乐', '游戏'],
        '手机游戏_角色扮演':['娱乐', '游戏', '文化'],
        '手机游戏_赛车游戏':['娱乐', '游戏'],
        '手机游戏_跑酷游戏':['娱乐', '游戏'],
        '手机游戏_棋牌游戏':['娱乐', '游戏'],
        '手机游戏_智力游戏':['娱乐', '游戏', '科学', '文化'],
        '手机游戏_策略游戏':['娱乐', '游戏', '科学'],
        '手机游戏_飞行游戏':['娱乐', '游戏'],
        '手机游戏_桌面游戏':['娱乐', '游戏'],
        '手机游戏_音乐游戏':['娱乐', '游戏'],
        '手机游戏_动作游戏':['娱乐', '游戏'],
        '手机游戏_小程序':['娱乐', '游戏'],

        '快递物流_物流运输':['美食', '本地'],
        '快递物流_快递服务':['美食', '本地'],

        '音乐广播_综合音乐':['娱乐', '电影', '游戏'],
        '音乐广播_在线电台':['娱乐', '情感', '探索'],
        '音乐广播_手机铃声':['娱乐', '时尚', '搞笑'],
        '音乐广播_在线K歌':['娱乐', '电影', '时尚'],
        '音乐广播_音乐工具':['娱乐', '电影', '时尚'],

        '生活服务_家政服务':['小窍门', '家居'],
        '生活服务_婚庆服务': ['社会', '育儿', '本地', '摄影'],

        '社会公益_社会公益':['社会', '文化', '历史'],

        '理财_证券':['财经', '商业', '科学'],
        '理财_综合理财':['财经', '商业', '科学'],
        '理财_互联网操盘':['财经', '商业', '科技'],
        '理财_五大行':['财经', '商业', '国际'],
        '理财_大型商业银行':['财经', '商业', '国际'],
        '理财_信息门户':['财经', '商业', '社会'],
        '理财_记账/信用卡管理':['财经', '商业', '社会'],
        '理财_p2p':['财经', '商业', '科技'],
        '理财_nan':['财经', '商业', '科技'],
        '理财_传统券商':['财经', '商业', '社会'],
        '理财_城商行':['财经', '商业', '社会'],
        '理财_传统保险':['财经', '商业'],
        '理财_智能投顾':['财经', '商业'],
        '理财_互联网保险':['财经', '商业'],
        '理财_虚拟货币':['财经', '商业'],
        '理财_农商行':['财经', '商业'],
        '理财_证券社区':['财经', '商业'],
        '借贷_网购分期':['财经', '商业'],
        '借贷_线上小额':['财经', '商业'],
        '借贷_教育分期':['教育', '职场', '毕业生'],
        '借贷_旅游分期':['旅游', '摄影', '数码', '探索'],
        '借贷_银行信用卡':['财经', '商业'],
        '借贷_信用卡代偿':['财经', '商业'],
        '借贷_线上大额':['财经', '商业'],
        '借贷_信用卡管理':['财经', '商业'],
        '借贷_汽车分期':['汽车', '商业'],
        '借贷_医美分期':['健康', '瘦身', '养生'],
        '借贷_租房分期':['职场', '毕业生'],
        '借贷_nan':['财经', '商业'],
        '借贷_农业分期':['财经', '商业'],
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
    label_list = Interest_1('新闻资讯_垂直新闻')
    print(label_list)