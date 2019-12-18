
# 文章分类标签映射函数
def ArticleCalssification(keys):

    labelDic = {
        '教育培训_K12教育':['教育'],
        '新闻资讯_趣味新闻':['娱乐', '社会'],
        '新闻资讯_垂直新闻':['娱乐', '社会', '商业'],
        '新闻资讯_综合新闻':['娱乐', '社会', '商业'],
        '新闻资讯_财经新闻':['财经', '商业'],
        '新闻资讯_体育资讯':['体育'],
        '新闻资讯_报纸':['娱乐', '社会', '本地'],
        '新闻资讯_男性时尚':['娱乐', '体育', '时尚', '游戏'],
        '新闻资讯_科学文化':['文化', '科学', '社会', '科技'],
        '新闻资讯_影视娱乐':['娱乐', '时尚', '电影', '动漫'],
        '新闻资讯_商业资讯':['财经', '商业', '国际', '政务'],
        '新闻资讯_杂志':['娱乐', '社会', '商业'],
        '新闻资讯_家电资讯':['家居', '房产', '商业'],
        '新闻资讯_法律资讯':['科学', '政务', '法制'],
        '新闻资讯_创业资讯':['财经', '政务', '商业', '社会'],
        '新闻资讯_互联网资讯':['科技', '科学', '游戏'],
        '通讯社交_社区论坛':['娱乐', '社会'],
        '通讯社交_家校沟通':['社会', '育儿', '教育'],
        '图片摄影_图片编辑':['数码', '摄影'],
        '图片摄影_影像剪辑':['数码', '摄影', '设计'],
        '图片摄影_相册图库':['数码', '摄影', '设计'],
        '图片摄影_摄影社区':['数码', '摄影', '旅游'],
        '图片摄影_照相机':['数码', '摄影', '旅游'],
        '实用工具_生活工具':['小窍门', '家居'],
        '实用工具_办公工具':['职场', '政务'],
        '实用工具_壁纸主题':['摄影', '星座', '漫画', '设计'],
        '实用工具_分类信息':['小窍门', '探索', '设计'],
        '实用工具_词典翻译':['教育', '毕业生', '职场', '科学'],
        '实用工具_应用商店':['游戏', '数码', '收藏'],
        '实用工具_手机工具':['小窍门', '数码', '收藏'],
        '实用工具_地图导航':['旅游', '本地'],
        '金融服务_保险':['财经', '商业'],
        '金融服务_博彩彩票':['探索', '商业'],
        '汽车服务_违章查询':['汽车','本地'],
        '汽车服务_加油油耗':['汽车','本地', '职场'],
        '通勤出行_租车':['本地', '汽车', '商业'],
        '通勤出行_拼车':['本地', '职场'],
        '通勤出行_共享单车':['汽车','本地', '职场'],
        '房产租售_房产租赁':['本地', '家居', '房产', '商业'],
        '房产租售_房产买卖':['本地', '家居', '房产', '商业'],
        '房产租售_房产金融':['本地', '家居', '房产', '商业'],
        '房产租售_综合租售':['本地', '家居', '房产', '商业'],
        '房产租售_家居家装':['本地', '家居', '房产', '商业'],
        '房产租售_房产运营':['本地', '家居', '房产', '商业'],
        '房产租售_房产开发':['本地', '家居', '房产', '商业'],
        '阅读漫画_在线阅读':['漫画', '动漫', '美文', '星座', '故事'],
        '阅读漫画_动漫漫画':['漫画', '动漫', '搞笑', '故事'],
        '阅读漫画_少儿读物':['漫画', '育儿', '瘦身', '故事'],
        '幼儿园_幼儿园教育':['漫画', '育儿', '故事', '搞笑', '宠物'],
        '幼儿园_幼儿园游戏':['漫画', '育儿', '故事', '搞笑', '宠物'],
        '初中_初中教育':['教育', '毕业生', '故事'],
        '小学_小学教育':['教育', '毕业生', '故事'],
        '中小学_5-12岁':['教育', '毕业生', '故事'],
        '3c_电脑':['科技', '游戏', '数码'],
        '3c_手机':['科技', '娱乐', '数码'],
        '3c_其他':['科技', '游戏', '数码'],
        '幼儿园_幼儿园阅读':['漫画', '育儿', '故事', '搞笑'],
        '智能硬件_智能安防':['科学', '科技', '数码'],
        '智能硬件_智能家居':['科学', '科技', '数码'],
        '智能硬件_穿戴设备':['科学', '科技', '数码'],
        '宗教占卜_宗教占卜':['故事', '辟谣', '星座'],
        '视频直播_垂直直播':['娱乐', '时尚', '游戏', '探索'],
        '视频直播_垂直视频':['娱乐', '时尚', '游戏', '探索'],
        '视频直播_游戏直播':['娱乐', '游戏', '探索', '数码'],
        '视频直播_电视视频':['娱乐', '探索', '数码'],
        '视频直播_综合视频':['娱乐', '探索', '数码'],
        '视频直播_播放器':['娱乐', '探索'],
        '视频直播_综合直播':['娱乐', '社会', '时尚', '探索'],
        '视频直播_体育直播':['体育'],
        '视频直播_VR视频':['娱乐', '探索', '数码'],
        '视频直播_短视频':['娱乐', '搞笑', '时尚', '情感'],
        '网络购物_二手交易':['社会', '商业', '本地'],
        '网络购物_水果生鲜':['社会', '瘦身', '美食'],
        '网络购物_垂直电商':['社会', '商业'],
        '网络购物_综合电商':['社会', '商业'],
        '网络购物_商家用户':['财经', '商业'],
        '网络购物_跨境海淘':['商业', '社会', '国际'],
        '网络购物_奢侈品':['商业', '政务', '财经', '国际'],
        '网络购物_嘻哈潮牌':['娱乐', '奇葩', '时尚'],
        '网络购物_艺术收藏':['文化', '社会', '历史'],
        '网络购物_优惠特卖':['商业', '社会'],
        '网络购物_商超便利':['商业', '本地', '家居'],
        '网络购物_网购导流':['商业', '本地', '家居'],
        '网络购物_珠宝首饰':['时尚', '国际', '瘦身'],
        '网络购物_成人用品':['情感', '奇葩', '探索', '养生'],
        '网络购物_宠物用品':['宠物', '娱乐'],
        '金融服务_传统银行':['商业', '财经'],
        '金融服务_借贷':['商业', '财经', '毕业生'],
        '金融服务_支付工具':['商业', '财经', '社会'],
        '金融服务_记账':['商业', '财经', '政务', '职场'],
        '金融服务_综合金融':['商业', '财经'],
        '金融服务_理财':['商业', '财经'],
        '金融服务_分期消费':['商业', '娱乐', '毕业生', '职场'],
        '金融服务_众筹':['商业', '财经', '娱乐', '探索'],
        '旅行出游_综合预定':['旅游', '摄影', '数码', '探索'],
        '旅行出游_行程管理':['旅游', '商业', '政务', '财经'],
        '旅行出游_商务酒店':['旅游', '摄影', '数码', '商业', '政务', '财经'],
        '旅行出游_非廉价航空':['旅游', '摄影', '数码', '商业', '政务', '财经'],
        '旅行出游_民宿短租':['旅游', '文化'],
        '旅行出游_火车高铁':['旅游', '摄影', '数码', '商业', '政务', '财经'],
        '旅行出游_行程攻略':['旅游', '摄影', '数码'],
        '旅行出游_廉价航空':['旅游', '摄影', '数码', '文化'],
        '旅行出游_经济酒店':['旅游', '摄影', '数码', '文化'],
        '生活服务_婚庆服务':['社会', '育儿', '本地', '摄影'],
        '教育培训_高等教育':['教育', '毕业生', '职场'],
        '教育培训_语言学习':['教育', '毕业生', '职场'],
        '教育培训_出国留学':['教育', '毕业生', '国际', '社会'],
        '教育培训_公开课':['教育', '毕业生', '职场'],
        '教育培训_继续教育':['教育', '毕业生', '职场'],
        '求职招聘_实习招聘':['教育', '毕业生', '职场'],
        '求职招聘_招聘求职':['教育', '毕业生', '职场'],
        '求职招聘_社保缴费':['教育', '毕业生', '职场'],
        '求职招聘_校园招聘':['教育', '毕业生', '职场'],
        '求职招聘_兼职招聘':['毕业生', '职场'],
        '求职招聘_人事资讯':['政务', '教育', '毕业生', '职场'],
        '求职招聘_人事管理':['政务', '教育', '毕业生', '职场'],
        '求职招聘_职场社交':['教育', '毕业生', '职场'],
        '教育培训_早教辅导':['教育', '育儿', '本地', '社会'],
        '通讯社交_知识百科':['社会', '小窍门', '科学', '探索'],
        '通讯社交_即时通讯':['数码', '本地'],
        '通讯社交_匿名社交':['奇葩', '探索', '情感', '故事'],
        '通讯社交_健身社区':['健康', '瘦身', '养生'],
        '通讯社交_职场社交':['职场', '政务'],
        '通讯社交_LGBT社交':['瘦身', '情感', '星座', '美食', '时尚'],
        '健康美容_运动健身':['健康', '瘦身', '养生'],
        '健康美容_健康管理':['健康', '瘦身', '养生'],
        '健康美容_减肥瘦身':['健康', '瘦身', '养生'],
        '健康美容_美容美妆':['健康', '瘦身', '养生'],
        '医疗健康_医药服务':['健康', '养生'],
        '医疗健康_预约挂号':['健康', '养生'],
        '医疗健康_医疗资讯':['健康', '养生'],
        '医疗健康_问诊咨询':['健康', '养生'],
        '医疗健康_养生保健':['健康', '养生'],
        '医疗健康_医疗管理':['健康', '政务', '商业'],
        '母婴亲子_母婴健康':['健康', '育儿', '瘦身', '养生'],
        '母婴亲子_月子中心':['健康', '育儿', '瘦身'],
        '母婴亲子_母婴社区':['健康', '育儿', '瘦身', '养生'],
        '母婴亲子_母婴工具':['健康', '育儿', '瘦身', '养生'],
        '垂直行业_母婴':['健康', '育儿', '瘦身', '养生'],
        '智能硬件_健康监测':['健康', '瘦身', '养生', '科技'],
        '母婴亲子_母婴购物':['健康', '育儿', '瘦身', '养生'],
        '通讯社交_婚恋社交':['社会', '职场', '情感', '奇葩', '本地', '瘦身'],
        '通讯社交_微博博客':['娱乐', '情感', '星座', '搞笑'],
        '餐饮娱乐_优惠团购':['美食', '小窍门', '本地'],
        '餐饮娱乐_点评推荐':['美食', '小窍门', '本地'],
        '餐饮娱乐_娱乐票务':['娱乐', '时尚', '星座'],
        '餐饮娱乐_外卖订餐':['美食', '本地', '探索'],
        '餐饮娱乐_美食菜谱':['美食', '本地'],
        '通勤出行_公共交通':['职场', '本地', '政务', '美文'],
        '通勤出行_打车':['职场', '本地', '政务'],
        '通勤出行_代驾':['职场', '本地', '本地', '汽车'],
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
        '汽车服务_驾考摇号':['汽车', '旅游', '本地'],
        '汽车服务_行车辅助':['汽车', '科技'],
        '汽车服务_汽车保险':['汽车', '商业'],
        '汽车服务_汽车买卖':['汽车', '本地'],
        '汽车服务_汽车养护':['汽车', '本地'],
        '汽车服务_汽车资讯':['汽车', '本地'],
        '汽车服务_汽车内容':['汽车', '本地'],
        '音乐广播_综合音乐':['娱乐', '电影', '游戏'],
        '音乐广播_在线电台':['娱乐', '情感', '探索'],
        '音乐广播_手机铃声':['娱乐', '时尚', '搞笑'],
        '音乐广播_在线K歌':['娱乐', '电影', '时尚'],
        '音乐广播_音乐工具':['娱乐', '电影', '时尚'],
        '生活服务_家政服务':['小窍门', '家居'],
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
        '理财_外汇':['财经', '商业'],
        '理财_贵金属':['财经', '商业'],
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
        '借贷_第三方信用卡':['财经', '商业']
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
    label_list = ArticleCalssification('新闻资讯_垂直新闻')
    print(label_list)
