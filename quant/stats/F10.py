# -*- coding: utf-8 -*-
#from quant.core.Stats import *
from quant.core.Spider import *
from quant.core.Stats import *
from pylsy import pylsytable
import time
import datetime
black_list = [
    #中信证券股份有限公司
    '10000018',
    #中国证券金融股份有限公司
    '80188285',
    #中央汇金资产管理有限责任公司
    '80553146',
    #香港中央结算有限公司
    '80010104'
]

'''
中国中投证券有限责任公司广州珠江东路证券营业部
银河资本-光大银行-银河资本-穗富2号资产管理计划

联讯证券股份有限公司广州东风中路证券营业部
广东粤财信托有限公司-穗富1号证券投资集合资金信托计划

#光大证券股份有限公司宁波中山西路证券营业部
上海七曜投资管理合伙企业(有限合伙)-安洪盈私募证券投资基金

80348562 西南证券股份有限公司西宁五四大街
深圳鑫泉资本管理有限公司-鑫泉复利增长1期私募基金

光大证券股份有限公司苏州苏惠路证券营业部
中信信托有限责任公司-中信稳健分层型证券投资集合资金信托计划1511F期

80040104 中银国际证券有限责任公司上海欧阳路
上海芮泰投资管理有限公司-芮泰投资上海怡鸣成长一号证券投资基金

'''

#最大35个交易日数据
MAX_BIG_ORDER_LIST = 35
#5档一天最多显示次数
MAX_FIVE_ORDER_NUM = 10
#5档最多显示的天数
MAX_FIVE_ORDER_DAYS = 30
'''
徐涛

当前持仓股东，在上一个周期的持仓中是否有退出某些股
股东持仓的周期还持有的其它股
上一个周期退出的股东，是否在当前周期有新有仓位变化
###
上海朱雀
珠海横琴昆仑鼎信
杭州理尚
广东君心盈泰
上海芮泰
深圳前海厚润德
哈程舟车(上海)股权投资管理有限公司
###

北京科瑞菲亚
山东海中湾
北京宏道
上海芮翊投资管理有限公司

深圳前海大概率
深圳市瀚信资产管理有限公
厦门国际信托有限公司
SELECT *  FROM `s_stock_shareholder` WHERE `end_date` >= 20160331 AND `sh_name` LIKE '%北京神州%' ORDER BY end_date DESC

SELECT * FROM  `s_stock_shareholdernum`  WHERE bigid IN (SELECT bigid FROM s_stock_shareholdernum WHERE  `enddate` >=20160815 AND totalshamt >500 ORDER BY enddate DESC ) ORDER BY askshrto ASC
浦江之星
中铁宝盈资产

广东泽泉
------
上海衍金投资

----------
上海永望资产管理有限公司-永望复利成长-文峰1号私募证券投资基金 华仪电气
上海镤月资产管理有限公司-虎皮永恒1号基金   南通锻压
上海华汯资产管理有限公司-华汯中国机遇私募投资基金 000811 烟台冰轮


-----------
\033[1;31;40m    <!--1-高亮显示 31-前景色红色  40-背景色黑色-->

'''
codeCodes = {
    '黑色':     '0;30', 'bright gray':    '0;37',
    '蓝色':      '0;34', '白色':          '1;37',
    '绿色':     '0;32', 'bright blue':    '1;34',
    'cyan':      '0;36', 'bright green':   '1;32',
    '红色':       '0;31', 'bright cyan':    '1;36',
    'purple':    '0;35', 'bright red':     '1;31',
    '***':    '0;33', 'bright purple':  '1;35',
    '灰色': '1;30', 'bright yellow':  '1;33',
    '正常':    '0'
}


class F10(StatsEngine):
    '''
    基本面
    '''
    def __init__(self):
        StatsEngine.__init__(self)
        #读取重点关注营业部
        #股东组合
        self.PARTNER = self.read_gudong_config()
        self.code_list = []

    def get_new_gudong(self):
        #统计新进个人股东大于等于3个的
        stock = self.mysql.getRecord("select * from s_stock_list where 1")
        for o in range(0, len(stock)):
            self.now_gudong(stock[o]['s_code'])
            print "=====================%s" % stock[o]['id']

    def get_jj_stock(self):
        #统计基金股,基金家数大于3
        stock = self.mysql.getRecord("select * from s_stock_list where 1")
        for o in range(0, len(stock)):
            self._get_jj_stock(stock[o]['s_code'])
            #sys.exit()
            print "=====================%s" % stock[o]['id']

    def _get_jj_stock(self, s_code):
        #获取最后一期时间
        #s_code = 'sz300059'
        last_date = self.mysql.getRecord("SELECT * FROM s_stock_shareholder WHERE  s_code='%s' group by end_date order by end_date desc limit 2 " % s_code)
        #退市
        if len(last_date) == 0:
            print 1
            return 1
        #最后一期前10
        current_gd = self.mysql.getRecord("SELECT * FROM  `s_stock_shareholder` WHERE  s_code='%s' and end_date=%s order by sh_rank asc" % (s_code, last_date[0]['end_date']))
        result = []
        for i in range(0, len(current_gd)):

            sh_name = current_gd[i]['sh_name']
            sh_code = current_gd[i]['sh_code']

            tmp_item = {
                's_code': current_gd[i]['s_code'],
                'sh_code': sh_code,
                'sh_name': sh_name,
                'sh_type': current_gd[i]['sh_type'],
                'sh_rank': current_gd[i]['sh_rank'],
                'sh_shares': current_gd[i]['sh_shares'],
                'sh_shares_p': current_gd[i]['sh_shares_p'],
                'ishis': self.__is_add(current_gd[i]['ishis'], current_gd[i]['chg']),
                'chg': current_gd[i]['chg'],
                'current_list': [],
                'prev_list': []
            }

            result.append(tmp_item)

        is_new_num = 0
        for a in range(0, len(result)):
            print '\033[1;31;40m'
            #this_money = int(result[a]['sh_shares'] * stock_list[result[a]['s_code']]['close'])
            this_money = 0
            print "%s===%s===%s===%s===%s===%s===%sW===%s" % (result[a]['sh_rank'], result[a]['sh_code'], self.__clear_gudong_name(result[a]['sh_name']), result[a]['ishis'], result[a]['sh_shares'], result[a]['sh_shares_p'], this_money, result[a]['chg'])
            print '\033[0m'
            #股东类型不是个人
            ms = re.findall(re.compile(r'私募|基金|期货|社保|汇金'), result[a]['sh_name'])
            #print len(ms)
            #sys.exit()
            if len(ms) == 0:
                #print 33333
                continue

            #break
            #if result[a]['sh_type'] != u'个人' and result[a]['ishis'] == u'【新进】':
            is_new_num += 1

            if is_new_num == 3:
                #print result[a]['sh_name']
                self.tools.sWrite(result[a]['s_code'], '/htdocs/jjglog.txt')
                break
                #is_new_num = -5

    def now_gudong(self, a_code=None):
        if a_code is not None:
            s_code = a_code
        else:
            s_code = sys.argv[2]
        s_code = s_code.upper()
        print s_code

        #获取最后一期时间
        last_date = self.mysql.getRecord("SELECT * FROM s_stock_shareholder WHERE  s_code='%s' group by end_date order by end_date desc limit 2 " % s_code)

        #退市
        if len(last_date) == 0:

            return 1
        #print len(last_date)
        #sys.exit()
        #最后一期前10
        current_gd = self.mysql.getRecord("SELECT * FROM  `s_stock_shareholder` WHERE  s_code='%s' and end_date=%s order by sh_rank asc" % (s_code, last_date[0]['end_date']))
        #上一期前10
        #prev_gd = self.mysql.getRecord("SELECT * FROM  `s_stock_shareholder` WHERE  s_code='%s' and end_date=%s order by sh_rank asc" % (s_code, last_date[1]['end_date']))

        result = []
        for i in range(0, len(current_gd)):

            sh_name = current_gd[i]['sh_name']
            sh_code = current_gd[i]['sh_code']

            tmp_item = {
                's_code': current_gd[i]['s_code'],
                'sh_code': sh_code,
                'sh_name': sh_name,
                'sh_type': current_gd[i]['sh_type'],
                'sh_rank': current_gd[i]['sh_rank'],
                'sh_shares': current_gd[i]['sh_shares'],
                'sh_shares_p': current_gd[i]['sh_shares_p'],
                'ishis': self.__is_add(current_gd[i]['ishis'], current_gd[i]['chg']),
                'chg': current_gd[i]['chg'],
                'current_list': [],
                'prev_list': []
            }

            result.append(tmp_item)

        is_new_num = 0
        for a in range(0, len(result)):
            print '\033[1;31;40m'
            #this_money = int(result[a]['sh_shares'] * stock_list[result[a]['s_code']]['close'])
            this_money = 0
            print "%s===%s===%s===%s===%s===%s===%sW===%s" % (result[a]['sh_rank'], result[a]['sh_code'], self.__clear_gudong_name(result[a]['sh_name']), result[a]['ishis'], result[a]['sh_shares'], result[a]['sh_shares_p'], this_money, result[a]['chg'])
            print '\033[0m'
            #股东个人新进个数大于等于3个
            if result[a]['sh_type'] == u'个人' and result[a]['ishis'] == u'【新进】':
                is_new_num += 1

            if is_new_num == 3:
                #print result[a]['sh_name']
                self.tools.sWrite(result[a]['s_code'], '/htdocs/glog.txt')
                is_new_num = -5

    def history_data(self, a_code=None):
        #n = u'高宏良'
        #print self.__find_majia(n)
        #sys.exit()
        #历史股东
        if a_code is not None:
            s_code = a_code
        else:
            s_code = sys.argv[2]
        kf = s_code[0:1]
        if int(kf) == 6:
            s_code = "SH%s" % s_code
        else:
            s_code = "SZ%s" % s_code

        self.s_code = s_code

        show_five_order = 0
        if len(sys.argv) == 4:
            show_five_order = 1

        stock = self.mysql.getRecord("select * from s_stock_list where 1")
        stock_list = {}
        for o in range(0, len(stock)):
            tmp_code = stock[o]['s_code'].upper()
            stock_list[tmp_code] = stock[o]

        #获取最后一期时间
        last_date = self.mysql.getRecord("SELECT * FROM s_stock_shareholder WHERE  s_code='%s' group by end_date order by end_date desc limit 2 " % s_code)
        #print last_date
        #最后一期前10
        current_gd = self.mysql.getRecord("SELECT * FROM  `s_stock_shareholder` WHERE  s_code='%s' and end_date=%s order by sh_rank asc" % (s_code, last_date[0]['end_date']))
        #上一期前10,新股只有一期数据
        prev_gd = []
        if len(last_date) == 2:
            prev_gd = self.mysql.getRecord("SELECT * FROM  `s_stock_shareholder` WHERE  s_code='%s' and end_date=%s order by sh_rank asc" % (s_code, last_date[1]['end_date']))
        '''
        如何有sh_code按股东代码查询,可能是基金
        查询当前股东当前周期操作什么股
        上一周期退出的股东，当前周期有没有操作什么股
        '''
        #出现股票代码,用来观察有那些营业部操作过
        #用来提示是否有大资金小号来操作
        xiaohao = []

        _current_list = {}
        for i in range(0, len(current_gd)):
            _current_list[current_gd[i]['sh_name']] = current_gd[i]
            self.__add_search_code(current_gd[i])
            if current_gd[i]['sh_type'] == u'个人':
                #1为持仓 2为清仓
                xiaohao.append({'name': current_gd[i]['sh_name'], 'opt': 1})

        #卖出的股东是否有新买入
        out_buy = []
        if len(prev_gd) > 0:
            #从上期中判断股东退出
            for i in range(0, len(prev_gd)):

                tmp_item = {
                    's_code': prev_gd[i]['s_code'],
                    'sh_code': prev_gd[i]['sh_code'],
                    'sh_name': prev_gd[i]['sh_name'],
                    'sh_type': prev_gd[i]['sh_type'],
                    'sh_rank': prev_gd[i]['sh_rank'],
                    'sh_shares': prev_gd[i]['sh_shares'],
                    'sh_shares_p': prev_gd[i]['sh_shares_p'],
                    'ishis': u'退出',
                    'chg': prev_gd[i]['chg'],
                    'prev_list': []
                }
                #_prev_list[prev_gd[k]['sh_name']] = prev_gd[k]
                #上一期股东不在当前周期,代表退出 退出股东是否有新进其他个股
                if prev_gd[i]['sh_name'] not in _current_list.keys():
                    #加入个人股东队列
                    if prev_gd[i]['sh_type'] == u'个人':
                        xiaohao.append({'name': prev_gd[i]['sh_name'], 'opt': 2})

                    if prev_gd[i]['sh_code'] != '0':
                        new_list = self.mysql.getRecord("SELECT * FROM  `s_stock_shareholder` WHERE  sh_code='%s' and end_date=%s order by sh_rank asc " % (prev_gd[i]['sh_code'], last_date[0]['end_date']))
                    else:
                        new_list = self.mysql.getRecord("SELECT * FROM  `s_stock_shareholder` WHERE  sh_name='%s' and end_date=%s order by sh_rank asc " % (prev_gd[i]['sh_name'], last_date[0]['end_date']))

                    if new_list is not None:
                        for j in range(0, len(new_list)):
                            #out_buy.append(new_list[j])
                            tmp_item['prev_list'].append(new_list[j])
                            self.__add_search_code(new_list[j])
                    out_buy.append(tmp_item)

        result = []
        for i in range(0, len(current_gd)):

            sh_name = current_gd[i]['sh_name']
            sh_code = current_gd[i]['sh_code']

            tmp_item = {
                's_code': current_gd[i]['s_code'],
                'sh_code': sh_code,
                'sh_name': sh_name,
                'sh_type': current_gd[i]['sh_type'],
                'sh_rank': current_gd[i]['sh_rank'],
                'sh_shares': current_gd[i]['sh_shares'],
                'sh_shares_p': current_gd[i]['sh_shares_p'],
                'ishis': self.__is_add(current_gd[i]['ishis'], current_gd[i]['chg']),
                'chg': current_gd[i]['chg'],
                'current_list': [],
                'prev_list': []
            }
            #股东在当前周期和上一周期分别持仓是什么
            if sh_code != '0':
                opt_list = self.mysql.getRecord("SELECT * FROM  `s_stock_shareholder` WHERE  sh_code='%s' and end_date=%s order by sh_rank asc " % (sh_code, last_date[0]['end_date']))
                prev_list = []
                if len(last_date) == 2:
                    prev_list = self.mysql.getRecord("SELECT * FROM  `s_stock_shareholder` WHERE  sh_code='%s' and end_date=%s order by sh_rank asc " % (sh_code, last_date[1]['end_date']))
                #print sh_code
            else:
                opt_list = self.mysql.getRecord("SELECT * FROM  `s_stock_shareholder` WHERE  sh_name='%s' and end_date=%s order by sh_rank asc " % (sh_name, last_date[0]['end_date']))
                prev_list = []
                if len(last_date) == 2:
                    prev_list = self.mysql.getRecord("SELECT * FROM  `s_stock_shareholder` WHERE  sh_name='%s' and end_date=%s order by sh_rank asc " % (sh_name, last_date[1]['end_date']))

                #print opt_list
                #sys.exit()
            #当前股东还有其它持股
            current_hold = []
            if opt_list is not None:
                for j in range(0, len(opt_list)):
                    tmp_item['current_list'].append(opt_list[j])
                    current_hold.append(opt_list[j]['s_code'])
                    self.__add_search_code(opt_list[j])

            #上一周期持仓
            if prev_list is not None:
                for j in range(0, len(prev_list)):
                    if prev_list[j]['s_code'] in current_hold:
                        continue
                    tmp_item['prev_list'].append(prev_list[j])
                    self.__add_search_code(prev_list[j])
            #退出股东是否新进新股

            result.append(tmp_item)

        for a in range(0, len(result)):
            print '\033[1;31;40m'
            this_money = int(result[a]['sh_shares'] * stock_list[result[a]['s_code']]['close'])
            print "%s==%s==%s==%s==%s==%sW==%s" % (result[a]['sh_rank'], result[a]['sh_code'], self.__clear_gudong_name(result[a]['sh_name']), result[a]['ishis'], round(result[a]['sh_shares'], 2), this_money, int(result[a]['chg']))
            print '\033[0m'

            ms = re.findall(re.compile(r'全国社保基金'), result[a]['sh_name'])
            if result[a]['sh_code'] in black_list or ms:
                print "..................BIG PIG..............."
                continue
            if len(result[a]['current_list']):
                #当前持仓
                current_hold = []
                ak = 1

                for b in range(0, len(result[a]['current_list'])):

                    bitem = result[a]['current_list'][b]
                    current_hold.append(bitem['s_code'])
                    if bitem['s_code'] == s_code:
                        continue
                    #超过10个不展示
                    if bitem['sh_type'] != '个人' and ak > 10:
                        continue
                    ak += 1
                    #持仓市值
                    hold_money = 0
                    aishis = self.__is_add(bitem['ishis'], bitem['chg'])
                    hold_money = int(bitem['sh_shares'] * stock_list[bitem['s_code']]['close'])
                   # hold_money = int(hold_money/10000)
                    print "%s===%s===%s===%s===%s===%s===%sW===%s" % ('\t' * 1, bitem['s_code'], stock_list[bitem['s_code']]['name'], bitem['sh_rank'], aishis, round(bitem['sh_shares'], 2), hold_money, int(bitem['chg']))
            #当前股东有没有在上周期中退出其它股
            if len(result[a]['prev_list']):
                print "=======SEll====="
                #prev_hold = []
                for b in range(0, len(result[a]['prev_list'])):
                    bitem = result[a]['prev_list'][b]
                    if bitem['s_code'] == s_code:
                        continue
                    #if bitem['s_code'] not in current_hold:
                    aishis = u'退出'
                    print "%s===%s===%s===%s===%s===%s===%s" % ('\t' * 1, bitem['s_code'], stock_list[bitem['s_code']]['name'], bitem['sh_rank'], aishis, round(bitem['sh_shares'], 2), int(bitem['chg']))

        print '\033[1;31;43m'
        print "=============================上一周期股东退出====================================="
        print '\033[0m'
        if out_buy is not None:
            for a in range(0, len(out_buy)):
                #退出的股东是基金同时,没有买入其它股不显示
                if out_buy[a]['sh_type'] != u'个人' and len(out_buy[a]['prev_list']) == 0:
                    continue

                print '\033[1;31;40m'
                this_money = int(out_buy[a]['sh_shares'] * stock_list[out_buy[a]['s_code']]['close'])
                print "%s==%s==%s==%s==%s==%sW==%s" % (out_buy[a]['sh_rank'], out_buy[a]['sh_code'], self.__clear_gudong_name(out_buy[a]['sh_name']), out_buy[a]['ishis'], round(out_buy[a]['sh_shares'], 2), this_money, int(out_buy[a]['chg']))
                print '\033[0m'
                ms = re.findall(re.compile(r'全国社保基金'), out_buy[a]['sh_name'])
                if out_buy[a]['sh_code'] in black_list or ms:
                    print "..................BIG PIG..............."
                    continue

                if len(out_buy[a]['prev_list']):
                    ak = 1
                    #持仓市值
                    hold_money = 0
                    for b in range(0, len(out_buy[a]['prev_list'])):
                        bitem = out_buy[a]['prev_list'][b]
                        if bitem['s_code'] == s_code:
                            continue
                        #超过10个不展示
                        if ak > 10:
                            continue
                        ak += 1
                        aishis = self.__is_add(bitem['ishis'], bitem['chg'])
                        hold_money += int(bitem['sh_shares'] * stock_list[bitem['s_code']]['close'])
                        #hold_money = int(hold_money/10000)
                        print "%s===%s===%s===%s===%s===%s===%sW===%s" % ('\t' * 1, bitem['s_code'], stock_list[bitem['s_code']]['name'], bitem['sh_rank'], aishis, round(bitem['sh_shares'], 2), hold_money, int(bitem['chg']))
        #股东人数变化
        print '\033[1;31;40m'
        print "================================股东人数变化================================"
        print '\033[0m'
        self.__get_gudong_nums(s_code.lower())

        #code_list 包含所有这次查询的股票池，和营业部数据进去匹配
        print '\033[1;31;40m'
        print "================================龙虎榜数据================================"
        print '\033[0m'
        self.__get_yyb_operate_code(s_code.lower())

        if show_five_order == 1:
            print '\033[1;31;40m'
            print "================================5档挂单================================"
            print '\033[0m'
            #5档持单数据,最近半年
            self.__get_five_order(s_code.lower())

            print '\033[1;31;40m'
            print "================================大单成交================================"
            print '\033[0m'
            self.__get_big_order(s_code.lower())

            print '\033[1;31;40m'
            print "================================每日买卖================================"
            print '\033[0m'
            self.__get_daily_order(s_code.lower())

        print '\033[1;31;40m'
        print "================================大资金马甲================================"
        print '\033[0m'
        for xx in range(0, len(xiaohao)):
            is_B = self.__find_majia(xiaohao[xx]['name'], xiaohao[xx]['opt'])
            if is_B:
                print is_B

        print '\033[1;31;40m'
        print "================================历史交叉================================"
        print '\033[0m'
        self.__get_gudong_history(xiaohao, stock_list)

        print '\033[1;31;40m'
        print "================================Over %s=%s===============================" % (s_code, stock_list[s_code]['name'])
        print '\033[0m'

    def __get_daily_order(self, s_code):
        opt_list = self.mysql.getRecord("SELECT * FROM  `s_stock_fenbi_daily` WHERE s_code='%s' order by dateline desc" % s_code)
        #print "SELECT * FROM  `s_stock_fenbi_daily` WHERE s_code='%s' order by dateline desc" % s_code
        if len(opt_list):
            attributes = ["Time", "Buy", "Sell", "All"]
            table = pylsytable(attributes)
            _time = []
            _buy = []
            _sell = []
            _all = []
            for x in range(0, len(opt_list)):
                _time.append(opt_list[x]['dateline'])
                _buy.append("%s/%s" % (opt_list[x]['b_price'], opt_list[x]['b_hands']))
                _sell.append("%s/%s" % (opt_list[x]['s_price'], opt_list[x]['s_hands']))
                _c = "{:.2f}".format((opt_list[x]['bs_count']/10000), '')
                _all.append(_c)
            table.add_data("Time", _time)
            table.add_data("Buy", _buy)
            table.add_data("Sell", _sell)
            table.add_data("All", _all)
            print(table.__str__())

    def __get_gudong_history(self, xiaohao, all_stock_list):
        #当期股东和退出股东历史仓位是否有重跌
        if len(xiaohao) == 0:
            return 1

        name_list = []
        for a in range(0, len(xiaohao)):
            name_list.append("'%s'" % xiaohao[a]['name'])

        namestr = ','.join(name_list)
        opt_list = self.mysql.getRecord("SELECT * FROM  `s_stock_shareholder` WHERE  sh_name in (%s)  order by end_date Desc " % namestr)

        if len(opt_list):
            stock_list = {}
            name_list = {}

            '''
            {
            'SH600888': [{'sh_name':'李白'}, {'sh_name':'李白2'}]
            }
            '''
            '''
            #股东操作过的历史股票
            history_stock_list = []
            for b in range(0, len(opt_list)):
                history_stock_list.append("'%s'" % opt_list[b]['s_code'])

            code_str = ','.join(history_stock_list)

            opt_stock_history = self.mysql.getRecord("SELECT * FROM  `s_stock_shareholder` WHERE s_code in (%s) order by end_date Desc " % code_str)

            for b in range(0, len(opt_stock_history)):
                s_code = opt_stock_history[b]['s_code']
                if self.s_code == s_code:
                    continue
                if opt_stock_history[b]['sh_type'] != u'个人':
                    continue
                if s_code not in stock_list.keys():
                    stock_list[s_code] = []
                    name_list[s_code] = []

                if opt_stock_history[b]['sh_name'] not in name_list[s_code]:
                    name_list[s_code].append(opt_stock_history[b]['sh_name'])
                    stock_list[s_code].append(opt_stock_history[b])

            for k, v in stock_list.items():
                if len(v) < 2:
                    continue
                print "===========%s===%s======" % (k, all_stock_list[k]['name'])
                for j in range(0, len(v)):
                    is_B = ''
                    if v[j]['sh_name'] in WZ_B:
                        is_B = u'【WZB】'
                    elif v[j]['sh_name'] in ZJ_B:
                        is_B = u'【ZJB】'
                    elif v[j]['sh_name'] in SD_B:
                        is_B = u'【SDB】'
                    else:
                        for av in range(0, len(NS_B)):
                            if v[j]['sh_name'] in NS_B[av]:
                                is_B = u'【%s/NSB】' % (av+1)
                                break

                    end_date = v[j]['end_date']
                    if v[j]['end_date'] == 20160930:
                        end_date = "\033[0;32;40m%s\033[0m" % v[j]['end_date']

                    print "\t====%s====%s====%s" % (end_date, is_B, v[j]['sh_name'])
            '''
            for b in range(0, len(opt_list)):
                s_code = opt_list[b]['s_code']
                #查询不展示
                if self.s_code == s_code:
                    continue
                if opt_list[b]['s_code'] not in stock_list.keys():
                    stock_list[s_code] = []
                    name_list[s_code] = []

                if opt_list[b]['sh_name'] not in name_list[s_code]:
                    name_list[s_code].append(opt_list[b]['sh_name'])
                    stock_list[s_code].append(opt_list[b])

            for k, v in stock_list.items():
                if len(v) < 2:
                    continue
                print "===========%s===%s======" % (k, all_stock_list[k]['name'])
                for j in range(0, len(v)):
                    is_B = self.__find_majia(v[j]['sh_name'], 0)

                    end_date = v[j]['end_date']
                    if v[j]['end_date'] == 20160930:
                        end_date = "\033[0;32;40m%s\033[0m" % v[j]['end_date']
                    if is_B:
                        print "\t====%s====%s====" % (end_date, is_B)
                    else:
                        print "\t====%s====%s====%s" % (end_date, is_B, v[j]['sh_name'])

    def __find_majia(self, name, ops=0):
        opt = ''
        if ops == 1:
            opt = '持仓'
        elif ops == 2:
            opt = '清仓'

        is_B = ''
        for k, v in self.PARTNER.items():
            for av in range(0, len(v)):
                if name in v[av]['people']:
                    is_B = u'【%s】【%s/%s/%s】==%s==%s===' % (v[av]['level'], (av+1), k, v[av]['key'], name, opt)
                    break

        return is_B

    def __get_gudong_nums(self, s_code):
        #股东人数变化
        data = self.mysql.getRecord("SELECT * FROM s_stock_shareholdernum WHERE  s_code='%s' order by enddate desc" % s_code)
        print "SELECT * FROM s_stock_shareholdernum WHERE  s_code='%s' order by enddate desc" % s_code
        if len(data) > 0:
            attributes = ["Time", "Peoples", "P_rate", "P_gu"]
            table = pylsytable(attributes)
            _time = []
            _peoples = []
            _p_rate = []
            _p_gu = []
            for x in range(0, len(data)):
                if data[x]['enddate'] < 20141230:
                    continue
                _time.append(data[x]['enddate'])
                _peoples.append(data[x]['totalshamt'])
                if data[x]['askshrto'] > 0:
                    _p_rate.append("\033[1;31;40m+%s\033[0m" % data[x]['askshrto'])
                else:
                    _p_rate.append("\033[0;32;40m%s\033[0m" % data[x]['askshrto'])
                _p_gu.append(data[x]['askavgsh'])

            table.add_data("Time", _time)
            table.add_data("Peoples", _peoples)
            table.add_data("P_rate", _p_rate)
            table.add_data("P_gu", _p_gu)
            print(table.__str__())

    def __get_five_order(self, s_code):
        data = self.mysql.getRecord("SELECT * FROM s_stock_runtime_snap WHERE  s_code='%s' order by dateline desc" % s_code)
        if len(data) > 0:
            attributes = ["Time", "Buy", "Sell"]
            table = pylsytable(attributes)
            _time = []
            _buy = []
            _sell = []
            _has_days = []
            _has_day_list = {}
            for x in range(0, len(data)):
                #最多有效期内的数据
                if len(_time) > MAX_FIVE_ORDER_DAYS:
                    break

                at = str(data[x]['date_str']).split(' ')
                _day = at[0]
                if _day not in _has_days:
                    _has_days.append(_day)
                    _has_day_list[_day] = 0

                #一天最多显示次数
                if _has_day_list[_day] == MAX_FIVE_ORDER_NUM:
                    continue
                _has_day_list[_day] += 1

                _time.append(data[x]['date_str'])
                _buy.append(data[x]['b_amount'])
                _sell.append(data[x]['s_amount'])

                #print "\t===%s===B:%sW====S:%sW====" % (data[x]['date_str'], data[x]['b_amount'], data[x]['s_amount'])
            table.add_data("Time", _time)
            table.add_data("Buy", _buy)
            table.add_data("Sell", _sell)
            print(table.__str__())

    def __get_big_order(self, s_code):
        #按天归类,取最近半年
        data = self.mysql.getRecord("SELECT * FROM s_stock_big_order WHERE  s_code='%s' order by datetime desc" % s_code)
        #print len(data)
        if len(data) > 0:
            #attributes = ["Day", "Small", "Middle", "Big", "Max"]
            attributes = ["Day", "Buy", "Sell", "Change"]
            table = pylsytable(attributes)
            days = []
            days2 = []
            res = {}
            for x in range(0, len(data)):
                at = str(data[x]['datetime']).split(' ')
                #print at
                _day = at[0]
                #_day = str(data[x]['dateline'])
                #35个交易日的数据
                if len(days2) > MAX_BIG_ORDER_LIST:
                    continue

                if _day not in res.keys():
                    res[_day] = {
                        'B_small': 0,
                        'B_s_money': 0,
                        'B_mid': 0,
                        'B_m_money': 0,
                        'B_big': 0,
                        'B_b_money': 0,
                        'B_max': 0,
                        'B_x_money': 0,
                        'S_small': 0,
                        'S_s_money': 0,
                        'S_mid': 0,
                        'S_m_money': 0,
                        'S_big': 0,
                        'S_b_money': 0,
                        'S_max': 0,
                        'S_x_money': 0,

                        'B_count': 0,
                        'B_money': 0,
                        'S_count': 0,
                        'S_money': 0,

                    }
                    d = datetime.datetime.strptime(_day, "%Y-%m-%d")
                    _dd = self.tools.d_date('%Y%m%d', time.mktime(d.timetuple()))
                    #print _day
                    days.append(_day)
                    days2.append(_dd)

                res[_day]["%s_count" % data[x]['bs_type']] += 1
                res[_day]["%s_money" % data[x]['bs_type']] += data[x]['bs_money']

                #150W以下
                if data[x]['bs_money'] <= 150:
                    vk = "%s_small" % data[x]['bs_type']
                    vkm = "%s_s_money" % data[x]['bs_type']
                    res[_day][vk] += 1
                    res[_day][vkm] += data[x]['bs_money']
                elif data[x]['bs_money'] <= 500 and data[x]['bs_money'] > 150:
                    vk = "%s_mid" % data[x]['bs_type']
                    vkm = "%s_m_money" % data[x]['bs_type']
                    res[_day][vk] += 1
                    res[_day][vkm] += data[x]['bs_money']
                elif data[x]['bs_money'] < 1000 and data[x]['bs_money'] > 500:
                    vk = "%s_big" % data[x]['bs_type']
                    vkm = "%s_b_money" % data[x]['bs_type']
                    res[_day][vk] += 1
                    res[_day][vkm] += data[x]['bs_money']
                elif data[x]['bs_money'] >= 1000:
                    vk = "%s_max" % data[x]['bs_type']
                    vkm = "%s_x_money" % data[x]['bs_type']
                    res[_day][vk] += 1
                    res[_day][vkm] += data[x]['bs_money']

            '''
            bs_f = []
            bm_f = []
            bb_f = []
            bx_f = []
            for k, v in res.items():
                #print k
                #print v
                bs_f.append("%s=%s=%s=%s" % (v['B_small'], v['B_s_money'], v['S_small'], v['S_s_money']))
                bm_f.append("%s=%s=%s=%s" % (v['B_mid'], v['B_m_money'], v['S_mid'], v['S_m_money']))
                bb_f.append("%s=%s=%s=%s" % (v['B_big'], v['B_b_money'], v['S_big'], v['S_b_money']))
                bx_f.append("%s=%s=%s=%s" % (v['B_max'], v['B_x_money'], v['S_max'], v['S_x_money']))
                #print len(res)

            table.add_data("Day", days)
            table.add_data("Small", bs_f)
            table.add_data("Middle", bm_f)
            table.add_data("Big", bb_f)
            table.add_data("Max", bx_f)
            print(table.__str__())
            '''
            #print res
            #print days
            #_a_days = list(reversed(days))
            #print _a_days
            stock_days = self.mysql.getRecord("SELECT * FROM s_stock_trade WHERE  s_code='%s' and dateline in(%s) " % (s_code, ','.join(days2)))
            #print stock_days
            #print "SELECT * FROM s_stock_trade WHERE  s_code='%s' and dateline in(%s) " % (s_code, ','.join(days))
            stock_daily = {}
            for j in range(0, len(stock_days)):
                d = datetime.datetime.strptime(str(stock_days[j]['dateline']), "%Y%m%d")
                _dd = self.tools.d_date('%Y-%m-%d', time.mktime(d.timetuple()))
                stock_daily[_dd] = stock_days[j]
                #stock_daily[str(stock_days[j]['dateline'])] = stock_days[j]
                #pass
            #print stock_daily['2016-10-10']
            __buy = []
            __sell = []
            __chg = []
            for ak in range(0, len(days)):
                k = days[ak]
                __buy.append("%s/%s" % (res[k]['B_money'], res[k]['B_count']))
                __sell.append("%s/%s" % (res[k]['S_money'], res[k]['S_count']))
                __chg.append(stock_daily[k]['chg'])
            '''
            for k, v in res.items():
                print k
                __buy.append("%s/%s" % (v['B_money'], v['B_count']))
                __sell.append("%s/%s" % (v['S_money'], v['S_count']))
                __chg.append(stock_daily[k]['chg'])
                #print "\t%s\tBUY:%s/%s\t=SELL:%s/%s\t%s==" % (k, v['B_money'], v['B_count'], v['S_money'], v['S_count'], stock_daily[k]['chg'])
            '''
            table.add_data("Day", days)
            table.add_data("Buy", __buy)
            table.add_data("Sell", __sell)
            table.add_data("Change", __chg)
            print(table.__str__())

    def __add_search_code(self, data):
        if data['sh_type'] == u'个人':
            self.code_list.append(data['s_code'])

    def __get_yyb_operate_code(self, s_code):
        #获取营业部操盘过的历史记录
        yyb_ids = []
        yyb_info = {}
        '''
        for i in range(0, len(self.yyb)):
            dep = self.yyb[i]
            for j in range(0, len(dep['items'])):
                yyb_ids.append(int(dep['items'][j]['yybid']))
                yyb_info[int(dep['items'][j]['yybid'])] = u"【%s】%s  %s" % (dep['area'], dep['items'][j]['yybid'], dep['items'][j]['name'])
        '''
        type_info = self.yyb_category()

        for k, v in self.PARTNER.items():
            for av in range(0, len(v)):
                for j in range(0, len(v[av]['yyb'])):
                    __yyb_id = v[av]['yyb'][j]['yyb_id']
                    yyb_ids.append(int(__yyb_id))
                    is_B = u'【%s】【%s】【%s/%s/%s】=%s=' % (type_info[k], v[av]['level'], (av+1), k, v[av]['key'], v[av]['yyb'][j]['name'])
                    yyb_info[int(__yyb_id)] = is_B

        #TODO 时间限定为最近1年
        lhb_data = self.mysql.getRecord("SELECT * FROM s_lhb_days_detail WHERE  s_code='%s' order by dateline desc" % s_code)
        if lhb_data is not None:
            b = []
            for x in range(0, len(lhb_data)):
                _tmp_data = lhb_data[x]
                if len(b) > 5:
                    break
                if _tmp_data['yyb_id'] in yyb_ids:
                    print "===%s==%s==%s==%s==%s=" % (_tmp_data['type'], self.__special_day(_tmp_data['dateline']), int(_tmp_data['B_volume']/10000), int(_tmp_data['S_volume']/10000), yyb_info[_tmp_data['yyb_id']],)
                    if _tmp_data['dateline'] not in b:
                        b.append(_tmp_data['dateline'])

    def __special_day(self, dateline):
        #关键时间，季报更新,加绿色
        a = ['0330', '0331', '0701', '0629', '0630', '0929', '0930', '1230', '1231']
        sp_day = []
        for i in range(2006, 2040):
            for k in range(0, len(a)):
                _t = "%s%s" % (i, a[k])
                sp_day.append(int(_t))

        if dateline in sp_day:
            dateline = "\033[0;32;40m%s\033[0m" % dateline
        return dateline

    def __is_add(self, ishis, chg):
        res = ''
        if ishis == 0:
            res = u'【新进】'
        else:
            if chg < 0:
                res = u'【减仓】'
            elif chg > 0:
                res = u'【加仓】'
            else:
                res = u'【持有】'
        return res

    def search_gudong_name(self):
        #根据股东名字查的
        name = sys.argv[2]
        stock = self.mysql.getRecord("select * from s_stock_list where 1")
        stock_list = {}
        for o in range(0, len(stock)):
            tmp_code = stock[o]['s_code'].upper()
            stock_list[tmp_code] = stock[o]
        #name_str = "%" + name + "%"
        name_str = name
        opt_list = self.mysql.getRecord("SELECT * FROM  `s_stock_shareholder` WHERE `sh_name` LIKE  '%s' order by end_date ASC " % name_str)
        if opt_list:
            for a in range(0, len(opt_list)):
                bitem = opt_list[a]
                if bitem['end_date'] < 20130101:
                    continue
                aishis = self.__is_add(bitem['ishis'], bitem['chg'])
                hold_money = int(bitem['sh_shares'] * stock_list[bitem['s_code']]['close'])
                print "%s===%s===%s===%s===%s===%s===%sW===%s==%s" % (bitem['s_code'], stock_list[bitem['s_code']]['name'], bitem['end_date'], bitem['sh_rank'], aishis, bitem['sh_shares'], hold_money, bitem['chg'], bitem['sh_code'])

    def search_jj_list(self):
        #查询私募基金是否持股,持仓不大于4个,单只占比不大于20%
        end_date = 20160630
        #prev_date = 20160331
        stock = self.mysql.getRecord("select * from s_stock_list where 1")
        stock_list = {}
        c = 0
        for o in range(0, len(stock)):
            tmp_code = stock[o]['s_code'].upper()
            stock_list[tmp_code] = stock[o]
        opt_list = self.mysql.getRecord("SELECT * FROM  `s_stock_shareholder` WHERE  `end_date` >=%s and sh_type !='%s' GROUP BY sh_code" % (end_date, u'个人'))
        jj_list = []
        for a in range(0, len(opt_list)):
            ditem = opt_list[a]
            ms = re.findall(re.compile(r'私募'), ditem['sh_name'])
            if len(ms) == 0:
                continue
            curr_list = self.mysql.getRecord("SELECT * FROM  `s_stock_shareholder` WHERE  sh_code='%s' and end_date=%s order by sh_rank asc " % (ditem['sh_code'], end_date))
            if len(curr_list) > 3:
                continue

            for b in range(0, len(curr_list)):
                if int(curr_list[b]['sh_shares_p']) > 30:
                    continue
                bitem = curr_list[b]
                jj_list.append(bitem['s_code'].lower())
                aishis = self.__is_add(bitem['ishis'], bitem['chg'])
                this_money = int(bitem['sh_shares'] * stock_list[bitem['s_code']]['close'])
                print "\t%s===%s===%s===%s===%s===%s===%s===%sW===%s" % (bitem['s_code'], stock_list[bitem['s_code']]['name'], bitem['end_date'], bitem['sh_rank'], aishis, bitem['sh_shares'], bitem['sh_shares_p'], this_money, bitem['chg'])

            print '\033[1;31;40m'
            print "%s====" % ditem['sh_name']
            print '\033[0m'
        c += 1
        print c

    def __clear_gudong_name(self, name):
        name = name.replace(u'股份', '')
        name = name.replace(u'有限公司', '')
        is_B = self.__find_majia(name, 0)
        if is_B:
            name = "\033[0;32;40m %s \033[0m" % is_B
        return name

    def gudong_zuhe_stock(self):
        #股东组合的持仓
        end_date = 20160930
        curr_list = self.mysql.getRecord("SELECT * FROM  `s_stock_shareholder` WHERE  end_date=%s order by sh_rank asc " % end_date)

        stock_list = []
        people_list = []
        for k, v in self.PARTNER.items():
            for av in range(0, len(v)):
                #if name in v[av]['people']:
                for k in range(0, len(v[av]['people'])):
                    if v[av]['people'][k] not in people_list:
                        people_list.append(v[av]['people'][k])

        for a in range(0, len(curr_list)):
            if curr_list[a]['sh_code'] == '0' and curr_list[a]['sh_name'] in people_list:
                if curr_list[a]['s_code'] not in stock_list:
                    stock_list.append(curr_list[a]['s_code'])
                    print curr_list[a]['s_code']

    def gudong_change(self):
        #股东减少排序
        vtype = sys.argv[2]

        if vtype == '1':
            #最后报告时间
            end_date = 20160815
            opt_list = self.mysql.getRecord("SELECT * FROM  `s_stock_shareholdernum`  WHERE bigid IN (SELECT bigid FROM s_stock_shareholdernum WHERE  `enddate` >=%s AND totalshamt >300 and askshrto<0 ORDER BY enddate DESC ) ORDER BY askshrto ASC limit 1500" % end_date)

        elif vtype == 2:
            #当期时间
            opt_list = []
        elif vtype == 3:
            #两连降
            opt_list = []
        elif vtype == 4:
            #三连降
            opt_list = []

        stock = self.mysql.getRecord("select * from s_stock_list where 1")
        stock_list = {}
        for o in range(0, len(stock)):
            tmp_code = stock[o]['s_code']
            stock_list[tmp_code] = stock[o]

        attributes = ["No", "Code", "Name", "Change", "All", "Gu", "Last"]
        table = pylsytable(attributes)
        _no = []
        _code = []
        _name = []
        _chage = []
        _all = []
        _gu = []
        _date = []

        codes = []
        i = 1
        for k in range(0, len(opt_list)):
            if opt_list[k]['s_code'] in codes:
                continue
            '''
            季度选股使用
            #市值大于150亿过滤,股数人数减少小于1000
            _stock_info = stock_list[opt_list[k]['s_code']]
            ms = re.findall(re.compile(r'\*|N|ST|航空|银行|钢铁|煤|药|酒|电力'), _stock_info['name'])
            if _stock_info['run_market'] > 15000000000 or _stock_info['run_market'] < 1500000000 or ms:
                continue

            #print opt_list[k]['s_code']
            #continue
            '''

            codes.append(opt_list[k]['s_code'])
            _no.append(i)
            _code.append(opt_list[k]['s_code'])
            _name.append(_stock_info['name'])
            _chage.append(opt_list[k]['askshrto'])
            _all.append(opt_list[k]['totalshamt'])
            _gu.append(opt_list[k]['askavgsh'])
            _date.append(opt_list[k]['enddate'])
            i += 1

        table.add_data("No", _no)
        table.add_data("Code", _code)
        table.add_data("Name", _name)
        table.add_data("Change", _chage)
        table.add_data("All", _all)
        table.add_data("Gu", _gu)
        table.add_data("Last", _date)
        print(table.__str__())

    def find_zh_majia(self):
        #查找历史股东中出现在特定股票组合的次数
        stock_A = ['SZ002552', 'SH600865', "SZ002346", "SZ002098", "SZ002287", "SZ002201", "SZ001748", "SZ000889", "SZ002333", "SH600310", "SZ300175", "SZ002753", "SZ002667", "SZ000791", "SZ002651", "SH603099", "SH600137", "002679", "SZ000916"]
        #stock_B = []
        res = {}
        current_list = self.mysql.getRecord("SELECT * FROM  `s_stock_shareholder` WHERE  sh_code=0  and end_date > 20140723")
        for a in range(0, len(current_list)):
            if current_list[a]['sh_name'] not in res.keys():
                res[current_list[a]['sh_name']] = []

            if current_list[a]['s_code'] in stock_A:
                if current_list[a]['s_code'] not in res[current_list[a]['sh_name']]:
                    res[current_list[a]['sh_name']].append(current_list[a]['s_code'])

        for k, v in res.items():
            if len(v) >= 2:
                print "%s===%s" % (k, "/".join(v))
