# -*- coding: utf-8 -*-
#from quant.core.Stats import *
from quant.core.Spider import *
from quant.core.Stats import *
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
wz = ['张令婷', '李淑芬']
'''
当前持仓股东，在上一个周期的持仓中是否有退出某些股
股东持仓的周期还持有的其它股
上一个周期退出的股东，是否在当前周期有新有仓位变化

'''


class F10(StatsEngine):
    '''
    基本面
    '''
    def __init__(self):
        StatsEngine.__init__(self)

    def get_new_gudong(self):
        #统计新进个人股东大于等于3个的
        stock = self.mysql.getRecord("select * from s_stock_list where 1")
        for o in range(0, len(stock)):
            self.now_gudong(stock[o]['s_code'])
            print "=====================%s" % stock[o]['id']

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
            print "%s===%s===%s===%s===%s===%s===%sW===%s" % (result[a]['sh_rank'], result[a]['sh_code'], result[a]['sh_name'], result[a]['ishis'], result[a]['sh_shares'], result[a]['sh_shares_p'], this_money, result[a]['chg'])
            print '\033[0m'
            #股东个人新进个数大于等于3个
            if result[a]['sh_type'] == u'个人' and result[a]['ishis'] == u'【新进】':
                is_new_num += 1
                #print result[a]['sh_name']

            if is_new_num == 3:
                #print result[a]['sh_name']
                self.tools.sWrite(result[a]['s_code'], '/htdocs/glog.txt')
                is_new_num = -5

    def history_gudong(self, a_code=None):
        #历史股东

        if a_code is not None:
            s_code = a_code
        else:
            s_code = sys.argv[2]
        s_code = s_code.upper()
        print s_code
        #sys.exit()
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
        #上一期前10
        prev_gd = self.mysql.getRecord("SELECT * FROM  `s_stock_shareholder` WHERE  s_code='%s' and end_date=%s order by sh_rank asc" % (s_code, last_date[1]['end_date']))
        '''
        如何有sh_code按股东代码查询,可能是基金
        查询当前股东当前周期操作什么股
        上一周期退出的股东，当前周期有没有操作什么股
        '''

        _current_list = {}
        for i in range(0, len(current_gd)):
            _current_list[current_gd[i]['sh_name']] = current_gd[i]

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
                if prev_gd[i]['sh_name'] not in _current_list.keys():
                    if prev_gd[i]['sh_code'] != '0':
                        new_list = self.mysql.getRecord("SELECT * FROM  `s_stock_shareholder` WHERE  sh_code='%s' and end_date=%s order by sh_rank asc " % (prev_gd[i]['sh_code'], last_date[0]['end_date']))
                    else:
                        new_list = self.mysql.getRecord("SELECT * FROM  `s_stock_shareholder` WHERE  sh_name='%s' and end_date=%s order by sh_rank asc " % (prev_gd[i]['sh_name'], last_date[0]['end_date']))

                    if new_list is not None:
                        for j in range(0, len(new_list)):
                            #out_buy.append(new_list[j])
                            tmp_item['prev_list'].append(new_list[j])
                    out_buy.append(tmp_item)

        #print _prev_list
        #sys.exit()
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
                prev_list = self.mysql.getRecord("SELECT * FROM  `s_stock_shareholder` WHERE  sh_code='%s' and end_date=%s order by sh_rank asc " % (sh_code, last_date[1]['end_date']))
                #print sh_code
            else:
                opt_list = self.mysql.getRecord("SELECT * FROM  `s_stock_shareholder` WHERE  sh_name='%s' and end_date=%s order by sh_rank asc " % (sh_name, last_date[0]['end_date']))
                prev_list = self.mysql.getRecord("SELECT * FROM  `s_stock_shareholder` WHERE  sh_name='%s' and end_date=%s order by sh_rank asc " % (sh_name, last_date[1]['end_date']))
            #当前股东还有其它持股
            current_hold = []
            if opt_list is not None:
                for j in range(0, len(opt_list)):
                    tmp_item['current_list'].append(opt_list[j])
                    current_hold.append(opt_list[j]['s_code'])
            #上一周期持仓
            if prev_list is not None:
                for j in range(0, len(prev_list)):
                    if prev_list[j]['s_code'] in current_hold:
                        continue
                    tmp_item['prev_list'].append(prev_list[j])
            #退出股东是否新进新股

            result.append(tmp_item)

        is_new_num = 0
        for a in range(0, len(result)):
            print '\033[1;31;40m'
            this_money = int(result[a]['sh_shares'] * stock_list[result[a]['s_code']]['close'])
            print "%s===%s===%s===%s===%s===%s===%sW===%s" % (result[a]['sh_rank'], result[a]['sh_code'], result[a]['sh_name'], result[a]['ishis'], result[a]['sh_shares'], result[a]['sh_shares_p'], this_money, result[a]['chg'])
            print '\033[0m'
            #股东个人新进个数大于等于3个
            if result[a]['sh_type'] == u'个人' and result[a]['ishis'] == u'【新进】':
                is_new_num += 1
                #print result[a]['sh_name']

            if is_new_num == 3:
                #print result[a]['sh_name']
                self.tools.sWrite(result[a]['s_code'], '/htdocs/glog.txt')
                is_new_num = -5

            ms = re.findall(re.compile(r'全国社保基金'), result[a]['sh_name'])
            if result[a]['sh_code'] in black_list or ms:
                print "..................BIG PIG..............."
                continue
            if len(result[a]['current_list']):
                #当前持仓
                current_hold = []
                ak = 1
                #持仓市值
                hold_money = 0
                for b in range(0, len(result[a]['current_list'])):

                    bitem = result[a]['current_list'][b]
                    current_hold.append(bitem['s_code'])
                    if bitem['s_code'] == s_code:
                        continue
                    #超过10个不展示
                    if ak > 10:
                        continue
                    ak += 1
                    aishis = self.__is_add(bitem['ishis'], bitem['chg'])
                    hold_money += int(bitem['sh_shares'] * stock_list[bitem['s_code']]['close'])
                   # hold_money = int(hold_money/10000)
                    print "%s====%s====%s====%s====%s====%s====%s====%sW====%s" % ('\t' * 1, bitem['s_code'], stock_list[bitem['s_code']]['name'], bitem['sh_rank'], aishis, bitem['sh_shares'], bitem['sh_shares_p'], hold_money, bitem['chg'])
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
                    print "%s====%s====%s====%s====%s====%s====%s====%s" % ('\t' * 1, bitem['s_code'], stock_list[bitem['s_code']]['name'], bitem['sh_rank'], aishis, bitem['sh_shares'], bitem['sh_shares_p'], bitem['chg'])

        print '\033[1;31;43m'
        print "=============================上一周期股东退出====================================="
        print '\033[0m'
        if out_buy is not None:
            for a in range(0, len(out_buy)):
                print '\033[1;31;40m'
                this_money = int(out_buy[a]['sh_shares'] * stock_list[out_buy[a]['s_code']]['close'])
                print "%s===%s===%s===%s===%s===%s===%sW===%s" % (out_buy[a]['sh_rank'], out_buy[a]['sh_code'], out_buy[a]['sh_name'], out_buy[a]['ishis'], out_buy[a]['sh_shares'], out_buy[a]['sh_shares_p'], this_money, out_buy[a]['chg'])
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
                        print "%s====%s====%s====%s====%s====%s====%s====%sW====%s" % ('\t' * 1, bitem['s_code'], stock_list[bitem['s_code']]['name'], bitem['sh_rank'], aishis, bitem['sh_shares'], bitem['sh_shares_p'], hold_money, bitem['chg'])

        #print result

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
        name = sys.argv[2]
        stock = self.mysql.getRecord("select * from s_stock_list where 1")
        stock_list = {}
        for o in range(0, len(stock)):
            tmp_code = stock[o]['s_code'].upper()
            stock_list[tmp_code] = stock[o]
        name_str = "%" + name + "%"
        opt_list = self.mysql.getRecord("SELECT * FROM  `s_stock_shareholder` WHERE  `sh_name` LIKE  '%s' order by end_date DESC " % name_str)
        if opt_list:
            for a in range(0, len(opt_list)):
                bitem = opt_list[a]
                aishis = self.__is_add(bitem['ishis'], bitem['chg'])
                print "%s====%s====%s====%s====%s====%s====%s====%s" % (bitem['s_code'], stock_list[bitem['s_code']]['name'], bitem['end_date'], bitem['sh_rank'], aishis, bitem['sh_shares'], bitem['sh_shares_p'], bitem['chg'])
