# -*- coding: utf-8 -*-
import sys
import logging
import json
import MySQLdb
from decimal import *
from quant.core.Spider import *


class StockSpider(SpiderEngine):
    '''
    更新指数
    '''
    def __init__(self):
        SpiderEngine.__init__(self)
        self.today = sys.argv[2]

    def run(self):
        self.tools.setup_logging(sys.argv[1], True, True)

        #self.get_xueqiu_base(1)
        #sys.exit()

        logging.debug('Start Base Stock=====Days:%s ' % sys.argv[2])
        day_list = self.mysql.getRecord("SELECT * FROM  `s_stock_list` WHERE  `run_market` =0 OR all_market =0")
        for i in range(0, len(day_list)):
            self.get_xueqiu_base(day_list[i])

    def get_level_2():
        #十档行情
        #https://app.leverfun.com/timelyInfo/timelyOrderForm?stockCode=300190
        pass

    def get_xueqiu_base(self, data):

        #s_code = 'SH600180'
        #print data
        s_code = data['s_code'].upper()
        self.curl_get('https://xueqiu.com/8205215793')
        url = "https://xueqiu.com/v4/stock/quote.json?code=%s&_=1423121365509" % s_code
        #url = "https://www.baidu.com"
        _data = self.curl_get(url)
        re = json.loads(_data)
        #流通
        a = int(Decimal(re[s_code]['float_shares']) * data['close'])
        #总股
        b = int(Decimal(re[s_code]['totalShares']) * data['close'])
        self.mysql.dbQuery("update s_stock_list set run_market=%s,all_market=%s where id=%s" % (a, b, data['id']))

    def __get_stock_ltgd(self, stock):
        #10大流通股东
        s_code = stock['s_code'].upper()
        #print s_code
        #sys.exit()
        self.curl_get('https://xueqiu.com/8205215793')
        url = 'https://xueqiu.com/stock/f10/otsholder.json?symbol=%s&page=1&size=4&_=1472904975952' % s_code
        _data = self.curl_get(url)
        re = json.loads(_data)
        #print re['list']

        if re['list'] is None:
            print "=========="
            return 1
        #sys.exit()
        for i in range(0, len(re['list'])):
            one = re['list'][i]
            #print one['list']

            for j in range(0, len(one['list'])):
                chg = one['list'][j]['chg']
                if chg is None:
                    chg = 0
                sh_code = one['list'][j]['shholdercode']
                if sh_code is None:
                    sh_code = 0
                name = one['list'][j]['shholdername'].replace("\\", "")
                name = name.replace("'", "")
                indata = {
                    'report_date': one['list'][j]['publishdate'],
                    'end_date': one['list'][j]['enddate'],
                    's_code': s_code,
                    'sh_code': sh_code,
                    'sh_name': MySQLdb.escape_string(name),
                    'sh_type': one['list'][j]['shholdertype'],
                    'sh_rank': one['list'][j]['rank2'],
                    'sh_shares': one['list'][j]['holderamt']/10000,
                    'sh_shares_p': one['list'][j]['pctoffloatshares'],
                    'sh_shares_a_p': one['list'][j]['holderrto'],
                    'sh_equity_type': one['list'][j]['shholdernature'],
                    'ishis': one['list'][j]['ishis'],
                    'chg': chg,

                }
                #print indata
                _where = "s_code='%s' and end_date=%s and sh_name='%s'" % (s_code, one['list'][j]['enddate'], name)
                _has = self.mysql.fetch_one("select * from  s_stock_shareholder where %s" % _where)
                if _has is not None:
                    self.mysql.dbUpdate('s_stock_shareholder', indata, _where)
                else:
                    self.mysql.dbInsert('s_stock_shareholder', indata)
                print indata
                #sys.exit()

    def get_ltgd(self):
        #10大流通股东
        day_list = self.mysql.getRecord("SELECT * FROM  `s_stock_list` WHERE  id>706")
        for i in range(0, len(day_list)):
            self.__get_stock_ltgd(day_list[i])
            print "=================%s" % day_list[i]['id']
            #time.sleep(3)

    def do_get(self):
        i = 1
        while 1:
            if i > 973381:
                break
            _has = self.mysql.fetch_one("select * from s_stock_shareholder where id=%s" % i)
            if _has['end_date'] < 20160530:
                i += 1
                continue
            i += 1
            print "%s===%s" % (i, a)