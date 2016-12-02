# -*- coding: utf-8 -*-
import sys
import logging
import json
import time
import datetime
import MySQLdb
import commands
import base64
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
        logging.debug('Start Base Stock=====Days:%s ' % sys.argv[2])
        day_list = self.mysql.getRecord("SELECT * FROM  `s_stock_list` WHERE  `run_market` =0 OR all_market =0")
        for i in range(0, len(day_list)):
            self.get_xueqiu_base(day_list[i])

    def get_level_2():
        #十档行情
        #https://app.leverfun.com/timelyInfo/timelyOrderForm?stockCode=300190
        pass

    def save_multi_data(self):
        dateline = sys.argv[2]
        i = 0
        while True:
            stock = self.mysql.getRecord("select * from s_stock_list where dateline=%s and id >%s limit 100" % (dateline, i))
            print "select * from s_stock_list where dateline=%s and id >%s limit 100" % (dateline, i)
            #stock = self.mysql.getRecord("select * from s_stock_trade where dateline=%s limit %s ,100" % (dateline, i))
            if len(stock) == 0:
                break
            datas = []
            for o in range(0, len(stock)):
                datas.append(stock[o]['s_code'])
                i = stock[o]['id']
                #i += 1
            self.run_worker(datas)

    def get_info(self, s_code):
        dateline = sys.argv[2]
        #d = datetime.datetime.strptime(dateline, "%Y%m%d")
        #days = self.tools.d_date('%Y-%m-%d', time.mktime(d.timetuple()))
        #url = 'http://market.finance.sina.com.cn/downxls.php?date=%s&symbol=%s' % (days, s_code)
        url = "%s=%s" % (dateline, s_code)
        out_put = '/usr/bin/php /htdocs/quant/fb.php %s' % base64.b64encode(url)
        print out_put
        _data = commands.getoutput(out_put)
        print _data

    def get_xueqiu_base(self, data):
        #s_code = 'SH600180'
        #print data
        s_code = data['s_code'].upper()
        self.curl_get('https://xueqiu.com/8205215793')
        url = "https://xueqiu.com/v4/stock/quote.json?code=%s&_=1423121365509" % s_code
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
        self.curl_get('https://xueqiu.com/8205215793')
        url = 'https://xueqiu.com/stock/f10/otsholder.json?symbol=%s&page=1&size=4&_=1472904975952' % s_code
        _data = self.curl_get(url)
        re = json.loads(_data)

        if re['list'] is None:
            print "=========="
            return 1
        for i in range(0, len(re['list'])):
            one = re['list'][i]
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
                if int(indata['end_date']) <= 20140930:
                    continue
                _where = "s_code='%s' and end_date=%s and sh_name='%s'" % (s_code, one['list'][j]['enddate'], name)
                _has = self.mysql.fetch_one("select * from  s_stock_shareholder where %s" % _where)
                if _has is not None:
                    self.mysql.dbUpdate('s_stock_shareholder', indata, _where)
                else:
                    self.mysql.dbInsert('s_stock_shareholder', indata)
                print indata

    def get_ltgd(self):
        #10大流通股东
        '''
        day_list = self.mysql.getRecord("SELECT sh_code, sh_name FROM  `s_stock_shareholder`  WHERE  `end_date` >=20160930 AND  `sh_name` LIKE  '%私募%' order by sh_name LIMIT 500")
        for i in range(0, len(day_list)):
            print "%s,%s" % (day_list[i]['sh_code'], day_list[i]['sh_name'])

        return
        '''
        day_list = self.mysql.getRecord("SELECT * FROM  `s_stock_list` WHERE  id>0")
        for i in range(0, len(day_list)):
            self.__get_stock_ltgd(day_list[i])
            print "=================%s" % day_list[i]['id']

    def show_shareholdernum(self):
        s_day = 20161115
        stock = self.mysql.getRecord("select * from s_stock_list where 1")
        stock_list = {}
        for o in range(0, len(stock)):
            tmp_code = stock[o]['s_code'].upper()
            stock_list[tmp_code] = stock[o]
        mklist = self.mysql.getRecord("SELECT * FROM  `s_stock_shareholdernum` WHERE enddate =%s ORDER BY askshrto ASC" % s_day)
        for a in range(0, len(mklist)):
            print mklist[a]['s_code']
            #print "%s==%s==%s" % (mklist[a]['s_code'], stock_list[mklist[a]['s_code'].upper()]['name'], mklist[a]['askshrto'])

    def get_xueqiu_shareholdernum(self):
        '''
        获取股东人数
        '''
        #self.__get_jqk_shareholdernum('sh603268')
        #self.__get_jqk_shareholdernum('sz002552')
        #self.__get_jqk_shareholdernum('sh601880')
        #sys.exit()
        #1435 海印股份
        i = 1
        _dd = 20160930
        step = 20161115
        while 1:
            if i > 2960:
                break
            _has = self.mysql.fetch_one("select * from s_stock_list where id=%s" % i)
            i += 1
            if _has is None or _has['status'] == 3:
                continue
            if _has['id'] < 1464:
                continue
            #self.__get_shareholdernum(_has['s_code'].upper())
            _where = "s_code='%s' and enddate>%s " % (_has['s_code'], _dd)
            _has2 = self.mysql.fetch_one("select * from  s_stock_shareholdernum where %s" % _where)
            if _has2 is None:
                print _has['s_code']
                continue
            if _has2['enddate'] == step:
                continue
            self.__get_jqk_shareholdernum(_has['s_code'])

            print "%s===%s" % (i, _has['s_code'])

    def __get_shareholdernum(self, s_code):
        #s_code = 'SH603268'
        self.curl_get('https://xueqiu.com/8205215793')
        url = "https://xueqiu.com/stock/f10/shareholdernum.json?symbol=%s&page=1&size=400&_=1476241363975" % s_code
        _data = self.curl_get(url)
        re = json.loads(_data)
        if re['list'] is None:
            return False
        for r in range(0, len(re['list'])):
            '''
            股东总户数、股东总户数较上期增减、A股股东户数、A股股东户数环比变化、A股平均每户持股、无限售A股股东户均持股数
            '''
            indata = {
                's_code': s_code.lower(),
                'enddate': re['list'][r]['enddate'],
                'totalshamt': re['list'][r]['totalshamt'],
                'totalshrto': re['list'][r]['totalshrto'],
                'askshamt': re['list'][r]['askshamt'],
                'askshrto': re['list'][r]['askshrto'],
                'askavgsh': re['list'][r]['askavgsh'],
                'afavgholdsum': re['list'][r]['afavgholdsum'],
            }
            print indata
            _where = "s_code='%s' and enddate=%s " % (s_code.lower(), re['list'][r]['enddate'])
            _has = self.mysql.fetch_one("select * from  s_stock_shareholdernum where %s" % _where)
            if _has is not None:
                self.mysql.dbUpdate('s_stock_shareholdernum', indata, _where)
            else:
                self.mysql.dbInsert('s_stock_shareholdernum', indata)

    def __get_jqk_shareholdernum(self, s_code):
        #变化
        url = "http://stockpage.10jqka.com.cn/%s/holder/" % s_code[2:]
        out_put = '/usr/bin/php /htdocs/quant/c.php %s' % base64.b64encode(url)
        _data = commands.getoutput(out_put)
        _vprint = self.sMatch('<div class="data_tbody">', '<div class="gdyj_15 holernumsort" style="display:none;">', _data, 0)
        if len(_vprint) == 0:
            return False
        #print _vprint
        #sys.exit()
        #是否是W基数
        _vw = self.sMatch('<th class="tl" style=\'padding:3px 2px\'>(.*?)\(', '\)<\/th>', _data, 0)
        #异常
        if len(_vw) == 0:
            return False
        _base = 1
        _gu_base = 1
        if _vw[0][1] == u'万户':
            _base = 10000
        #h股多一条行数据,不处理
        _hg = len(_vw)
        print _hg
        if _hg > 3:
            print 'H data'
            return False

        if _vw[1][1] == u'万股':
            _gu_base = 10000

        '''
        print _vw[0][1]
        print _vw[1][1]
        print _vw[2][1]
        print _vw[1][0]
        '''
        _vtr = self.sMatch('<tr>', '</tr>', _vprint[0], 0)
        _days = self.sMatch('<t(.*?)>', '</t(.*?)>', _vtr[0], 0)
        #股东人数
        _nums = self.sMatch('<t(.*?)>', '</t(.*?)>', _vtr[1], 0)
        #股东变化比
        _nums_p = self.sMatch('<t(.*?)>', '</t(.*?)>', _vtr[2], 0)
        _nums_gu = self.sMatch('<t(.*?)>', '</t(.*?)>', _vtr[3], 0)
        #_nums_gu_p = self.sMatch('<t(.*?)>', '</t(.*?)>', _vtr[4], 0)

        for k in range(0, len(_days)):
            _date = self.tools.strip_tags(_days[k][1])
            d = datetime.datetime.strptime(_date, "%Y-%m-%d")
            _dd = self.tools.d_date('%Y%m%d', time.mktime(d.timetuple()))

            _bh = self.tools.strip_tags(_nums_p[k][1])
            if _nums_p[k][1] == '-' or _nums_p[k][1] == u'未公布':
                _bh = 0
            else:
                _bh = _bh.replace('%', '')
            indata = {
                's_code': s_code,
                'enddate': _dd,
                #'totalshamt': self.tools.strip_tags(_nums[k][1]),
                #'totalshrto': 0,
                #'askshamt': self.tools.strip_tags(_nums[k][1]),
                'askshrto': _bh,
                #'askavgsh': self.tools.strip_tags(_nums_gu[k][1]),
                #'afavgholdsum': self.tools.strip_tags(_nums_gu[k][1]),
            }
            _where = "s_code='%s' and enddate=%s " % (s_code, _dd)
            _has = self.mysql.fetch_one("select * from  s_stock_shareholdernum where %s" % _where)

            _rs = self.tools.strip_tags(_nums[k][1])
            if _rs == u'未公布':
                _rs = 0
            else:
                if _base > 1:
                    if _rs.find('.') == -1:
                        _rs = int(_rs)
                    else:
                        _rs = float(_rs)
                else:
                    _rs = int(_rs)
                _rs = int(_rs * _base)

            #持股数
            _rgs = self.tools.strip_tags(_nums_gu[k][1])
            if _rgs == u'未公布':
                _rgs = 0
            else:
                if _rgs == '-':
                    _rgs = 0
                else:
                    if _rgs.find('.') == -1:
                        _rgs = int(_rgs)
                    else:
                        _rgs = float(_rgs)
                _rgs = int(_rgs * _gu_base)

            if _has is not None:
                #有可能xq没有数据
                if _has['afavgholdsum'] == 0:
                    indata['askavgsh'] = _rgs
                    indata['afavgholdsum'] = _rgs
                self.mysql.dbUpdate('s_stock_shareholdernum', indata, _where)
            else:
                indata['totalshamt'] = _rs
                indata['askshamt'] = _rs
                indata['totalshrto'] = 0
                indata['askavgsh'] = _rgs
                indata['afavgholdsum'] = _rgs
                indata['last_update'] = self.tools.d_date('%Y%m%d')
                self.mysql.dbInsert('s_stock_shareholdernum', indata)
            print indata
