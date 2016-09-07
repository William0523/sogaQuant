# -*- coding: utf-8 -*-
import sys
import time
import datetime
import logging
from quant.core.Spider import *


class LhbDataSpider(SpiderEngine):
    '''
    更新营业部数据
    '''
    def __init__(self):
        SpiderEngine.__init__(self)
        self.today = sys.argv[2]

    def get_info(self, yyb_id):
        self.get_daily_detail(yyb_id, sys.argv[2])

    def run(self):
        print sys.argv
        self.tools.setup_logging(sys.argv[1], True, True)
        '''
        day_list = self.mysql.getRecord("SELECT * FROM s_lhb WHERE 1")
        for i in range(520, len(day_list)):
            print "====%s" % i
            if day_list[i]['codex'] == 8888:
                continue
            print day_list[i]['id']
            self.get_yyb_last_dateline(day_list[i]['codex'])
            time.sleep(1)

        sys.exit()
        return True
        '''
        #self.get_city_yyb()
        #sys.exit()

        logging.debug('Start Daily Lhb=====Days:%s ' % sys.argv[2])
        self.daily_lhb(sys.argv[2])

        logging.debug('Start Daily Lhb=====Detail:%s ' % sys.argv[2])
        day_list = self.mysql.getRecord("SELECT * FROM s_lhb_days WHERE status=0 and dateline=%s" % sys.argv[2])
        for i in range(0, len(day_list)):
            self.get_daily_detail(day_list[i], sys.argv[2])
            #time.sleep(5)

        logging.debug('Start Daily Lhb=====LhbCount:%s ' % sys.argv[2])
        self.count_detail(sys.argv[2])

        #self.get_city_yyb()
        return True

        '''
        logging.debug('Start Daily Lhb=====New YYB:%s ' % sys.argv[2])
        sql_data = "select yyb_id from s_lhb_days_detail where 1 group by yyb_id"
        tmpdf = pandas.read_sql(sql_data, self.mysql.db)

        has_yyb = pandas.read_sql("select * from s_lhb where name!=''", self.mysql.db)
        has = []
        for code in has_yyb.values:
            has.append(code[1])
        #print tmpdf
        #print has
        #sys.exit()

        for xd in tmpdf.values:

            if xd[0] == 0 or xd[0] == 8888:
                continue
            print xd[0]
            sys.exit()
            if xd[0] not in has:
                self.get_yyb_data(xd[0])
        '''
    def get_yyb_last_dateline(self, ds):
        #ds = 80135988
        url = "http://data.eastmoney.com/DataCenter_V3/stock2016/jymx.ashx?pagesize=50&page=1&js=var+fguIHta&param=&sortRule=-1&sortType=&gpfw=0&code=%s&rt=24462227" % ds
        a = self.sGet(url)
        a = a.replace("var fguIHta=", "")
        re = json.loads(a)
        if len(re['data']) > 0:
            for k in range(0, len(re['data'])):
                _tmp = re['data'][k]
                last_date = int(_tmp['TDate'].replace('-', ''))
                self.mysql.dbUpdate('s_lhb', {'last_dateline': last_date}, "codex=%s" % ds)
                print last_date
                break
                #print last_date

                #sys.exit()

    def get_city_yyb(self):
        url = "http://data.eastmoney.com/stock/yybcx.html"
        _data = self.sGet(url)
        _urls = self.sMatch('href="/Stock/lhb/city/', '\.html"', _data, 0)
        for x in xrange(0, len(_urls)):
            #_urls[x] = 440000
            detail = "http://data.eastmoney.com/DataCenter_V3/stock2016/yybSearch.ashx?pagesize=1000&page=1&js=var+fguIHta&param=&sortRule=-1&sortType=UpCount&city=%s&typeCode=2&gpfw=0&code=%s&rt=24462162" % (_urls[x], _urls[x])
            a = self.sGet(detail)
            a = a.replace("var fguIHta=", "")
            re = json.loads(a)
            for k in range(0, len(re['data'])):
                _tmp = re['data'][k]

                indata = {
                    'province': _tmp['Province'],
                    'codex': _tmp['SalesCode'],
                    'name': _tmp['SalesName'],
                    'SumActMoney': _tmp['SumActMoney'],
                    'SumActBMoney': _tmp['SumActBMoney'],
                    'SumActSMoney': _tmp['SumActSMoney'],
                    'UpCount': _tmp['UpCount'],
                    'BCount': _tmp['BCount'],
                    'SCount': _tmp['SCount']
                }
                print indata
                _has = self.mysql.fetch_one("select * from  s_lhb where codex=%s" % _tmp['SalesCode'])
                _where = "codex=%s" % _tmp['SalesCode']

                if _has is not None:
                    self.mysql.dbUpdate('s_lhb', indata, _where)
                else:
                    self.mysql.dbInsert('s_lhb', indata)

    def get_daily_detail(self, s_data, dateline):
        s_code = s_data['s_code']
        d = datetime.datetime.strptime(dateline, "%Y%m%d")
        days = self.tools.d_date('%Y-%m-%d', time.mktime(d.timetuple()))
        url = "http://data.eastmoney.com/stock/lhb,%s,%s.html" % (days, s_code[2:10])
        #url = "http://data.eastmoney.com/stock/lhb,2016-03-30,000534.html"
        logging.debug('Detail=====:%s ' % url)
        _data = self.sGet(url)

        _tr = self.sMatch(u'成交量：', u"成交金额：", _data, 1)
        _td = self.sMatch(u'涨跌幅：', u'成交量：', _data, 1)

        if _tr is None:
            self.tools.sWrite(_data, log_file)
            self.mysql.dbUpdate('s_lhb_days', {'status': 1}, "id=%s" % s_data['id'])
            return 1
        if _tr is not None:
            _hands = _tr[0].replace(u'万股', '')
            _hands = _hands.replace('&nbsp;', '')
            _hands = float(_hands)*10000
            ud = 0
            if _tr is not None:
                if len(_td) > 0:
                    ud = _td[0].replace(u'万股', '')
                    ud = ud.replace('&nbsp;', '')
            code = s_code.lower()
            indata = {
                'v_hands': _hands,
                'ud': ud,
                'status': 1
            }
            _where = " dateline=%s AND s_code='%s'" % (dateline, code)
            self.mysql.dbUpdate('s_lhb_days', indata, _where)

            _tables_1 = self.sMatch('<table cellpadding="0" cellspacing="0" class="default_tab stock-detail-tab" id="tab-2">', '<\/table>', _data, 0)
            _xx_1 = self.sMatch('<tr(.*?)>', "<\/tr>", _tables_1[0], 0)

            self.__format_datail(dateline, s_code, _xx_1, 'B', 2, 1)
            self.__format_datail(dateline, s_code, _xx_1, 'B', 3, 2)
            self.__format_datail(dateline, s_code, _xx_1, 'B', 4, 3)
            self.__format_datail(dateline, s_code, _xx_1, 'B', 5, 4)
            self.__format_datail(dateline, s_code, _xx_1, 'B', 6, 5)

            _tables_2 = self.sMatch('<table cellpadding="0" cellspacing="0" class="default_tab tab-2" id="tab-4">', '<\/table>', _data, 0)
            _xx_2 = self.sMatch('<tr(.*?)>', "<\/tr>", _tables_2[0], 0)

            self.__format_datail(dateline, s_code, _xx_2, 'S', 2, 1)
            self.__format_datail(dateline, s_code, _xx_2, 'S', 3, 2)
            self.__format_datail(dateline, s_code, _xx_2, 'S', 4, 3)
            self.__format_datail(dateline, s_code, _xx_2, 'S', 5, 4)
            self.__format_datail(dateline, s_code, _xx_2, 'S', 6, 5)

    def __format_datail(self, dateline, code, html, o_type='B', i=0, sort=1):
        yyb_id = 8888
        if len(html) <= i:
            return False
        tdhtml = self.sMatch('<td(.*?)>', "<\/td>", html[i][1], 0)
        if tdhtml[1][1].find(u'机构专用') < 0:
            _c1 = self.sMatch('<a href="\/stock\/lhb\/yyb\/(.*?)\.html">', '<\/a>', tdhtml[1][1], 0)
            if len(_c1) == 0:
                return False
            yyb_id = _c1[0][0]

        _B = self.tools.strip_tags(tdhtml[2][1])
        if _B == '-':
            _B = 0

        _S = self.tools.strip_tags(tdhtml[4][1])
        if _S == '-':
            _S = 0
        _in_data = {
            'type': o_type,
            'dateline': dateline,
            's_code': code,
            'yyb_id': yyb_id,
            'B_volume': float(_B) * 10000,
            'B_p': self.tools.strip_tags(tdhtml[3][1]),
            'S_volume': float(_S) * 10000,
            'S_p': self.tools.strip_tags(tdhtml[5][1]),
            's_sort': sort
        }
        print _in_data
        self.mysql.dbInsert('s_lhb_days_detail', _in_data)
        return _in_data

    def daily_lhb(self, dateline):
        #每日上榜
        d = datetime.datetime.strptime(dateline, "%Y%m%d")
        days = self.tools.d_date('%Y-%m-%d', time.mktime(d.timetuple()))
        #url = "http://data.eastmoney.com/stock/lhb/%s.html" % days
        #url = 'http://data.eastmoney.com/stock/tradedetail.html'
        #url = 'http://data.eastmoney.com/DataCenter_V3/stock2016/TradeDetail/pagesize=200,page=1,sortRule=-1,sortType=,startDate=%s,endDate=%s,gpfw=0,js=var data_tab_1.html' % (days, days)
        #url = 'http://data.eastmoney.com/'
        url = 'http://data.10jqka.com.cn/market/longhu/date/%s/' % days
        logging.debug('GetUrl:%s ' % url)

        _data = self.sGet(url)
        _tr = self.sMatch('<td class="tip-trigger" code="(.*?)">', '<\/td>', _data, 0)
        _vprint = self.sMatch('<td class=" c_eq">', '<\/td>', _data, 0)
        #print _tr
        #sys.exit()
        for i in range(0, len(_tr)):

            s_code = _tr[i][0]
            _prx = s_code[0:1]
            if int(_prx) not in [0, 3, 6]:
                print "what"
                continue
            if int(_prx) == 0 or int(_prx) == 3:
                s_code = 'sz%s' % s_code
            else:
                s_code = 'sh%s' % s_code

            '''
            _has = self.mysql.fetch_one("select * from  s_lhb_days where s_code='%s' and dateline=%s" % (s_code, dateline))
            if _has is None:
                pass
            else:
                up = {'volume': float(_vprint[i]) * 10000}
                self.mysql.dbUpdate('s_lhb_days', up, "id=%s" % _has['id'])
            '''

            indata = {
                'dateline': dateline,
                's_code': s_code,
                'v_hands': 0,
                'ud': 0,
                'volume': float(_vprint[i]) * 10000,
                's_reason': ''
            }
            print indata
            _has = self.mysql.fetch_one("select * from  s_lhb_days where s_code='%s' and dateline=%s" % (indata['s_code'], dateline))
            if _has is None:
                self.mysql.dbInsert('s_lhb_days', indata)

    def count_detail(self, dateline):
        #print self.args
        #dateline = self.args[2]
        x = self.mysql.getRecord("SELECT * FROM  `s_lhb_days_detail` WHERE  `dateline` =%s" % dateline)
        _data = {}
        xhash = []
        for k in range(0, len(x)):
            if x[k]['yyb_id'] == 0 or x[k]['yyb_id'] == 8888:
                continue

            if x[k]['yyb_id'] not in _data:
                _data[x[k]['yyb_id']] = {'B': 0, 'S': 0}

            word = "%s%s%s" % (x[k]['s_code'], x[k]['B_volume'], x[k]['S_volume'])
            _md5 = hashlib.md5(word).hexdigest()
            if _md5 in xhash:
                continue

            xhash.append(_md5)
            _data[x[k]['yyb_id']]['B'] += x[k]['B_volume']
            _data[x[k]['yyb_id']]['S'] += x[k]['S_volume']

        if len(_data) > 0:
            for k, v in _data.items():
                if v['B'] == 0 and v['S'] == 0:
                    continue
                indata = {
                    'yyb_id': k,
                    'B': v['B'],
                    'S': v['S'],
                    'dateline': dateline
                }
                self.mysql.dbInsert('s_lhb_daily', indata)
