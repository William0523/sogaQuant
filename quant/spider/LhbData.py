# -*- coding: utf-8 -*-
import sys
import time
import datetime
import logging
from quant.core.Spider import *
from pylsy import pylsytable


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
        #url = "http://data.eastmoney.com/stock/lhb,2016-09-13,300534.html"
        logging.debug('Detail=====:%s ' % url)
        _data = self.sGet(url)

        _tr = self.sMatch(u'成交量：', u"成交金额：", _data, 1)
        _td = self.sMatch(u'涨跌幅：', u'成交量：', _data, 1)
        #print _tr
        #sys.exit()
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
            #print indata
            #sys.exit()
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

        _data = self.sGet(url)
        #print _data
        logging.debug('GetUrl:%s  Length:%s' % (url, len(_data)))
        print len(_data)
        #sys.exit()
        _tr = self.sMatch('<td class="tip-trigger" code="(.*?)">', '<\/td>', _data, 0)
        _vprint = self.sMatch('<td class=" c_eq">', '<\/td>', _data, 0)
        print _tr
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

    def show_daily_lhb(self):
        s_code = self.change_scode(sys.argv[2])
        start_date = 20150610
        show_table_max = 4
        x = self.mysql.getRecord("SELECT a.*,b.codex,b.name FROM  `s_lhb_days_detail` as a left join s_lhb as b on a.yyb_id=b.codex WHERE  a.s_code='%s' and dateline >=%s order by dateline desc " % (s_code, start_date))
        if len(x):
            #先按天归类
            daily_yyb = {}
            daily_yyb2 = {}
            attributes = ["ID", "Name", "Num"]
            table_attr = ["Name"]

            tdheader = ['<tr height="40"><th width="60" bgColor="#CCCCCC" align="center">ID</th>', '<th width="100" bgColor="#CCCCCC" align="center">Name</th>', '<th width="30" bgColor="#CCCCCC" align="center">Num</th>']
            for a in range(0, len(x)):
                key = "D%s" % x[a]['dateline']
                if key not in attributes:
                    attributes.append(key)
                    tdheader.append('<th width="120" bgColor="#CCCCCC" align="center">%s</th>' % key)
                    if len(table_attr) < show_table_max:
                        table_attr.append(key)

                if x[a]['name'] is None:
                    x[a]['name'] = "YYB-%s" % x[a]['yyb_id']
                if x[a]['name'] not in daily_yyb.keys():
                    daily_yyb[x[a]['name']] = []

                daily_yyb[x[a]['name']].append(x[a])
                _name_key = "D%s|%s" % (x[a]['dateline'], x[a]['name'])
                if _name_key not in daily_yyb2.keys():
                    daily_yyb2[_name_key] = []
                daily_yyb2[_name_key].append(x[a])

            tdheader.append('</tr>')

            name_list = []
            table_td_list = {}
            show_table = pylsytable(table_attr)
            #

            #print "====".join(tdheader)
            #sys.exit()
            all_tr_list = []
            for _name, v in daily_yyb.items():
                #制作每一行数据
                _name2 = _name.replace('证券股份有限公司', '')
                _name2 = _name2.replace('证券有限公司', '')
                _name2 = _name2.replace('证券有限责任公司', '')
                _name2 = _name2.replace('证券营业部', '')

                #显示最近5次有龙虎榜
                name_list.append(_name2)
                for d in range(1, len(table_attr)):
                    _name_key = "%s|%s" % (table_attr[d], _name)
                    #if _name_key in daily_yyb2:
                        #tr_data.append(12)
                    if table_attr[d] not in table_td_list.keys():
                        table_td_list[table_attr[d]] = []

                    BS_str = ''
                    if _name_key in daily_yyb2:
                        BS_strA = "%s%s=%s=%s" % (daily_yyb2[_name_key][0]['type'], daily_yyb2[_name_key][0]['s_sort'], self._format_money(daily_yyb2[_name_key][0]['B_volume']), self._format_money(daily_yyb2[_name_key][0]['S_volume']))
                        BS_strB = ''
                        if len(daily_yyb2[_name_key]) > 1:
                            BS_strB = "%s%s=%s=%s" % (daily_yyb2[_name_key][1]['type'], daily_yyb2[_name_key][1]['s_sort'], self._format_money(daily_yyb2[_name_key][1]['B_volume']), self._format_money(daily_yyb2[_name_key][1]['S_volume']))
                        BS_str = u"%s=%s==" % (BS_strA, BS_strB)
                    table_td_list[table_attr[d]].append(BS_str)

                tr_data = ["%s" % v[0]['yyb_id'], _name2, 0]
                #周期内出现的天数
                _has_num = 0
                for b in range(3, len(attributes)):
                    _name_key = "%s|%s" % (attributes[b], _name)

                    BS_str = ''
                    if _name_key in daily_yyb2:
                        _has_num += 1
                        #营业部做T
                        BS_strA = "%s%s=%s=%s" % (daily_yyb2[_name_key][0]['type'], daily_yyb2[_name_key][0]['s_sort'], self._format_money(daily_yyb2[_name_key][0]['B_volume']), self._format_money(daily_yyb2[_name_key][0]['S_volume']))
                        BS_strB = ''
                        if len(daily_yyb2[_name_key]) > 1:
                            BS_strB = "%s%s=%s=%s" % (daily_yyb2[_name_key][1]['type'], daily_yyb2[_name_key][1]['s_sort'], self._format_money(daily_yyb2[_name_key][1]['B_volume']), self._format_money(daily_yyb2[_name_key][1]['S_volume']))
                        BS_str = u"%s\n%s==\n" % (BS_strA, BS_strB)
                    tr_data.append(BS_str)
                tr_data[2] = _has_num

                _td_str = ['<tr height="40">']
                for c in range(0, len(tr_data)):
                    _td_str.append('<td bgColor="#FFFFFF" align="center">%s</td>' % tr_data[c])
                _td_str.append('</tr>')

                all_tr_list.append("\n".join(_td_str))

        if len(sys.argv) == 3:
            table = '<html><head><meta http-equiv="Content-Type" content="text/html; charset=UTF-8" /><title></title></head><body>\n\
                <table border="1" width="1024">%s\n%s</table></body></html>' % ("\n".join(tdheader), "\n".join(all_tr_list))
            print table
        else:
            for b in range(1, len(table_attr)):
                show_table.add_data(table_attr[b], table_td_list[table_attr[b]])
            show_table.add_data("Name", name_list)
            print(show_table.__str__())

    def _format_money(self, m):
        res = 0
        if m > 0:
            res = m/10000
            res = '%.2f' % res
        return res
