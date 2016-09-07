# -*- coding: utf-8 -*-
import hashlib
import re
import commands
import memcache
#from quant.core.Stats import *
from datetime import date, datetime
from quant.core.Spider import *
black_list = ['sh600000', 'sh600010', 'sh600011', 'sh600015', 'sh600016', 'sh600018', 'sh600019', 'sh600028', 'sh600029', 'sh600030', 'sh600031', 'sh600036', 'sh600048', 'sh600050', 'sh600066', 'sh600085', 'sh600091', 'sh600104', 'sh600111', 'sh600115', 'sh600145', 'sh600179', 'sh600212', 'sh600221', 'sh600230', 'sh600234', 'sh600247', 'sh600265', 'sh600271', 'sh600276', 'sh600301', 'sh600306', 'sh600309', 'sh600316', 'sh600319', 'sh600339', 'sh600340', 'sh600346', 'sh600375', 'sh600381', 'sh600383', 'sh600390', 'sh600432', 'sh600489', 'sh600518', 'sh600519', 'sh600520', 'sh600535', 'sh600539', 'sh600546', 'sh600547', 'sh600581', 'sh600585', 'sh600603', 'sh600608', 'sh600637', 'sh600649', 'sh600663', 'sh600675', 'sh600690', 'sh600701', 'sh600703', 'sh600705', 'sh600710', 'sh600721', 'sh600725', 'sh600732', 'sh600741', 'sh600760', 'sh600793', 'sh600795', 'sh600806', 'sh600817', 'sh600837', 'sh600866', 'sh600886', 'sh600887', 'sh600893', 'sh600900', 'sh600958', 'sh600999', 'sh601006', 'sh601009', 'sh601018', 'sh601021', 'sh601088', 'sh601111', 'sh601166', 'sh601169', 'sh601186', 'sh601211', 'sh601238', 'sh601288', 'sh601318', 'sh601328', 'sh601336', 'sh601377', 'sh601390', 'sh601398', 'sh601600', 'sh601601', 'sh601618', 'sh601628', 'sh601633', 'sh601668', 'sh601669', 'sh601688', 'sh601727', 'sh601766', 'sh601788', 'sh601800', 'sh601818', 'sh601857', 'sh601888', 'sh601898', 'sh601899', 'sh601901', 'sh601918', 'sh601939', 'sh601988', 'sh601989', 'sh601991', 'sh601998', 'sh603885', 'sh603993', 'sz000001', 'sz000002', 'sz000033', 'sz000037', 'sz000046', 'sz000063', 'sz000069', 'sz000155', 'sz000166', 'sz000333', 'sz000403', 'sz000408', 'sz000504', 'sz000505', 'sz000511', 'sz000538', 'sz000568', 'sz000594', 'sz000606', 'sz000611', 'sz000617', 'sz000622', 'sz000625', 'sz000629', 'sz000633', 'sz000651', 'sz000670', 'sz000691', 'sz000693', 'sz000717', 'sz000725', 'sz000768', 'sz000776', 'sz000783', 'sz000831', 'sz000856', 'sz000858', 'sz000895', 'sz000913', 'sz000933', 'sz000950', 'sz000962', 'sz000968', 'sz000995', 'sz002024', 'sz002061', 'sz002069', 'sz002109', 'sz002142', 'sz002173', 'sz002199', 'sz002252', 'sz002289', 'sz002304', 'sz002336', 'sz002379', 'sz002415', 'sz002423', 'sz002450', 'sz002513', 'sz002594', 'sz002608', 'sz002673', 'sz300059', 'sz300104', 'sz300372', 'sh600919', 'sz300529', 'sz300526']


class Pankou(SpiderEngine):
    '''
    实时盘口统计
    '''
    def __init__(self):
        SpiderEngine.__init__(self)

    def __filter_code(self, stock):
        #过滤代码,符合条件返回true
        '''
        流通盘>400亿
        名称中有N|ST|航空|银行
        上市日期
        不是涨停或跌停
        '''
        res = False
        ms = re.findall(re.compile(r'\*|N|ST|航空|银行'), stock['name'])
        t = time.time()-60*86400
        listing_date = self.tools.d_date('%Y%m%d', t)
        #listing_date = 20151230
        if stock['run_market'] > 40000000000 or stock['listing_date'] == 0 or stock['listing_date'] > listing_date or ms:
            res = True
        _top = float(stock['last_close']) * 1.1
        _bottom = float(stock['last_close']) * 0.9

        #涨跌停一定有大单,跳转
        if (float(stock['close']) == round(_top, 2)) or (float(stock['close']) == round(_bottom, 2)):
            res = True
        return res

    def replay(self):
        today = self.tools.d_date('%Y%m%d')
        self.today = sys.argv[2]
        ax = self.mysql.getRecord("select s_code,code,close,last_close,listing_date,run_market,name,chg from s_stock_list where dateline=%s" % self.today)
        self._filter_code = []
        self._list_code = {}
        for i in range(0, len(ax)):
            if self.__filter_code(ax[i]):
                self._filter_code.append(ax[i]['s_code'])
                continue

            self._list_code[ax[i]['s_code']] = ax[i]

        aid = 0
        while 1:
            sql_data = "select * from s_stock_runtime_%s where id > %s limit 500" % (today, aid)
            #sql_data = "select * from s_stock_runtime_20160624 where s_code='sz002040' "
            tmpdf = self.mysql.getRecord(sql_data)

            if len(tmpdf) == 0:
                break

            for k in range(0, len(tmpdf)):
                aid = tmpdf[k]['id']
                if tmpdf[k]['s_code'] not in self._list_code.keys():
                    continue
                if float(self._list_code[tmpdf[k]['s_code']]['chg']) > 9.7:
                    continue
                self.__fupan_order(tmpdf[k])

            print aid

    def realtime(self):
        #实时统计,大于1300W排单
        #python realtime.py realtime_pankou 2
        today = self.tools.d_date('%Y%m%d')
        #today = 20160629
        maxnums = 0
        ac = []
        while True:
            ax = self.mysql.getRecord("select * from s_stock_runtime_snap where dateline=%s order by id ASC" % today)

            for i in range(maxnums, len(ax)):
                #判断是否已经涨停或跌停不再提示
                stock = self.mysql.fetch_one("select s_code,code,close,last_close,listing_date,run_market,name,chg from s_stock_list where s_code='%s'" % ax[i]['s_code'])
                #if stock['s_code'] != 'sz002695':
                #    continue
                if self.__filter_code(stock):
                    continue
                if ax[i]['s_code'] in ac:
                    continue
                ac.append(ax[i]['s_code'])
                print "%s===%s|%s===B%s====S%s" % (ax[i]['date_str'], ax[i]['s_code'], stock['name'], ax[i]['b_amount'], ax[i]['s_amount'])
            maxnums = len(ax)
            time.sleep(2)

    def daily_open(self):
        # 收盘价写memcache python realtime.py daily_open 2
        today = self.tools.d_date('%Y%m%d')
        mcache = memcache.Client(['127.0.0.1:11211'])
        #today = 20160725
        #_hash2 = "%s==black_list" % today

        ax = self.mysql.getRecord("select * from s_stock_list where dateline=%s " % today)
        #ax = self.mysql.getRecord("select * from s_stock_list where 1")
        black_list = {}
        #ac = []
        for i in range(0, len(ax)):
            ms = re.findall(re.compile(r'\*|N|ST|航空|银行'), ax[i]['name'])
            if ax[i]['run_market'] > 40000000000 or ax[i]['listing_date'] == 0 or ms:
                black_list[ax[i]['s_code']] = 1
                #ac.append(ax[i]['s_code'])

            _hash = "%s==%s" % (today, ax[i]['s_code'])
            has = mcache.get(_hash)
            if has:
                #mcache.delete(_hash)
                continue
            #sys.exit()
            d = {'open': ax[i]['open'], 'last_close': ax[i]['last_close'], 's_code': ax[i]['s_code']}
            mcache.set(_hash, d, 66400)
        #print ac
        #mcache.set(_hash2, black_list, 66400)

    def save_q(self):
        #from qq
        vid = int(sys.argv[2])
        out_put = '/usr/bin/php /htdocs/quant/soga/stock.php %s' % vid
        _data = commands.getoutput(out_put)

        URL = 'http://qt.gtimg.cn/0.6473073084700043q=%s' % _data
        #URL = 'http://qt.gtimg.cn/0.6473073084700043q=sh600083'
        self.mcache = memcache.Client(['127.0.0.1:11211'])

        is_opening = self.is_opening()
        if is_opening is False:
            print "GetFiveHandicap===Market Close===%s===;" % self.tools.d_date('%H%M%S')
            #time.sleep(30)
            #return False
        self.today = self.tools.d_date('%Y%m%d')
        data = self.sGet(URL, 'utf-8')
        ms = re.findall(re.compile(r';'), data)
        if len(data) < 50 or ms is None:
            print "=========================内容没有找到......"

        data = data.split(';')
        l = len(data)
        for i in range(0, l):
            res = data[i].split('~')
            if len(res) < 10:
                continue
            cc = res[0].split('=')
            xcode = cc[0].replace('v_', '').replace("\n", "")
            print res
            #sys.exit()
            #没有交易记录

            if int(res[6]) == 0:
                continue

            item = {}
            item['dateline'] = datetime.strftime(date.today(), "%Y%m%d")
            #item['dateline'] = '20160805'
            item['s_code'] = xcode
            item['all_volume'] = str(int(res[36])*100)
            item['all_money'] = str(int(res[37])*10000)

            item['B_1_price'] = res[9]
            item['B_1_volume'] = res[10]
            item['B_2_price'] = res[11]
            item['B_2_volume'] = res[12]
            item['B_3_price'] = res[13]
            item['B_3_volume'] = res[14]
            item['B_4_price'] = res[15]
            item['B_4_volume'] = res[16]
            item['B_5_price'] = res[17]
            item['B_5_volume'] = res[18]
            item['S_1_price'] = res[19]
            item['S_1_volume'] = res[20]
            item['S_2_price'] = res[21]
            item['S_2_volume'] = res[22]
            item['S_3_price'] = res[23]
            item['S_3_volume'] = res[24]
            item['S_4_price'] = res[25]
            item['S_4_volume'] = res[26]
            item['S_5_price'] = res[27]
            item['S_5_volume'] = res[28]

            mm = res[29].split('/')
            item['min_sec'] = mm[0].replace(':', '')
            item['date_str'] = "%s %s" % (datetime.strftime(date.today(), "%Y-%m-%d"), mm[0])
            #print item
            #sys.exit()
            a = item.values()
            word = '-'.join(a)
            _hash = hashlib.md5(word).hexdigest()
            #print _hash
            #sys.exit()
            has = self.mcache.get(_hash)
            if has:
                print "memcache------hit"
                continue

            table = 's_stock_runtime_%s' % item['dateline']
            aitem = self.mysql.dbInsert(table, item)
            item['id'] = aitem['LAST_INSERT_ID()']

            self.mcache.set(_hash, 1, 66400)
            #过滤
            if xcode in black_list:
                continue
            #today_black_list = self.mcache.get("%s==black_list" % item['dateline'])
            #if item['s_code'] in today_black_list.keys():
                #print item['s_code']
            #    continue

            _t_hash = "%s==%s" % (self.today, item['s_code'])
            #print _t_hash
            #sys.exit()
            stock = self.mcache.get(_t_hash)
            if stock:
                _top = float(stock['last_close']) * 1.1
                _bottom = float(stock['last_close']) * 0.9
                if (float(item['B_1_price']) == round(_top, 2)) or (float(item['S_1_price']) == round(_bottom, 2)):
                    return False
                else:
                    self.__real_order(item)

    def save(self):
        #5档盘口保存
        vid = int(sys.argv[2])
        out_put = '/usr/bin/php /htdocs/quant/soga/stock.php %s' % vid
        _data = commands.getoutput(out_put)

        URL = 'http://hq.sinajs.cn/?func=getData._hq_cron();&list=%s' % _data
        #URL = 'http://hq.sinajs.cn/?func=getData._hq_cron();&list=sh600749'

        self.mcache = memcache.Client(['127.0.0.1:11211'])

        is_opening = self.is_opening()
        if is_opening is False:
            print "GetFiveHandicap===Market Close===%s===;" % self.tools.d_date('%H%M%S')
            time.sleep(30)
            return False

        self.today = self.tools.d_date('%Y%m%d')
        data = self.sGet(URL, 'utf-8')
        ms = re.findall(re.compile(r'";'), data)
        if len(data) < 50 or ms is None:
            print "=========================内容没有找到......"

        data = data.split('";')
        print data
        l = len(data)
        for i in range(0, l-1):
            if len(data[i]) < 30:
                continue
            res = data[i].split(',')

            if int(res[8]) == 0:
                continue

            _tmp = res[0].split('=')
            xcode = _tmp[0].replace('var hq_str_', '')
            item = {}
            item['dateline'] = datetime.strftime(date.today(), "%Y%m%d")
            item['s_code'] = xcode.replace("\n", "")
            item['all_volume'] = res[8]
            item['all_money'] = res[9]
            item['B_1_price'] = res[11]
            item['B_1_volume'] = res[10]
            item['B_2_price'] = res[13]
            item['B_2_volume'] = res[12]
            item['B_3_price'] = res[15]
            item['B_3_volume'] = res[14]
            item['B_4_price'] = res[17]
            item['B_4_volume'] = res[16]
            item['B_5_price'] = res[19]
            item['B_5_volume'] = res[18]
            item['S_1_price'] = res[21]
            item['S_1_volume'] = res[20]
            item['S_2_price'] = res[23]
            item['S_2_volume'] = res[22]
            item['S_3_price'] = res[25]
            item['S_3_volume'] = res[24]
            item['S_4_price'] = res[27]
            item['S_4_volume'] = res[26]
            item['S_5_price'] = res[29]
            item['S_5_volume'] = res[28]

            item['min_sec'] = res[31].replace(':', '')
            item['date_str'] = "%s %s" % (res[30], res[31])
            a = item.values()
            word = '-'.join(a)
            _hash = hashlib.md5(word).hexdigest()
            has = self.mcache.get(_hash)

            if has:
                print "memcache------hit"
                continue

            table = 's_stock_runtime_%s' % item['dateline']
            aitem = self.mysql.dbInsert(table, item)
            item['id'] = aitem['LAST_INSERT_ID()']
            self.mcache.set(_hash, 1, 66400)
            #过滤
            if item['s_code'] in black_list:
                continue
            #today_black_list = self.mcache.get("%s==black_list" % item['dateline'])
            #if item['s_code'] in today_black_list.keys():
                #print item['s_code']
            #    continue

            _t_hash = "%s==%s" % (self.today, item['s_code'])
            stock = self.mcache.get(_t_hash)
            if stock:
                _top = float(stock['last_close']) * 1.1
                _bottom = float(stock['last_close']) * 0.9
                if (float(item['B_1_price']) == round(_top, 2)) or (float(item['S_1_price']) == round(_bottom, 2)):
                    return False
                else:
                    self.__real_order(item)

    def __real_order(self, x):
        #实时监控 监控1300W
        self.stem = 13000000
        self.table = 's_stock_runtime_snap'
        self.__filter_big_order(x)

    def __fupan_order(self, x):
        #盘后
        if x['s_code'] in self._filter_code:
            return False
        self.stem = 15000000
        self.table = 's_stock_runtime_snap_3'
        self.__filter_big_order(x)

    def __filter_big_order(self, x):
        #stem = 13000000
        b1_m = int(float(x['B_1_volume']) * float(x['B_1_price']))
        b2_m = int(float(x['B_2_volume']) * float(x['B_2_price']))
        b3_m = int(float(x['B_3_volume']) * float(x['B_3_price']))
        b4_m = int(float(x['B_4_volume']) * float(x['B_4_price']))
        b5_m = int(float(x['B_5_volume']) * float(x['B_5_price']))
        b_money = 0
        b1_money = b2_money = b3_money = b4_money = b5_money = 0
        if b1_m > self.stem:
            b1_money = b1_m
        if b2_m > self.stem:
            b2_money = b2_m
        if b3_m > self.stem:
            b3_money = b3_m
        if b4_m > self.stem:
            b4_money = b4_m
        if b5_m > self.stem:
            b5_money = b5_m

        b_money = max(b1_money, b2_money, b3_money, b4_money, b5_money)

        s1_m = int(float(x['S_1_volume']) * float(x['S_1_price']))
        s2_m = int(float(x['S_2_volume']) * float(x['S_2_price']))
        s3_m = int(float(x['S_3_volume']) * float(x['S_3_price']))
        s4_m = int(float(x['S_4_volume']) * float(x['S_4_price']))
        s5_m = int(float(x['S_5_volume']) * float(x['S_5_price']))
        s_money = 0
        s1_money = s2_money = s3_money = s4_money = s5_money = 0
        if s1_m > self.stem:
            s1_money = s1_m
        if s2_m > self.stem:
            s2_money = s2_m
        if s3_m > self.stem:
            s3_money = s3_m
        if s4_m > self.stem:
            s4_money = s4_m
        if s5_m > self.stem:
            s5_money = s5_m
        s_money = max(s1_money, s2_money, s3_money, s4_money, s5_money)
        d_str = str(x['date_str'])
        if b_money > 1 or s_money > 1:
            a = {
                'dateline': self.today,
                'date_str': x['date_str'],
                's_code': x['s_code'],
                'view_id': x['id'],
                'b_amount': int(b_money/10000),
                's_amount': int(s_money/10000)
            }
            word = d_str[0:-3]
            word = "%s=%s" % (word, x['s_code'])
            word = hashlib.md5(word).hexdigest()

            _has = self.mysql.fetch_one("select * from  %s where hash_str='%s'" % (self.table, word))
            if _has is None:
                a['hash_str'] = word
                self.mysql.dbInsert(self.table, a)
    '''
    def __filter_big_order(self, x):
        if x['s_code'] in self._filter_code:
            return False

        stem = 15000000
        b1_m = int(float(x['B_1_volume']) * float(x['B_1_price']))
        b2_m = int(float(x['B_2_volume']) * float(x['B_2_price']))
        b3_m = int(float(x['B_3_volume']) * float(x['B_3_price']))
        b4_m = int(float(x['B_4_volume']) * float(x['B_4_price']))
        b5_m = int(float(x['B_5_volume']) * float(x['B_5_price']))
        b_money = 0
        b1_money = b2_money = b3_money = b4_money = b5_money = 0
        if b1_m > stem:
            b1_money = b1_m
        if b2_m > stem:
            b2_money = b2_m
        if b3_m > stem:
            b3_money = b3_m
        if b4_m > stem:
            b4_money = b4_m
        if b5_m > stem:
            b5_money = b5_m

        b_money = max(b1_money, b2_money, b3_money, b4_money, b5_money)

        s1_m = int(float(x['S_1_volume']) * float(x['S_1_price']))
        s2_m = int(float(x['S_2_volume']) * float(x['S_2_price']))
        s3_m = int(float(x['S_3_volume']) * float(x['S_3_price']))
        s4_m = int(float(x['S_4_volume']) * float(x['S_4_price']))
        s5_m = int(float(x['S_5_volume']) * float(x['S_5_price']))
        s_money = 0
        s1_money = s2_money = s3_money = s4_money = s5_money = 0
        if s1_m > stem:
            s1_money = s1_m
        if s2_m > stem:
            s2_money = s2_m
        if s3_m > stem:
            s3_money = s3_m
        if s4_m > stem:
            s4_money = s4_m
        if s5_m > stem:
            s5_money = s5_m
        s_money = max(s1_money, s2_money, s3_money, s4_money, s5_money)
        d_str = str(x['date_str'])
        if b_money > 1 or s_money > 1:
            a = {
                'dateline': self.today,
                'date_str': x['date_str'],
                's_code': x['s_code'],
                'view_id': x['id'],
                'b_amount': int(b_money/10000),
                's_amount': int(s_money/10000)
            }
            #print a
            #return True
            #同一分钟买卖盘去重
            word = d_str[0:-3]
            word = "%s=%s" % (word, x['s_code'])
            word = hashlib.md5(word).hexdigest()

            _has = self.mysql.fetch_one("select * from  s_stock_runtime_snap_3 where hash_str='%s'" % word)
            if _has is None:
                a['hash_str'] = word
                self.mysql.dbInsert('s_stock_runtime_snap_3', a)
        '''
