# -*- coding: utf-8 -*-
import sys
import os
import logging
import json
import random
import datetime
import httplib
import urllib
from quant.core.Spider import *


class TouTiaoSpider(SpiderEngine):
    '''
    分钟数据下载
    '''
    def __init__(self):
        SpiderEngine.__init__(self)
        quncms_db = self.config['mysql']['quncms']
        self.qundb = sMysql(quncms_db['host'], quncms_db['user'], quncms_db['password'], quncms_db['dbname'])

    def get_info(self, s_code):
        pass
        #self.get_minute_from_qq(s_code)
        #self.get_minute_from_sina(s_code)

    def run_list(self):
        #头条列表
        print sys.argv
        self.tools.setup_logging(sys.argv[1], True, True)
        #_data = self.sGet('http://www.toutiao.com/api/article/recent/?source=2&count=20&category=video&max_behot_time=1470447756&utm_source=toutiao&offset=0&as=A185D70AF5B760A&cp=57A5D716505A2E1&max_create_time=1463846778&_=1470461440433')
        _data = self.sGet('http://www.toutiao.com/api/article/feed/?category=video&utm_source=toutiao&max_behot_time=1473424477&max_behot_time_tmp=1473424477&as=A1E5F7EDF3942FB&cp=57D304E21F1BAE1')
        #print _data
        #sys.exit()
        re = json.loads(_data)
        quncms_db2 = self.config['mysql']['quncms2']
        self.qundb_online = sMysql(quncms_db2['host'], quncms_db2['user'], quncms_db2['password'], quncms_db2['dbname'])
        for i in range(0, len(re['data'])):
            if 'media_url' not in re['data'][i].keys():
                continue
            if 'ad' in re['data'][i].keys():
                continue
            item = {}
            uid = re['data'][i]['media_url'].replace('http://toutiao.com/m', '')
            item['user_id'] = uid.replace('/', '')
            item['title'] = re['data'][i]['title']
            #item['tag'] = re['data'][i]['tag']
            item['tag'] = 'video_funny'
            item['image_url'] = re['data'][i]['image_url']
            #item['item_seo_url'] = re['data'][i]['item_seo_url']
            item['item_seo_url'] = str(re['data'][i]['group_id'])
            item['v_id'] = str(re['data'][i]['group_id'])
            item['item_id'] = str(re['data'][i]['group_id'])
            item['keywords'] = ''
            item['video_play_count'] = 10
            item['external_visit_count'] = 20
            item['digg_count'] = 12
            item['create_time'] = time.time()
            item['video_id'] = ''
            item['video_url'] = ''
            #print item
            #sys.exit()
            _has = self.qundb_online.fetch_one("select * from  video_contents where item_id='%s' OR v_id =%s" % (item['item_id'], item['v_id']))
            print item
            if _has is None:
                logging.debug('Done=====:%s=====%s ' % (item['title'], item['item_seo_url']))
                self.qundb_online.dbInsert('video_contents', item)

            vauthor = {}
            vauthor['media_name'] = re['data'][i]['source']
            vauthor['media_url'] = re['data'][i]['media_url']
            vauthor['middle_image'] = ''

            _has = self.qundb_online.fetch_one("select * from video_author where media_url='%s'" % vauthor['media_url'])
            #print indata
            if _has is None:
                self.qundb_online.dbInsert('video_author', vauthor)
            #sys.exit()

    def run_post(self):
        #定时发布一次80
        #i = 80489
        data = self.qundb.fetch_one("select * from video_contents where is_done=0 and source_site=1 order by itemid asc limit 1")
        if data is None:
            print "done..."
            return False
        i = data['itemid']
        j = 1
        while 1:
        #print i
        #sys.exit()
            if j > 1000:
                break
                #data = self.qundb.fetch_one("select * from video_contents where is_done=0 and source_site=1")
                #print "select * from video_contents where itemid >%s limit 1" % i
            #sys.exit()
            data = self.qundb.fetch_one("select * from video_contents where itemid ='%s'" % i)
            print "select * from video_contents where itemid ='%s'" % i
            #print data
            i += 1
            if data is None:
                print 'not found'
                continue
            #sys.exit()
            if data['is_done'] == 1:
                continue
            if data['source_site'] == 2:
                continue
    #       print data
            #sys.exit()
            if data['item_seo_url'].index('item'):
                idx = data['item_seo_url'].replace('http://toutiao.com/item/', '')
                idx = data['item_seo_url'].replace('/item/', '')

                url = "http://www.toutiao.com/i%s" % idx
            else:
                url = "http://www.toutiao.com%s" % data['item_seo_url']
                #
                print url
     #           sys.exit()
                logging.debug('===%s===url:%s' % (data['itemid'], url))
                status = urllib.urlopen(url).code
                #print status
            #sys.exit()i
            j += 1
            if status == 404 or status == 502:
                self.qundb.dbQuery("DELETE FROM video_contents where itemid=%s" % data['itemid'])
            else:
                html = self.sGet(url, 'utf-8')
                #print html
                tag = self.sMatch('tt-videoid="', '"', html, 0)
                #print tag
                #sys.exit()
                if len(tag) == 0:
                    self.qundb.dbQuery("DELETE FROM video_contents where itemid=%s" % data['itemid'])
                else:
                    up = {'video_id': tag[0], 'is_done': 1}
                self.qundb.dbUpdate('video_contents', up, "itemid=%s" % data['itemid'])

    def run_user_list(self):
        quncms_db = self.config['mysql']['quncms2']
        self.qundb2 = sMysql(quncms_db['host'], quncms_db['user'], quncms_db['password'], quncms_db['dbname'])
        i = 464
        #tmp = self.qundb2.getRecord("select * from video_author where vtype=1")
        #vmax = len(tmp)
        vmax = 11815
        while 1:
            row = self.qundb2.fetch_one("select * from video_author where id='%s'" % i)
            if i > vmax:
                break
            i += 1
            if row is None:
                continue

            status = urllib.urlopen(row['media_url']).code
            if status == 404:
                self.qundb2.dbQuery("DELETE FROM video_author where id='%s'" % i)
                continue

            uid = row['media_url'].replace('http://toutiao.com/m', '')
            uid = uid.replace('/', '')
            _source_data = self.sGet(row['media_url'], 'utf-8')
            _content = self.sMatch('<div class="pin">', '<\/table>', _source_data, 0)
            #print _content[0]
            #sys.exit()
            last = self.qundb2.fetch_one("select * from video_contents where user_id=%s order by itemid desc limit 1" % uid)
            tag = 'video_life'
            if last is not None:
                tag = last['tag']
            #print last
            #sys.exit()

            print "====%s=====%s==" % (i, row['media_url'])
            for k in range(0, len(_content)):
                ms = re.findall(re.compile(r'阅读|seo_url'), _content[k])
                if ms:
                    continue
                _data = _content[k]
                title = self.sMatch('<h3>', '<\/h3>', _data, 0)
                items = self.sMatch('http:\/\/toutiao.com\/item\/', '\/', _data, 0)
                imgs = self.sMatch('<img src="', '"', _data, 0)
                times = self.sMatch('<td align="right">', '<\/td>', _data, 0)
                vids = self.sMatch('group_id="', '"', _data, 0)
                #print _data
                #print times
                #sys.exit()

                item = {}
                d = datetime.datetime.strptime(times[0], "%Y-%m-%d %H:%M")
                ctime = time.mktime(d.timetuple())
                item['user_id'] = uid
                item['title'] = self.tools.strip_tags(title[0])
                item['tag'] = tag
                item['image_url'] = imgs[0]
                item['item_seo_url'] = "http://toutiao.com/item/%s" % items[0]
                item['v_id'] = str(vids[0])
                item['item_id'] = str(items[0])
                item['keywords'] = ''
                item['video_play_count'] = random.randint(10, 2000)
                item['external_visit_count'] = random.randint(10, 100)
                item['digg_count'] = random.randint(10, 100)
                item['create_time'] = str(ctime)
                item['video_id'] = ''
                item['video_url'] = ''
                _has = self.qundb2.fetch_one("select * from  video_contents where item_id='%s'" % item['item_id'])
                #print item
                #sys.exit()
                if _has is None:
                    logging.debug('Done=====:%s=====%s ' % (item['title'], item['item_seo_url']))
                    self.qundb2.dbInsert('video_contents', item)
                else:
                    print "hasss...."

            #print 1111
            #sys.exit()

            #is_video = self.sMatch('<td>播放:', '<\/td>', _data, 0)

           # if len(is_video) < 3:
            #    print "not video"
            #    self.qundb2.dbUpdate('video_author', {'vtype': 2}, "id=%s" % row['id'])
            #    continue

            '''

            title = self.sMatch('<div class="text">', '<\/div>', _data, 0)
            items = self.sMatch('http:\/\/toutiao.com\/item\/', '\/', _data, 0)
            imgs = self.sMatch('<img src="', '"', _data, 0)
            times = self.sMatch('<td align="right">', '<\/td>', _data, 0)
            vids = self.sMatch('group_id="', '"', _data, 0)
            for k in range(0, len(title)-1):
                if title[k] == '{{{abstract}}}':
                    continue
                item = {}

                d = datetime.datetime.strptime(times[k], "%Y-%m-%d %H:%M")
                ctime = time.mktime(d.timetuple())
                item['user_id'] = uid
                item['title'] = self.tools.strip_tags(title[k])
                item['tag'] = tag
                item['image_url'] = imgs[k+1]
                item['item_seo_url'] = "http://toutiao.com/item/%s" % items[k]
                item['v_id'] = str(vids[k])
                item['item_id'] = str(items[k])
                item['keywords'] = ''
                item['video_play_count'] = random.randint(10, 2000)
                item['external_visit_count'] = random.randint(10, 100)
                item['digg_count'] = random.randint(10, 100)
                item['create_time'] = str(ctime)
                item['video_id'] = ''
                item['video_url'] = ''
                _has = self.qundb2.fetch_one("select * from  video_contents where item_id='%s'" % item['item_id'])
                #print item
                #sys.exit()
                if _has is None:
                    logging.debug('Done=====:%s=====%s ' % (item['title'], item['item_seo_url']))
                    self.qundb2.dbInsert('video_contents', item)
                else:
                    print "hasss...."
            '''

    def run_vlook(self):
        #微录
        self.__get_vlook_cate_list(1150)
        self.__get_vlook_cate_list(1151)
        self.__get_vlook_cate_list(1153)
        self.__get_vlook_cate_list(1154)
        self.__get_vlook_cate_list(1156)
        self.__get_vlook_cate_list(1157)
        self.__get_vlook_cate_list(1159)
        self.__get_vlook_cate_list(1160)
        self.__get_vlook_cate_list(1161)

    def save_video(self):
        #发布 python site.py 1 vps_1
        video_db = self.config['mysql']['quncms2']
        self.qundb2 = sMysql(video_db['host'], video_db['user'], video_db['password'], video_db['dbname'])
        itemid = sys.argv[2]
        vps_no = sys.argv[3]

        url = "http://%s/ajax/SaveVideo/?itemid=%s" % (video_db['video_host_ajax'], itemid)
        print url
        html = self.sGet(url, 'utf-8')
        _data = json.loads(html)
        print _data
        if _data['url'] is not None:
            local_file = "/htdocs/video/%s.mp4" % itemid
            os.system("wget -O %s %s" % (local_file, _data['url']))
            #判断是否空
            if os.path.getsize(local_file) == 0:
                print "==Download Faild"
                sys.exit()
            os.system('scp -P 7422 %s root@%s:/home/wwwroot/video.quncms.com/mp4/' % (local_file, video_db[vps_no]))

            file_url = 'http://%s.%s/mp4/%s.mp4' % (vps_no, video_db['video_host'], itemid)
            print "Done======%s" % file_url
            self.qundb2.dbUpdate('video_contents', {'video_url': file_url}, "itemid=%s" % itemid)

    def run_vlook_post(self):
        data = self.qundb.getRecord("select * from video_contents where is_done=0 and source_site=2")
        for i in range(0, len(data)):
            url = 'http://wtest.vlook.cn/show/qs/%s' % data[i]['video_id']
            _data = self.sGet(url, 'utf-8')
            #print _data
            params = self.sMatch("http:\/\/service.vlook.cn:8080", "'\);", _data, 0)
            conn = httplib.HTTPConnection("service.vlook.cn:8080")
            conn.request('GET', params[0])
            l = ''
            for item in conn.getresponse().getheaders():
                if item[0] == 'location' and item[1] != '':
                    l = item[1]
                    break
            if l:
                up = {'video_url': l, 'is_done': 1}
                print up
                self.qundb.dbUpdate('video_contents', up, "itemid=%s" % data[i]['itemid'])

    def __get_vlook_cate_list(self, cate_id):
        caate = {
            '1150': 'video_child',
            '1151': 'video_pet',
            '1153': 'video_hongren',
            '1154': 'video_meinv',
            '1156': 'video_funny',
            '1157': 'video_travel',
            '1159': 'video_sports',
            '1160': 'video_food',
            '1161': 'video_movie'
        }
        url = "http://www.vlook.cn/ajx/n/square/category?cid=%s&scrollSpan=25&no=1&size=50&sid=3&rnd=0.2246547263694343" % cate_id
        html = self.sGet(url, 'utf-8')
        _data = json.loads(html)
        _data = _data['rst']['html']
        print "=====cate_id:%s======%s===" % (cate_id, len(_data))
        for i in range(0, len(_data)):
            if len(_data[i]) < 30:
                continue
            titles = self.sMatch('<font>', '<\/font>', _data[i], 0)
            username = self.sMatch('<span title="(.*?)">', '<\/span>', _data[i], 0)
            video_id = self.sMatch('<a href="\/show\/qs\/', '"', _data[i], 0)
            blogid = self.sMatch('<a href="\/ta\/qs\/', '"', _data[i], 0)
            blogid = "http://www.vlook.cn/ta/qs/%s" % blogid[0]
            dateline = self.sMatch('<span class="font1" title="', '">', _data[i], 0)
            srcs = self.sMatch('src="', '"', _data[i], 0)
            d = datetime.datetime.strptime(dateline[0], "%Y-%m-%d %H:%M:%S")
            ctime = time.mktime(d.timetuple())

            uid = self.sMatch('blogId="', '"', _data[i], 0)
            #sys.exit()
            axd = self.tools.strip_tags(titles[0])
            axd = self.__clear_html(axd)
            item = {}
            item['vlook_uid'] = uid[0]
            item['source_site'] = 2
            item['user_id'] = 0
            item['title'] = axd
            item['tag'] = caate[str(cate_id)]
            item['image_url'] = srcs[1]
            item['item_seo_url'] = ''
            item['v_id'] = 0
            item['item_id'] = 0
            item['keywords'] = ''
            item['video_play_count'] = random.randint(10, 2000)
            item['external_visit_count'] = random.randint(10, 100)
            item['digg_count'] = random.randint(10, 100)
            item['create_time'] = str(ctime)
            item['video_id'] = video_id[0]
            item['video_url'] = ''
            #item['is_done'] = 1
            _has = self.qundb.fetch_one("select * from  video_contents where video_id='%s'" % item['video_id'])
            #print _has
            #sys.exit()
            if _has is None:
                logging.debug('Done=====:%s=====%s ' % (item['title'], item['video_id']))
                self.qundb.dbInsert('video_contents', item)

            vauthor = {}
            vauthor['media_name'] = username[0][0]
            vauthor['media_url'] = "%s|%s" % (uid[0], str(blogid))
            vauthor['middle_image'] = srcs[0]
            #print vauthor
            _has = self.qundb.fetch_one("select * from video_author where media_url='%s'" % vauthor['media_url'])
            if _has is None:
                self.qundb.dbInsert('video_author', vauthor)

    def run_yy(self):
        #yy直播
        self.__get_yy_show('dance', 11)

        self.__get_yy_show('mc', 29)

    def __get_yy_show(self, vtype, mid):
        self.tools.setup_logging(sys.argv[1], True, True)
        for i in range(1, 10):
            url = 'http://www.yy.com/specialMore/page?biz=%s&subBiz=&moduleId=%s&page=%s' % (vtype, mid, i)
            print url
            html = self.sGet(url, 'utf-8')
            _data = json.loads(html)
            _data = _data['data']['data']
            for j in range(0, len(_data)):
                item = {
                    'yy_title': MySQLdb.escape_string(_data[j]['name']),
                    'yy_view': _data[j]['users'],
                    'yy_pic': _data[j]['thumb'],
                    'yy_id': _data[j]['sid'],
                }
                _has = self.qundb.fetch_one("select * from  video_yy where yy_id=%s" % item['yy_id'])
                if _has is None:
                    action = 'Add'
                    self.qundb.dbInsert('video_yy', item)
                else:
                    action = 'Update'
                    up = {'yy_title': item['yy_title'], 'yy_view': item['yy_view']}
                    self.qundb.dbUpdate('video_yy', up, "yy_id=%s" % item['yy_id'])

                logging.debug('%s=====:%s=====%s ' % (action, item['yy_title'], _data[j]['sid']))

    def __get_yy_red(self):
        #红人,不用直播不固定
        self.tools.setup_logging(sys.argv[1], True, True)
        links = [
            'http://www.yy.com/t/red',
            #'http://www.yy.com/ent/dance',
            #'http://www.yy.com/ent/music'
        ]

        for i in range(0, len(links)):
            url = links[i]
            html = self.sGet(url, 'utf-8')
            titles = self.sMatch('<p class="video-title">', '<\/p>', html, 0)
            count = self.sMatch('<div class="audience-count">', '<\/div>', html, 0)
            pics = self.sMatch('data-original="', '"', html, 0)
            #3086431716
            gourl = self.sMatch('class="video-box" href="', '"', html, 0)

            for j in range(0, len(titles)):
                _vvid = gourl[j].split('_')
                if len(_vvid) < 2:
                    continue
                axd = self.tools.strip_tags(count[j])
                axd = axd.strip()
                item = {
                    'yy_title': MySQLdb.escape_string(titles[j]),
                    'yy_view': axd,
                    'yy_pic': pics[j],
                    'yy_id': _vvid[1],
                }

                _has = self.qundb.fetch_one("select * from  video_yy where yy_id=%s" % item['yy_id'])
                if _has is None:
                    action = 'Add'
                    self.qundb.dbInsert('video_yy', item)
                else:
                    action = 'Update'
                    up = {'yy_title': item['yy_title'], 'yy_view': item['yy_view']}
                    self.qundb.dbUpdate('video_yy', up, "yy_id=%s" % item['yy_id'])

                logging.debug('%s=====:%s=====%s ' % (action, item['yy_title'], gourl[j]))

    def __clear_html(self, htmlstr):
        htmlstr = htmlstr.replace('\r\n', '')
        htmlstr = htmlstr.replace("'", '')
        htmlstr = htmlstr.replace("\\", '')
        return htmlstr
