# -*- coding: utf-8 -*-
import sys
import logging
import httplib
import hashlib
import pycurl
import cStringIO
#import time
import commands
import base64
import MySQLdb
#from bson.objectid import ObjectId
#from elasticsearch import Elasticsearch
#from bs4 import BeautifulSoup

from quant.core.Spider import *
UA = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36"


class TaoSpider(SpiderEngine):
    '''
    '''
    def __init__(self):
        SpiderEngine.__init__(self)
        quncms_db = self.config['mysql']['avtao']
        self.db2 = sMysql(quncms_db['host'], quncms_db['user'], quncms_db['password'], quncms_db['dbname'])

    def get_info(self, catid):
        #self.get_from_news(url)
        #self.get_video_info(catid)
        pass

    def run(self):
        print sys.argv
        self.tools.setup_logging(sys.argv[1], True, True)
        #self.get_video_info(1)
        #return

        listx = self.db2.getRecord("select * from videos where id>3457")
        for a in range(0, len(listx)):
            url = "http://www.avtb.me%s" % listx[a]['video_url']
            print "%s====%s" % (url, listx[a]['id'])

            self.get_video_text(url, listx[a]['id'])

        return

        catelist = self.db2.getRecord("select * from cate where id>=4")
        for x in range(0, len(catelist)):
            vid = catelist[x]['cate_url']
            page = int(catelist[x]['cate_page']) + 1
            for j in range(1, page):
                self.get_video_info(vid, j)

            #break
        print "done"
        return

        datas = []
        for i in range(1, 6):
            nvid = vid + i
            datas.append(nvid)

        vid += 5
        logging.debug('Done Vid=====:%s ' % vid)
        self.run_worker(datas)

    def get_video_text(self, url, kid):
        _source_data = self.sGet(url, 'utf-8')
        #out_put = '/usr/bin/php /htdocs/quant/c.php %s' % base64.b64encode(url)
        #print out_put
        #_source_data = commands.getoutput(out_put)
        #print _source_data
        video_ul = self.sMatch('<source src="', '"', _source_data, 0)
        tags = self.sMatch('<a href="\/tag/(.*?)\/', '"', _source_data, 0)
        #print tags
        atags = []
        if len(tags):
            for x in range(0, len(tags)):
                atags.append(tags[x][0])

        satags = ",".join(atags)
        if video_ul is not None:
            #print video_ul
            self.db2.dbUpdate('videos', {'video_mp4': video_ul[0], 'video_tags': satags}, "id=%s" % kid)
       # sys.exit()

    def get_video_info(self, catid, page):
        #url = "http://www.avtb.me/premium/recent/"
        #url = "http://www.avtb.me/%s/" % catid
        if page == 1:
            url = "http://www.avtb.me/%s/" % catid
        else:
            url = "http://www.avtb.me/%s/recent/%s/" % (catid, page)

        print url
        #return

        _source_data = self.sGet(url, 'utf-8')
        video_ul = self.sMatch('<ul class="videos">', '<\/ul>', _source_data, 0)
        video_view = self.sMatch('<a href="(.*?)" title="(.*?)" class="thumbnail" target="_blank">', '<\/a>', video_ul[0], 0)
        video_pic = self.sMatch('<img src="(.*?)', '"', video_ul[0], 0)
        video_lenth = self.sMatch('<span class="video-overlay badge transparent">', '<\/span>', video_ul[0], 0)
        print video_view[0][0]
        #print video_view[0][1]

        #sys.exit()
        for i in range(0, len(video_view)):
            item = {
                'video_url': video_view[i][0],
                'video_cate': catid,
                'video_title': video_view[i][1],
                'video_pic': video_pic[i][1],
                'video_length': video_lenth[i].replace("\t", "").replace("\n", ""),
                'video_tags': '',
                'video_mp4': ''
            }
            _has = self.db2.fetch_one("select * from videos where video_url='%s'" % item['video_url'])
            if _has is None:
                self.db2.dbInsert('videos', item)
