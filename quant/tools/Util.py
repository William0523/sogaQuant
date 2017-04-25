# -*- coding: UTF-8 -*-

'''
a = sTools()
staticmethod调用不用实例化 sTools.func()
'''
#import urllib.parse,
import re
import os
import time
import logging
import logging.handlers
from datetime import *
'''
__all__ = [
    'sWrite',
    'sGet',
    'sPost',
    'sMatch',
    'sTags'
    'one_stringhash'
]
'''
DOT = frozenset('.')
JOB_LOGS_DIRECTORY = '/htdocs/logs/python'


class sTools:

    def __init__(self):
        pass

    def __call__(self):
        pass

    def get_current_year(self):
        return time.localtime(time.time())[0]

    def is_alpha_chn(self, content_str):
        for uchar in content_str:
            if not re.match(ur'[\.0-9·《》\sA-z\u4e00-\u9fa5]+', uchar) and not re.match(ur'[^<>:]+', uchar):
                return False
        return True

    def is_alphabet(self, uchar):
        """判断一个unicode是否是英文字母"""
        if (uchar >= u'\u0041' and uchar <= u'\u005a') or (uchar >= u'\u0061' and uchar <= u'\u007a'):
            return True
        else:
            return False

    def is_chinese(self, uchar):
        """判断一个unicode是否是汉字"""
        if uchar >= u'\u4e00' and uchar <= u'\u9fa5':
            return True
        else:
            return False

    def val(self, val):
        if val is None:
            return None
        if val == 'null':
            return None
        if val == 'undefined':
            return None
        if val == 0:
            return val
        if val == []:
            return None
        if val == {}:
            return None
        if isinstance(val, basestring) and len(val) == 0:
            return None
        if isinstance(val, dict) and len(val) == 0:
            return None
        return val

    def debug(self, xlist):
        l = len(xlist)
        for i in range(0, l):
            print "===".join(xlist[i])

    #格式化Money为千分位
    def format_money(self, m):
        return "{:,.2f}".format(m, ',')

    #格式化日期
    def d_date(self, ymd='%Y-%m-%d %H:%M:%S', unix_time=0):
        if unix_time == 0:
            unix_time = datetime.now().strftime("%s")

        unix_time = int(unix_time)
        return datetime.fromtimestamp(unix_time).strftime(ymd)

    def sWrite(self, text, filename='log.txt'):
        fp = open(filename, 'a+')
        #text = self.d_date() + " == " + text + "\n"
        #output = " %s == %s \n" % (self.d_date(), text)
        output = "%s\n" % text
        fp.write(output)

    _benchmark = {}

    def benchmark_begin(self, name):
        self._benchmark[name] = {'b': datetime.now().strftime("%s"), 'b2': datetime.now().microsecond}

    def benchmark_end(self, name):
        if name in self._benchmark:
            self._benchmark[name].update({'e': datetime.now().strftime("%s"), 'e2': datetime.now().microsecond})
        else:
            self._benchmark[name] = {'e': datetime.now().strftime("%s"), 'e2': datetime.now().microsecond}

    def setup_logging(self, job, daemon=False, verbose=False):
        #log_folder = '%s/%s' % (JOB_LOGS_DIRECTORY, job)
        #file_path = '%s/%s' % (MIN_DATA_LOG, self.today)
        if os.path.exists(JOB_LOGS_DIRECTORY) is False:
            os.makedirs(JOB_LOGS_DIRECTORY)

        log_filename = '%s/%s.log' % (JOB_LOGS_DIRECTORY, job)
        logger = logging.getLogger()

        if verbose:
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO)

        handlers = []

        if daemon:
            handlers.append(logging.handlers.TimedRotatingFileHandler(filename=log_filename, when='midnight'))
        else:
            handlers.append(logging.FileHandler(filename='%s.%s' % (log_filename, time.strftime('%Y-%m-%d'))))

        handlers.append(logging.StreamHandler())

        for handler in handlers:
            if verbose:
                handler.setLevel(logging.DEBUG)
            else:
                handler.setLevel(logging.INFO)
            handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
            logger.addHandler(handler)

    '''
    def strip_tags(self, string, allowed_tags=''):
        if allowed_tags != '':
            # Get a list of all allowed tag names.
            allowed_tags_list = re.sub(r'[\/<> ]+', '', allowed_tags).split(',')
            allowed_pattern = ''
            for s in allowed_tags_list:
                if s == '':
                    continue
                    # Add all possible patterns for this tag to the regex.
                    if allowed_pattern != '':
                        allowed_pattern += '|'
                        allowed_pattern += '<' + s + ' [^><]*>$|<' + s + '>|'
                     # Get all tags included in the string.
            all_tags = re.findall(r'<]+>', string, re.I)
            for tag in all_tags:
                # If not allowed, replace it.
                if not re.match(allowed_pattern, tag, re.I):
                    string = string.replace(tag, '')
        else:
            # If no allowed tags, remove all.
            string = re.sub(r'<[^>]*?>', '', string)
        return string
    '''
    ##过滤HTML中的标签
    #将HTML中标签等信息去掉
    #@param htmlstr HTML字符串.
    def strip_tags(self, htmlstr):
        #先过滤CDATA
        re_cdata = re.compile('//<!\[CDATA\[[^>]*//\]\]>', re.I)
        re_script = re.compile('<\s*script[^>]*>[^<]*<\s*/\s*script\s*>', re.I)
        re_style = re.compile('<\s*style[^>]*>[^<]*<\s*/\s*style\s*>', re.I)
        re_br = re.compile('<br\s*?/?>')
        re_h = re.compile('</?\w+[^>]*>')
        re_comment = re.compile('<!--[^>]*-->')
        htmlstr = htmlstr.replace('\r\n', '')
        htmlstr = htmlstr.replace('\t', '')
        htmlstr = htmlstr.replace('\n', '')
        s = re_cdata.sub('', htmlstr)
        s = re_script.sub('', s)
        s = re_style.sub('', s)
        s = re_br.sub('\n', s)
        s = re_h.sub('', s)
        s = re_comment.sub('', s)
        #去掉多余的空行
        blank_line = re.compile('\n+')
        s = blank_line.sub('\n', s)

        #s=replaceCharEntity(s)
        return s

    ##替换常用HTML字符实体.
    #使用正常的字符替换HTML中特殊的字符实体.
    #你可以添加新的实体字符到CHAR_ENTITIES中,处理更多HTML字符实体.
    #@param htmlstr HTML字符串.
    def replaceCharEntity(self, htmlstr):
        CHAR_ENTITIES = {
            'nbsp': ' ',
            '160': ' ',
            'lt': '<',
            '60': '<',
            'gt': '>',
            '62': '>',
            'amp': '&',
            '38': '&',
            'quot': '"',
            '34': '"',
        }

        re_charEntity = re.compile(r'&#?(?P<name>\w+);')
        sz = re_charEntity.search(htmlstr)
        while sz:
            #entity全称，如>
            #entity = sz.group()
            #去除&;后entity,如>为gt
            key = sz.group('name')
            try:
                htmlstr = re_charEntity.sub(CHAR_ENTITIES[key], htmlstr, 1)
                sz = re_charEntity.search(htmlstr)
            except KeyError:
                #以空串代替
                htmlstr = re_charEntity.sub('', htmlstr, 1)
                sz = re_charEntity.search(htmlstr)
        return htmlstr

    def repalce(self, s, re_exp, repl_string):
        return re_exp.sub(repl_string, s)
