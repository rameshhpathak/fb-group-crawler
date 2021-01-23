# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy.loader.processors import Join
from datetime import datetime

def parse_date(date):
    import json

    d = json.loads(date[0]) #nested dict of features
    flat_d = dict() #only retain 'leaves' of d tree

    def recursive_items(dictionary):
        '''
        Get most nested key:value pair of nested dict
        '''
        for key, value in dictionary.items():
            if type(value) is dict:
                yield from recursive_items(value)
            else:
                yield (key, value)

    for key, value in recursive_items(d):
        flat_d[key] = value

    #returns timestamp in localtime conversion from linux timestamp UTC
    ret = str(datetime.fromtimestamp(flat_d['publish_time'])) if 'publish_time' in flat_d else None
    return ret

def parse_date2(init_date, loader_context):
    lang = loader_context['lang']

# =============================================================================
# English Date Parsing if not found on features
# =============================================================================
    if lang == 'en':
        months = {
        'january':1,
        'february':2,
        'march':3,
        'april':4,
        'may':5,
        'june':6,
        'july':7,
        'august':8,
        'september':9,
        'october':10,
        'november':11,
        'december':12
        }

        months_abbr = {
        'jan':1,
        'feb':2,
        'mar':3,
        'apr':4,
        'may':5,
        'jun':6,
        'jul':7,
        'aug':8,
        'sep':9,
        'oct':10,
        'nov':11,
        'dec':12
        }

        days = {
        'monday':0,
        'tuesday':1,
        'wednesday':2,
        'thursday':3,
        'friday':4,
        'saturday':5,
        'sunday':6
        }

        date = init_date[0].split()
        year, month, day = [int(i) for i in str(datetime.now().date()).split(sep='-')] #default is today

        l = len(date)

        #sanity check
        if l == 0:
            return 'Error: no data'

        #Yesterday, Now, 4hr, 50mins
        elif l == 1:
            if date[0].isalpha():
                if date[0].lower() == 'yesterday':
                    day = int(str(datetime.now().date()-timedelta(1)).split(sep='-')[2])
                    #check that yesterday was not in another month
                    month = int(str(datetime.now().date()-timedelta(1)).split(sep='-')[1])
                elif date[0].lower() == 'now':
                        return datetime(year,month,day).date()    #return today
                else:  #not recognized, (return date or init_date)
                    return date
            else:
                #4h, 50min (exploit future parsing)
                l = 2
                new_date = [x for x in date[0] if x.isdigit()]
                date[0] = ''.join(new_date)
                new_date = [x for x in date[0] if not(x.isdigit())]
                date[1] = ''.join(new_date)
# l = 2
        elif l == 2:
            if date[1] == 'now':
                return datetime(year,month,day).date()
            #22 min (ieri)
            if date[1] == 'min' or date[1] == 'mins':
                if int(str(datetime.now().time()).split(sep=':')[1]) - int(date[0]) < 0 and int(str(datetime.now().time()).split(sep=':')[0])==0:
                    day = int(str(datetime.now().date()-timedelta(1)).split(sep='-')[2])
                    month = int(str(datetime.now().date()-timedelta(1)).split(sep='-')[1])
                    return datetime(year,month,day).date()
                #22 min (oggi)
                else:
                    return datetime(year,month,day).date()

            #4 h (ieri)
            elif date[1] == 'hr' or date[1] == 'hrs':
                if int(str(datetime.now().time()).split(sep=':')[0]) - int(date[0]) < 0:
                    day = int(str(datetime.now().date()-timedelta(1)).split(sep='-')[2])
                    month = int(str(datetime.now().date()-timedelta(1)).split(sep='-')[1])
                    return datetime(year,month,day).date()
                #4 h (oggi)
                else:
                    return datetime(year,month,day).date()

            #2 jan
            elif len(date[1]) == 3 and date[1].isalpha():
                day = int(date[0])
                month = months_abbr[date[1].lower()]
                return datetime(year,month,day).date()
            #2 january
            elif len(date[1]) > 3 and date[1].isalpha():
                day = int(date[0])
                month = months[date[1]]
                return datetime(year,month,day).date()
            #jan 2
            elif len(date[0]) == 3 and date[0].isalpha():
                day = int(date[1])
                month = months_abbr[date[0].lower()]
                return datetime(year,month,day).date()
            #january 2
            elif len(date[0]) > 3 and date[0].isalpha():
                day = int(date[1])
                month = months[date[0]]
                return datetime(year,month,day).date()
            #parsing failed
            else:
                return date
            return date
# l = 3
        elif l == 3:
            #5 hours ago
            if date[2] == 'ago':
                if date[1] == 'hour' or date[1] == 'hours' or date[1] == 'hr' or date[1] == 'hrs':
                    # 5 hours ago (yesterday)
                    if int(str(datetime.now().time()).split(sep=':')[0]) - int(date[0]) < 0:
                        day = int(str(datetime.now().date()-timedelta(1)).split(sep='-')[2])
                        month = int(str(datetime.now().date()-timedelta(1)).split(sep='-')[1])
                        return datetime(year,month,day).date()
                    # 5 hours ago (today)
                    else:
                        return datetime(year,month,day).date()
                #10 minutes ago
                elif date[1] == 'minute' or date[1] == 'minutes' or date[1] == 'min' or date[1] == 'mins':
                    #22 minutes ago (yesterday)
                    if int(str(datetime.now().time()).split(sep=':')[1]) - int(date[0]) < 0 and int(str(datetime.now().time()).split(sep=':')[0])==0:
                        day = int(str(datetime.now().date()-timedelta(1)).split(sep='-')[2])
                        month = int(str(datetime.now().date()-timedelta(1)).split(sep='-')[1])
                        return datetime(year,month,day).date()
                    #22 minutes ago (today)
                    else:
                        return datetime(year,month,day).date()
                else:
                    return date
            else:
                #21 Jun 2017
                if len(date[1]) == 3 and date[1].isalpha() and date[2].isdigit():
                    day = int(date[0])
                    month = months_abbr[date[1].lower()]
                    year = int(date[2])
                    return datetime(year,month,day).date()
                #21 June 2017
                elif len(date[1]) > 3 and date[1].isalpha() and date[2].isdigit():
                    day = int(date[0])
                    month = months[date[1].lower()]
                    year = int(date[2])
                    return datetime(year,month,day).date()
                #Jul 11, 2016
                elif len(date[0]) == 3 and len(date[1]) == 3 and date[0].isalpha():
                    day = int(date[1][:-1])
                    month = months_abbr[date[0].lower()]
                    year = int(date[2])
                    return datetime(year,month,day).date()
                #parsing failed
                else:
                    return date
# l = 4
        elif l == 4:
            #yesterday at 23:32 PM
            if date[0].lower() == 'yesterday' and date[1] == 'at':
                day = int(str(datetime.now().date()-timedelta(1)).split(sep='-')[2])
                month = int(str(datetime.now().date()-timedelta(1)).split(sep='-')[1])
                return datetime(year,month,day).date()
            #Thursday at 4:27 PM
            elif date[1] == 'at':
                today = datetime.now().weekday() #today as a weekday
                weekday = days[date[0].lower()]   #day to be match as number weekday
                #weekday is chronologically always lower than day
                delta = today - weekday
                if delta >= 0:
                    day = int(str(datetime.now().date()-timedelta(delta)).split(sep='-')[2])
                    month = int(str(datetime.now().date()-timedelta(delta)).split(sep='-')[1])
                    return datetime(year,month,day).date()
                #monday = 0 saturday = 6
                else:
                    delta += 8
                    day = int(str(datetime.now().date()-timedelta(delta)).split(sep='-')[2])
                    month = int(str(datetime.now().date()-timedelta(delta)).split(sep='-')[1])
                    return datetime(year,month,day).date()
            #parsing failed
            else:
                return date
# l = 5
        elif l == 5:
           if date[2] == 'at':
               #Jan 29 at 10:00 PM
               if len(date[0]) == 3:
                   day = int(date[1])
                   month = months_abbr[date[0].lower()]
                   return datetime(year,month,day).date()
               #29 febbraio alle ore 21:49
               else:
                   day = int(date[1])
                   month = months[date[0].lower()]
                   return datetime(year,month,day).date()
           #parsing failed
           else:
               return date
# l = 6
        elif l == 6:
           if date[3] == 'at':
               date[1]
               #Aug 25, 2016 at 7:00 PM
               if len(date[0]) == 3:
                   day = int(date[1][:-1])
                   month = months_abbr[date[0].lower()]
                   year = int(date[2])
                   return datetime(year,month,day).date()
               #August 25, 2016 at 7:00 PM
               else:
                   day = int(date[1][:-1])
                   month = months[date[0].lower()]
                   year = int(date[2])
                   return datetime(year,month,day).date()
           #parsing failed
           else:
               return date
# l > 6
        #parsing failed - l too big
        else:
            return date
    #parsing failed - language not supported
    else:
        return init_date



def url_strip(url):
    fullurl = url[0]
    #catchin '&id=' is enough to identify the post
    i = fullurl.find('&id=')
    if i != -1:
        return fullurl[:i+4] + fullurl[i+4:].split('&')[0]
    else:  #catch photos
        i = fullurl.find('/photos/')
        if i != -1:
            return fullurl[:i+8] + fullurl[i+8:].split('/?')[0]
        else: #catch albums
            i = fullurl.find('/albums/')
            if i != -1:
                return fullurl[:i+8] + fullurl[i+8:].split('/?')[0]
            else:
                return fullurl

class FbGroupSpiderItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    date = scrapy.Field()
    text = scrapy.Field(output_processor=Join(separator=u'')) #full text of the post
    url = scrapy.Field(output_processor=url_strip)
    img = scrapy.Field()
