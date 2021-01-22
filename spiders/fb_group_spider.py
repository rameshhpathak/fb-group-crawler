import logging
import datetime

import scrapy

from scrapy.loader import ItemLoader
from scrapy.exceptions import CloseSpider
from fb_group_spider.items import FbGroupSpiderItem, parse_date

class FbGroupSpider(scrapy.Spider):
    '''
    Crawles facebook group and extracts main text, date and images from posts
    '''
    name = "FbGroupSpider"
    custom_settings = {
        'FEED_EXPORT_FIELDS': ['date', 'text', 'img'],
        'DUPEFILTER_CLASS' : 'scrapy.dupefilters.BaseDupeFilter',
    }

    def __init__(self, *args, **kwargs):
        #set LOG_LEVEL=DEBUG in settings.py to see more logs
        logger = logging.getLogger('scrapy.middleware')
        logger.setLevel(logging.WARNING)
        
        super().__init__(*args,**kwargs)

        # to count consecutive 3 pages where all post are greater than 3 weeks old
        self.count = 0
        self.contains_old_post = None
        #current year, this variable is needed for proper.parse recursion
        self.now = datetime.datetime.now()
        self.k = self.now.year
        self.three_week_before = self.now - datetime.timedelta(days=21)
        self.start_urls = ['https://mbasic.facebook.com/groups/kbhlejebolig/']

    def parse(self, response):
        '''
        Parse the facebook group
        '''
        self.contains_old_post = []
        # selecting all posts
        for post in response.xpath("//article[contains(@data-ft, 'top_level_post_id')]"):

            features = post.xpath('./@data-ft').get()
            date = []
            
            date.append(features)
            date = parse_date(date)

            post_publish_date = datetime.datetime.strptime(date,'%Y-%m-%d %H:%M:%S')
            
            self.logger.info('Post found with date {}'.format(date))

            if post_publish_date > self.three_week_before:
                self.contains_old_post.append(False)
                new = ItemLoader(item=FbGroupSpiderItem(), selector=post)
                self.logger.info('Parsing post..  page_count = {}, post_date = {}'.format(self.count, date))

                new.add_value('date', date)
                new.add_xpath('url', ".//a[contains(@href, 'footer')]/@href")

                #returns full post-link in a list
                post = post.xpath(".//a[contains(@href,'footer')]/@href").extract() 
                temp_post = response.urljoin(post[0])
                yield scrapy.Request(temp_post, self.parse_post, meta={'item':new})
            else:
                self.contains_old_post.append(True)


        if all(self.contains_old_post):
            self.count += 1
        else:
            self.count = 0

        # if we find 3 consecutive page where all posts are older than 3 weeks, we end the spider
        if self.count == 3:
            raise CloseSpider('{} Consecutive pages where all posts are older than 3 weeks. Crawling finished'.format(self.count))
        
        #load following page, try to click on "more"
        #after few pages have been scraped, the "more" link might disappears 
        #if not present look for the highest year not parsed yet
        #click once on the year and go back to clicking "more"

        new_page = response.xpath("//div[contains(@id,'stories_container')]/div/a/@href").extract()
    
        if not new_page: 
            self.logger.info('[!] "more" link not found, will look for a "year" link')
            #self.k is the year link that we look for 
      
            xpath = "//div/a[contains(@href,'time') and contains(text(),'" + str(self.k) + "')]/@href"
            new_page = response.xpath(xpath).extract()
            if new_page:
                new_page = response.urljoin(new_page[0])
                self.k -= 1
                self.logger.info('Found a link for year "{}", new_page = {}'.format(self.k,new_page))
                yield scrapy.Request(new_page, callback=self.parse, meta={'flag':self.k})
            else:
                while not new_page: #sometimes the years are skipped this handles small year gaps
                    self.logger.info('Link not found for year {}, trying with previous year {}'.format(self.k,self.k-1))
                    self.k -= 1
                    xpath = "//div/a[contains(@href,'time') and contains(text(),'" + str(self.k) + "')]/@href"
                    new_page = response.xpath(xpath).extract()
                self.logger.info('Found a link for year "{}", new_page = {}'.format(self.k,new_page))
                new_page = response.urljoin(new_page[0])
                self.k -= 1
                yield scrapy.Request(new_page, callback=self.parse, meta={'flag':self.k}) 

        else:
            new_page = response.urljoin(new_page[0])
            if 'flag' in response.meta:
                self.logger.info('Page scraped, clicking on "more"! new_page = {}'.format(new_page))
                yield scrapy.Request(new_page, callback=self.parse, meta={'flag':response.meta['flag']})
            else:
                self.logger.info('First page scraped, clicking on "more"! new_page = {}'.format(new_page))
                yield scrapy.Request(new_page, callback=self.parse, meta={'flag':self.k})


    def parse_post(self,response):
        new = ItemLoader(item=FbGroupSpiderItem(),response=response,parent=response.meta['item'])
        new.add_xpath('text','//div[@data-ft]//p//text() | //div[@data-ft]/div[@class]/div[@class]/text()')
        new.add_xpath('img', '//div[@data-ft]//div[@class]/div[@class]/a[@href]//img/@src')
        
        yield new.load_item()