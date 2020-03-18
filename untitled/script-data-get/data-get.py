import GetOldTweets3 as Tweet
import pandas as pd
from datetime import datetime
import time
import os
from SYS.DATA_GET.FileProcessor import FileProcessor
from SYS.URI import raw_data as d
from SYS.CANDIDATES import candidates_raw
from SYS.DATE import month_start, month_final, election_date as e_date
from urllib.error import HTTPError

twitter_max = 10000

date_start = 0
date_final = 0


class TwitterSpider:
    def __init__(self, in_candidate, in_date_start, in_date_final, in_twitter_max, top_bool):
        self.candidate = in_candidate
        self.date_start = in_date_start
        self.top_bool = top_bool
        self.twitter_conf = Tweet.manager.TweetCriteria()
        self.twitter_conf.setQuerySearch(in_candidate)
        self.twitter_conf.setSince(in_date_start)
        self.twitter_conf.setUntil(in_date_final)
        self.twitter_conf.setMaxTweets(in_twitter_max)
        self.twitter_conf.setTopTweets(top_bool)
        self.twitters = None
        self.df = pd.DataFrame()

    def get_twitter(self):
        import time
        while True:
            try:
                start = time.time()
                print("{} [System]: Retrieving {}'s data. ({}-{})"
                      .format(datetime.now(), date_start, self.candidate, self.top_bool))
                self.twitters = Tweet.manager.TweetManager.getTweets(self.twitter_conf)
                end = time.time()
                print("{} [System]: Retrieved {}'s data. ({}-{}) Took {:.3f} seconds."
                      .format(datetime.now(), date_start, self.candidate, self.top_bool, end - start))
                break
            except Exception:
                print("Too many requests, sleep 20 seconds")
                time.sleep(20)

    def to_pandas(self):
        columns = ['id', 'permalink', 'username', 'to', 'text', 'date',
                   'retweets', 'favorites', 'mentions', 'hashtags', 'geo']
        self.df = pd.DataFrame(columns=columns)
        for tt in self.twitters:
            new_row = {'id': tt.id,
                       'permalink': tt.permalink, 'username': tt.username,
                       'to': tt.to,
                       'text': tt.text,
                       'date': tt.date,
                       'retweets': tt.retweets,
                       'favorites': tt.favorites,
                       'mentions': tt.mentions,
                       'hashtags': tt.hashtags,
                       'geo': tt.geo
                       }
            self.df = self.df.append(new_row, ignore_index=True)
        print("{} [System]: Created {}'s data data frame. ({}-{}) Data size: {} tweets."
              .format(datetime.now(), self.date_start, self.candidate, self.top_bool, len(self.df)))

    def to_parquet(self, filename=os.path.join(d, "output.parquet.gzip")):
        self.df.to_parquet(filename, compression='gzip')


for isTop in (True, False):
    for candidate in candidates_raw:
        for month in range(month_start, month_final + 1):
            for day in range(1, 32):
                try:
                    c_date = datetime(year=2016, month=month, day=day)
                except ValueError:
                    continue
                if c_date > e_date:
                    continue
                date_start, date_final = FileProcessor.get_dates(month, day)
                fd, fn, fe = FileProcessor.get_file_dir_uri(date_start, candidate, isTop)
                if fe:
                    continue
                spider = TwitterSpider(candidate, date_start, date_final, twitter_max, isTop)
                spider.get_twitter()
                spider.to_pandas()
                spider.to_parquet(fn)
                break
