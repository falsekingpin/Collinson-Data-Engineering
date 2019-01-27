# -*- coding: utf-8 -*-
__author__ = "Akshay Nar"

# python dependencies
import json
import logging
import time
from datetime import datetime
from constant import Constant
import unicodedata

class InsertData:
    """
    CleanParseData class is used for inserting data into RDS for different tables

    APPLICATION
        - Based on the input type, insert data into RDS
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # create a file handler
    handler = logging.FileHandler('E:/Collinson_Task/insert_data.log')
    handler.setLevel(logging.INFO)

    # create a logging format
    formatter = logging.Formatter(
        '%(levelname)-8s %(asctime)s,%(msecs)d  [%(filename)s:%(lineno)d] %(message)s')
    handler.setFormatter(formatter)

    # add the handlers to the logger
    logger.addHandler(handler)

    def insert_data_in_rds(self,data,data_type,db_con):
        try:
            if data_type == Constant.TWEET_DATA:
                self.insert_tweet_data(data,db_con)
            elif data_type == Constant.TWEET_MENTIONS_DATA:
                self.insert_tweet_mentions_data(data,db_con)
            elif data_type == Constant.TWEET_TAGS_DATA:
                self.insert_tweet_tags_data(data,db_con)
            elif data_type == Constant.TWEET_URLS_DATA:
                self.insert_tweet_urls_data(data,db_con)
            elif data_type == Constant.TWEET_USERS_DATA:
                self.insert_tweet_users_data(data,db_con)
        except Exception as error:
            self.logger.error("insert_data_in_rds : {}".format(error))
            self.logger.exception("insert_data_in_rds_exception : {}".format(error))

    def insert_tweet_data(self,data,db_con):
        lat = 0.0
        longi = 0.0
        is_rt = 1
        tweet_date = datetime.strptime(data[Constant.CREATED_AT],'%a %b %d %H:%M:%S +0000 %Y')

        if(len(data[Constant.GEO_DATA]) != 0):
            lat = data[Constant.GEO_DATA][1]
            longi = data[Constant.GEO_DATA][0]
        elif(data[Constant.IS_RETWEETED] == True):
            is_rt = 0

        query = """INSERT INTO tweets(tweet_id, tweet_text, created_at, geo_lat, geo_long,
        user_id, screen_name, name, profile_image_url, source, is_rt)
        VALUES({0}, {1}, '{2}', {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10})
        """.format(data[Constant.TWEET_ID],data[Constant.TWEET_TEXT],tweet_date,
                    lat,longi,data[Constant.USER_ID],data[Constant.SCREEN_NAME],
                    data[Constant.NAME],data[Constant.PROFILE_IMAGE_URL], data[Constant.SOURCE], is_rt)

        self.logger.info("insert_tweet_data : query : {}".format(query))        
        self.db_operation(db_con,query)

    def insert_tweet_mentions_data(self,data,db_con):
        for target_user_id in data[Constant.TARGET_USER_IDS]:
            query = """INSERT INTO tweet_mentions(tweet_id, source_user_id, target_user_id)
            VALUES({0}, {1}, {2})
            """.format(data[Constant.TWEET_ID],data[Constant.SOURCE_USER_ID],target_user_id)

            self.logger.info("insert_tweet_mentions_data : query : {}".format(query)) 
            self.db_operation(db_con,query)

    def insert_tweet_tags_data(self,data,db_con):
        for tweet_tag in data[Constant.TWEET_TAGS]:
            query = """INSERT INTO tweet_tags(tweet_id, tag)
            VALUES({0}, {1})""".format(data[Constant.TWEET_ID],tweet_tag)

            self.logger.info("insert_tweet_tags_data : query : {}".format(query)) 
            self.db_operation(db_con,query)

    def insert_tweet_urls_data(self,data,db_con):
        for tweet_url in data[Constant.TWEET_URLS]:
            query = """INSERT INTO tweet_urls(tweet_id, url)
            VALUES({0}, {1})""".format(data[Constant.TWEET_ID],tweet_url)

            self.logger.info("insert_tweet_urls_data : query : {}".format(query)) 
            self.db_operation(db_con,query)

    def insert_tweet_users_data(self,data,db_con):
        user_created_date = datetime.strptime(data[Constant.USER_CREATED_AT],'%a %b %d %H:%M:%S +0000 %Y')

        query = """INSERT INTO users(user_id, screen_name, name, profile_image_url,
        location, url, description, created_at, followers_count, friends_count, statuses_count, time_zone)
        VALUES({0}, {1}, {2}, {3}, {4}, {5}, {6}, '{7}', {8}, {9}, {10}, {11})""".format(
            data[Constant.USER_ID],data[Constant.SCREEN_NAME],data[Constant.NAME],
            data[Constant.PROFILE_IMAGE_URL],data[Constant.USER_LOCATION],
            data[Constant.USER_URL],data[Constant.USER_DESCRIPTION],user_created_date,
            data[Constant.USER_FOLLOWERS_COUNT],data[Constant.USER_FRIENDS_COUNT],
            data[Constant.USER_STATUSES_COUNT],data[Constant.USER_TIMEZONE]
        )

        self.logger.info("insert_tweet_users_data : query : {}".format(query)) 
        self.db_operation(db_con,query)

    def db_operation(self,db_con,query):
        # prepare a cursor object using cursor() method
        cursor = db_con.cursor()
        try:
            # Execute the SQL command
            cursor.execute(query)
            # Commit your changes in the database
            db_con.commit()
        except Exception as err:
            # Rollback in case there is any error
            db_con.rollback()
            self.logger.error("db_operation : {}".format(err))
            self.logger.exception("db_operation_exception : {}".format(err))