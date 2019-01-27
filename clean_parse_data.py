# -*- coding: utf-8 -*-
__author__ = "Akshay Nar"

# python dependencies
import json
import sys
import logging
import ConfigParser
import pymysql
from constant import Constant
from insert_data import InsertData
from read_data_from_files import ReadDataFromFiles

class CleanParseData:
    """
    CleanParseData class is used for extracting files -> reading data from files ->
    parsing the data -> sending it to InsertData Class for data insertion in DB

    APPLICATION
        - Given a folder name, read all the elements in the folder
        - Extract all the elements in the folder
        - After extraction, read each file and parse the data
        - After parsing the data, extract relevant fields from the data and send it to
          InsertData Class for inserting data into database
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # create a file handler
    handler = logging.FileHandler('E:/Collinson_Task/clean_parse_data.log')
    handler.setLevel(logging.INFO)

    # create a logging format
    formatter = logging.Formatter(
        '%(levelname)-8s %(asctime)s,%(msecs)d  [%(filename)s:%(lineno)d] %(message)s')
    handler.setFormatter(formatter)

    # add the handlers to the logger
    logger.addHandler(handler)

    #global variables
    db_connection = None
    folder = sys.argv[1]

    def invoked_script(self):
        """
        invoked_script function is used for doing the actual parsing and cleaning the data
        """
        self.logger.info('CleanParseData processing initiated')

        try:
            self.make_db_connection()

            self.logger.info("invoked_script : Folder Name : {}".format(self.folder))

            self.initiate_processing(self.folder)
        except Exception as error:
            self.logger.error("invoked_script : {}".format(error))
            self.logger.exception("invoked_script_exception : {}".format(error))

    def process_data(self,filepath):
        try:
            data_list = self.read_data(filepath)
            self.parse_data(data_list)
        except Exception as error:
            self.logger.error("process_data : {}".format(error))
            self.logger.exception("process_data_exception : {}".format(error))

    def initiate_processing(self,folder):
        try:
            processing_files_list = ReadDataFromFiles().read_from_files(folder)
            for processing_list in processing_files_list:
                for processing_files in processing_list:
                    print(processing_files)
                    self.process_data(processing_files)
        except Exception as error:
            self.logger.error("initiate_processing : {}".format(error))
            self.logger.exception("initiate_processing_exception : {}".format(error))

    def read_data(self,input_file_name):
        #reading data from file
        tweets_data = []
        tweets_file = open(input_file_name, "r")
        for line in tweets_file:
            try:
                self.logger.info("Single json object : {}".format(line))

                tweet = json.loads(line)
                tweets_data.append(tweet)
            except Exception as error:
                self.logger.error("read_data : {}".format(error))
                self.logger.exception("read_data : {}".format(error))
                continue
        self.logger.info("Total no of json objects: {}".format(len(tweets_data)))
        return tweets_data

    def parse_data(self,unparsed_data):
        try:
            for twitter_data in unparsed_data:
                self.logger.info("parse_data : {}".format(twitter_data))
                self.clean_data(twitter_data)
        except Exception as error:
            self.logger.error("parse_data : {}".format(error))
            self.logger.exception("parse_data_exception : {}".format(error))

    def clean_data(self,twitter_data):
        try:
            #loading string object as json
            # data = json.loads(twitter_data)
            data = twitter_data

            #common atrributes
            user_id = data[Constant.USER][Constant.ID]

            #tweet data
            geo_data = []
            tweet_id = json.dumps(data[Constant.ID])
            tweet_text = json.dumps(data[Constant.TEXT])
            tweet_created_at = data[Constant.CREATED_AT]
            is_coordinates = data[Constant.COORDINATES]

            if(is_coordinates != None):
                geo_data = self.get_geo_data(data[Constant.COORDINATES])

            name = json.dumps(data[Constant.USER][Constant.NAME])
            screen_name = json.dumps(data[Constant.USER][Constant.SCREEN_NAME])
            profile_image_url = json.dumps(data[Constant.USER][Constant.PROFILE_IMAGE_URL])
            is_retweeted = data[Constant.RETWEETED]
            source = json.dumps(data[Constant.SOURCE])
            
            tweet_data_json = {
                Constant.TWEET_ID : tweet_id,
                Constant.TWEET_TEXT : tweet_text,
                Constant.CREATED_AT : tweet_created_at,
                Constant.GEO_DATA : geo_data,
                Constant.USER_ID : user_id,
                Constant.SCREEN_NAME : screen_name,
                Constant.NAME : name,
                Constant.PROFILE_IMAGE_URL : profile_image_url,
                Constant.SOURCE : source,
                Constant.IS_RETWEETED : is_retweeted
            }
            InsertData().insert_data_in_rds(data=tweet_data_json,data_type=Constant.TWEET_DATA,db_con=self.db_connection)
            #tweets data insertion done

            #tweet_mentions_data
            target_user_ids = []
            target_user_ids = self.process_tweet_mentions(data[Constant.ENTITIES])
            tweet_mentions_data = {
                Constant.TWEET_ID : tweet_id,
                Constant.SOURCE_USER_ID : user_id,
                Constant.TARGET_USER_IDS : target_user_ids
            }
            InsertData().insert_data_in_rds(data=tweet_mentions_data,data_type=Constant.TWEET_MENTIONS_DATA,db_con=self.db_connection)
            #tweet mentions data insertion done

            #tweet_tags_data
            tags = []
            tags = self.process_tags(data[Constant.ENTITIES])
            tweet_tags_data = {
                Constant.TWEET_ID : tweet_id,
                Constant.TWEET_TAGS : tags
            }
            InsertData().insert_data_in_rds(data=tweet_tags_data,data_type=Constant.TWEET_TAGS_DATA,db_con=self.db_connection)
            #tweet tags data insertion done

            #tweet urls data
            urls = []
            urls = self.process_urls(data[Constant.ENTITIES])
            # urls = json.dumps(data[Constant.SOURCE])
            tweet_urls_data = {
                Constant.TWEET_ID : tweet_id,
                Constant.TWEET_URLS : urls
            }
            InsertData().insert_data_in_rds(data=tweet_urls_data,data_type=Constant.TWEET_URLS_DATA,db_con=self.db_connection)
            #tweet urls data insertion done

            #tweet user data
            user_url = json.dumps(data[Constant.USER][Constant.URL])
            user_location = json.dumps(data[Constant.USER][Constant.LOCATION])
            user_description = json.dumps(data[Constant.USER][Constant.DESCRIPTION])
            user_created_at = data[Constant.USER][Constant.CREATED_AT]
            user_followers_count = data[Constant.USER][Constant.FOLLOWERS_COUNT]
            user_friends_count = data[Constant.USER][Constant.FRIENDS_COUNT]
            user_statuses_count = data[Constant.USER][Constant.STATUSES_COUNT]
            user_timezone = json.dumps(data[Constant.USER][Constant.TIMEZONE])
            tweet_user_data = {
                Constant.USER_ID : user_id,
                Constant.SCREEN_NAME : screen_name,
                Constant.NAME : name,
                Constant.PROFILE_IMAGE_URL : profile_image_url,
                Constant.USER_LOCATION : user_location,
                Constant.USER_URL : user_url,
                Constant.USER_DESCRIPTION : user_description,
                Constant.USER_CREATED_AT : user_created_at,
                Constant.USER_FOLLOWERS_COUNT : user_followers_count,
                Constant.USER_FRIENDS_COUNT : user_friends_count,
                Constant.USER_STATUSES_COUNT : user_statuses_count,
                Constant.USER_TIMEZONE : user_timezone
            }
            InsertData().insert_data_in_rds(data=tweet_user_data,data_type=Constant.TWEET_USERS_DATA,db_con=self.db_connection)
            #tweet users data insertion done
        except Exception as error:
            self.logger.error("clean_data : {}".format(error))
            self.logger.exception("clean_data_exception : {}".format(error))

    def get_geo_data(self,coordinates):
        try:
            geo_data = []
            longi = coordinates[Constant.COORDINATES][0]
            geo_data.append(longi)

            lat = coordinates[Constant.COORDINATES][1]
            geo_data.append(lat)

            return geo_data
        except Exception as error:
            self.logger.error("get_geo_data : {}".format(error))
            self.logger.exception("get_geo_data : {}".format(error))

    def process_tweet_mentions(self,tweet_mentions):
        try:
            target_user_ids = []
            user_mentions_data = tweet_mentions[Constant.USER_MENTIONS]

            if(len(user_mentions_data) != 0):
                for data in user_mentions_data:
                    target_user_ids.append(data[Constant.ID])
            else:
                return target_user_ids
            
            return target_user_ids
        except Exception as error:
            self.logger.error("process_tweet_mentions : {}".format(error))
            self.logger.exception("process_tweet_mentions : {}".format(error))

    def process_tags(self,tag):
        try:
            tags = []
            tags_data = tag[Constant.HASH_TAGS]

            if(len(tags_data) != 0):
                for data in tags_data:
                    tags.append(json.dumps(data[Constant.TEXT]))
            else:
                return tags
            
            return tags
        except Exception as error:
            self.logger.error("process_tags : {}".format(error))
            self.logger.exception("process_tags : {}".format(error))

    def process_urls(self,url):
        try:
            urls = []
            urls_data = url[Constant.URLS]

            if(len(urls_data) != 0):
                for data in urls_data:
                    urls.append(json.dumps(data[Constant.URL]))
            else:
                return urls

            return urls
        except Exception as error:
            self.logger.error("process_urls : {}".format(error))
            self.logger.exception("process_urls : {}".format(error))
    
    def make_db_connection(self):
        config = ConfigParser.ConfigParser()

        #to be set when executing
        config.read('E:/Collinson_Task/collinsondbparameters.ini')
        stage = 'DEV'
        self.db_connection = pymysql.connect(host=config.get(stage,'DB_HOST'),  # your host
                                            user=config.get(stage,'DB_USERNAME'),  # username
                                            passwd=config.get(stage,'DB_PASSWORD'),  # password
                                            db=config.get(stage,'DB_NAME')) #db name

if __name__ == '__main__':
    CleanParseData().invoked_script()
