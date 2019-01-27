# -*- coding: utf-8 -*-
__author__ = "Akshay Nar"

#python dependencies
import logging
import gzip
import shutil
from os import listdir
from os.path import isfile, join, isdir

class ReadDataFromFiles:
    """
    ReadDataFromFiles class is used from extracting gz type of files from a 
    directory

    APPLICATION
        - Given a file path for a folder, read all the elements in the folder
        - Extract all the elements in the folder
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # create a file handler
    handler = logging.FileHandler('E:/Collinson_Task/read_data_from_files.log')
    handler.setLevel(logging.INFO)

    # create a logging format
    formatter = logging.Formatter(
        '%(levelname)-8s %(asctime)s,%(msecs)d  [%(filename)s:%(lineno)d] %(message)s')
    handler.setFormatter(formatter)

    # add the handlers to the logger
    logger.addHandler(handler)

    def read_from_files(self,directory):
        print(directory)
        processing_fileslist = []
        directory_list = [join(directory, o) for o in listdir(directory) 
                    if isdir(join(directory,o))]

        self.logger.info("read_from_files : {}".format(directory_list))

        for filepath in directory_list:
            fileslist = [f for f in listdir(filepath) if isfile(join(filepath, f))]
            processing_fileslist.append(self.unzip_files(fileslist,filepath))
            self.logger.info("read_from_files : {}".format(fileslist))

        self.logger.info("read_from_files : {}".format(processing_fileslist))    
        return processing_fileslist

    def unzip_files(self,filelist,directory):
        folder_fileslist = []
        for zip_file in filelist:
            with gzip.open(directory + '\\' + zip_file, 'rb') as f_in:
                with open(directory + '\\' + zip_file.split('.gz')[0], 'wb') as f_out:
                    folder_fileslist.append(directory + '\\' + zip_file.split('.gz')[0])
                    shutil.copyfileobj(f_in, f_out)

        return folder_fileslist