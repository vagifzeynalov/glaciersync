'''
Created on Feb 6, 2016

@author: vagif
'''
import os
import glob
import shelve

class ShelveStorage(object):

    __PATTERN_SHELVE_STORAGE_FILE = '%s/%s'

    def __init__(self, shelveFolder, shelveFilename):
        shelveFilePath = os.path.expanduser(shelveFolder)
        if not os.path.isdir(shelveFilePath):
            os.mkdir(shelveFilePath)
        self.shelveFile = self.__PATTERN_SHELVE_STORAGE_FILE % (shelveFilePath, shelveFilename)

    def remove(self):
        filelist = glob.glob(self.shelveFile + "*")
        for f in filelist:
            os.remove(f)

    def __enter__(self):
        self.shelve = shelve.open(self.shelveFile)
        return self.shelve

    def __exit__(self, exc_type, exc_value, traceback):
        self.shelve.close()
