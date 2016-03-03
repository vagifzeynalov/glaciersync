'''
Created on Jan 18, 2016

@author: vagif
'''
import os.path

class Config:
    __STORAGE_FOLDER = "~/.aws"
    
    __CONFIG_FILENAME = 'config.py'
    
    def __init__(self, configFile = None):
        allConfigFiles = []
        if configFile is not None:
            allConfigFiles.append(configFile)
        allConfigFiles.append(self.__CONFIG_FILENAME)
        allConfigFiles.append(os.path.expanduser(self.__STORAGE_FOLDER) + "/" + self.__CONFIG_FILENAME)

        for config in allConfigFiles:
            if os.path.isfile(config): 
                self.__config = __import__(config.replace('.py',''))
                return

    def getLocations(self):
        return self.__config.locations

    def getAWScredentials(self):
        return self.__config.awsCredentials
    
    def getAWSPartSize(self):
        return self.__config.partSize
