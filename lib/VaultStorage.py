'''
Created on Feb 6, 2016

@author: vagif
'''

import datetime

from lib.ShelveStorage import ShelveStorage

class VaultStorage():
    ## TODO Make the time format the same as AWS uses
    __TIME_FORMAT = "%Y%m%d_%H%M%S"

    __PATTERN_VAULT_STORAGE_FILE = 'vault_%s_%s'
    __KEY_ARCHIVES = 'archives'
    __KEY_LAST_INVENTORY_DATE = 'lastInventoryDate'
    __KEY_UPDATED_DATE = 'updatedDate'
    __KEY_STATUS = 'status'
    
    STATUS_VALUE_READY = 'ready'
    STATUS_VALUE_WAITING_INVENTORY = 'waiting inventory'

    def __init__(self, region, vaultName, storageFolder = "~/.aws"):
        self.region = region
        self.vault = vaultName
        self.shelveFile = self.__PATTERN_VAULT_STORAGE_FILE % (region, vaultName)
        self.storage = ShelveStorage(storageFolder, self.shelveFile)

    def createStorage(self):
        self.setStatus(self.STATUS_VALUE_READY)
        self.setCurrentUpdatedDate()
        self.setArchives({})

    def isStatusReady(self):
        if self.STATUS_VALUE_READY == self.getStatus():
            return True
        return False

    def getStatus(self):
        with self.storage as d:
            if d.has_key(self.__KEY_STATUS):
                return d[self.__KEY_STATUS]
            return None

    def setStatus(self, status = None):
        with self.storage as d:
            d[self.__KEY_STATUS] = status

    def getUpdatedDate(self):
        with self.storage as d:
            if d.has_key(self.__KEY_UPDATED_DATE):
                return d[self.__KEY_UPDATED_DATE]
        return None

    def setUpdatedDate(self, date):
        with self.storage as d:
            d[self.__KEY_UPDATED_DATE] = date

    def setCurrentUpdatedDate(self):
        self.setUpdatedDate(self.__getCurrentTime())

    def getLastInventoryDate(self):
        with self.storage as d:
            if d.has_key(self.__KEY_LAST_INVENTORY_DATE):
                return d[self.__KEY_LAST_INVENTORY_DATE]
        return None

    def setLastInventoryDate(self, date):
        with self.storage as d:
            d[self.__KEY_LAST_INVENTORY_DATE] = date

    def getArchives(self):
        with self.storage as d:
            if d.has_key(self.__KEY_ARCHIVES):
                return d[self.__KEY_ARCHIVES]
        return {}

    def setArchives(self, archives = {}):
        with self.storage as d:
            d[self.__KEY_ARCHIVES] = archives

    def isArchiveExist(self, filename):
        ## TODO check the archive size as well
        ## TODO throw exception if doesn't match
        if self.getArchive(filename) is not None:
            return True
        return False
    
    def getArchive(self, filename):
        archives = self.getArchives()
        if filename in archives:
            return archives[filename]
        return None

    def updateArchive(self, filename, archive = {}):
        archives = self.getArchives()
        archives[archive.filename] = archive
        self.setArchives(archives)

    def removeArchive(self, filename):
        archives = self.getArchives()
        if filename in archives:
            del archives[filename]
            self.setArchives(archives)

    def __getCurrentTime(self):
        return datetime.datetime.now().strftime(self.__TIME_FORMAT)
