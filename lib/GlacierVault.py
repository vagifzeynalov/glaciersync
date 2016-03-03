'''
Created on Jan 27, 2016

@author: vagif.zeynalov
'''
import os

from lib.VaultStorage import VaultStorage
from lib.GlacierUpload import GlacierUpload
from lib.GlacierClient import GlacierClient

##########################################################################
### NoInventoryException
##########################################################################
class NoInventoryException(Exception):
        def __init__(self, vault):
            self.vault = vault
        def __str__(self):
            return repr("No archives in the vault `%s`, you have to request the inventory or create an empty one" % self.vault)


##########################################################################
### Archive
##########################################################################
class Archive(object):
    def __init__(self, path = None, filename = None):
        self.path = path
        self.filename = filename
        self.description = filename
        if filename is not None: 
            fileStat = os.stat(self.path + "/" + self.filename)
            self.size = fileStat.st_size
        self.archiveId = None
        self.checksum = None
        self.creationDate = None

##########################################################################
### GlacierVault
##########################################################################
class GlacierVault(object):
    
    __KEY_GLACIER_SERVICE = 'glacier'
    __KEY_AWS_ACCESS_KEY_ID = 'awsAccessKeyId'
    __KEY_AWS_SECRET_ACCESS_KEY = 'awsSecretAccessKey'
    __KEY_VAULT_LIST = 'VaultList'
    __KEY_VAULT_NAME = 'VaultName'
    __KEY_LAST_INVENTORY_DATE = 'LastInventoryDate'

    def __init__(self, region, vaultName, awsCredentials = None, partSize = 0):
        self.region = region
        self.vaultName = vaultName
        self.partSize = partSize
        self.vaultStorage = VaultStorage(region, vaultName)
        self.client = GlacierClient(self.region, awsCredentials) 
        self.__createVaultIfRequired()

    def __createVaultIfRequired(self):
        vaultsList = self.client.getVaultsList()
        for vault in vaultsList:
            if self.__KEY_VAULT_NAME in vault and vault[self.__KEY_VAULT_NAME] == self.vaultName:
#                 if self.vaultStorage.getLastInventoryDate() is None:
#                     print "ERROR: The local storage is not ready, request the inventory"
#                     exit(1) ## TODO throw an exception?
                return
        print "Create vault %s" % self.vaultName
        self.client.createVault(self.vaultName)
        self.vaultStorage.createStorage()


    def uploadArchive(self, path, filename):
        if not self.vaultStorage.isStatusReady():
            print "WARNING: The inventory is not ready"
            return

        if self.vaultStorage.isArchiveExist(filename):
            print ">>> Skip %s... (exists in the inventory)" % filename
        else:
            archive = Archive(path, filename)
            print ">>> Uploading %s (%d bytes)..." % (filename, archive.size)
            glacierUpload = GlacierUpload(self.client, self.region, self.vaultName, archive, self.partSize)
            archive.archiveId = glacierUpload.upload()
            if archive.archiveId is not None:
                self.vaultStorage.updateArchive(filename, archive)
                glacierUpload.remove()
            else:
                print "### ERROR: Something went wrong, file %s wasn't archived" % filename
#                 glacierUpload.abortUpload() ### TODO

    def deleteArchive(self, filename):
        if not self.vaultStorage.isArchiveExist(filename):
            print "WARNING: File %s not in the inventory, can't be deleted" % filename
            return
        
        archive = self.vaultStorage.getArchive(filename)
        if self.client.deleteArchive(self.vaultName, archive.archiveId):
            self.vaultStorage.removeArchive(filename)


    def getArchivesList(self):
        archives = self.vaultStorage.getArchives()
        for filename, archive in archives.iteritems():
            print "%s (%s bytes), created %s" % (filename, archive.size, archive.creationDate)


