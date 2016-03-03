'''
Created on Feb 6, 2016

@author: vagif
'''

from lib.GlacierClient import GlacierClient
from lib.ShelveStorage import ShelveStorage
from lib.VaultStorage import VaultStorage
from lib.GlacierVault import Archive

class Job(object):
    
    def __init__(self):
        self.jobId = None
        
        

class GlacierJob(object):
    '''
    classdocs
    '''

    __PATTERN_VAULT_STORAGE_FILE = 'jobs_%s_%s'

    __KEY_JOBS = 'jobs'

    __JOB_ACTION_ARCHIVE_RETRIEVAL = 'ArchiveRetrieval'
    __JOB_ACTION_INVENTORY_RETRIEVAL = 'InventoryRetrieval'

    def __init__(self, region, vaultName, awsCredentials, storageFolder = "~/.aws"):
        '''
        Constructor
        '''
        self.region = region
        self.vault = vaultName
        self.client = GlacierClient(self.region, awsCredentials)
        self.shelveFile = self.__PATTERN_VAULT_STORAGE_FILE % (region, vaultName)
        self.jobsStorage = ShelveStorage(storageFolder, self.shelveFile)
        self.vaultStorage = VaultStorage(region, vaultName, storageFolder)


    def checkJobs(self):
        jobsList = self.client.getJobs(self.vault)
        if jobsList is None or len(jobsList) == 0:
            print "No any jobs"
            return

        foundCompeledJob = False
        for job in jobsList:
            if job['Completed']:
                foundCompeledJob = True
                print "`%s` job completed" % job['JobDescription']
                if job['Action'] == self.__JOB_ACTION_ARCHIVE_RETRIEVAL:
                    self.__downloadFile(job)

                if job['Action'] == self.__JOB_ACTION_INVENTORY_RETRIEVAL:
                    self.__downloadInventory(job)

        if foundCompeledJob: 
            self.__cleanUpJobs(jobsList)
        else:
            print "No completed jobs yet"


    def __cleanUpJobs(self, jobsList):
        completedJobs = {}
        for job in jobsList:
            if job['Completed']:
                completedJobs[job['JobId']] = job

        with self.jobsStorage as d:
            if d.has_key(self.__KEY_JOBS):
                processedJobs = d[self.__KEY_JOBS]
                for jobid,job in completedJobs.iteritems():
                    if jobid not in processedJobs:
                        del processedJobs[jobid]

    def __getProcessedJobs(self):
        with self.jobsStorage as d:
            if d.has_key(self.__KEY_JOBS):
                return d[self.__KEY_JOBS]
            else:
                return {}


    def __saveProcessedJob(self, job):
        with self.jobsStorage as d:
            processedJobs = {}
            if d.has_key(self.__KEY_JOBS):
                processedJobs = d[self.__KEY_JOBS]
            processedJobs[job['JobId']] = job
            d[self.__KEY_JOBS] = processedJobs


    def __downloadInventory(self, job = {}):
        processedJobs = self.__getProcessedJobs()
        if processedJobs.has_key(job['JobId']):
            print "Job %s was already processed. Skip" % job['JobId']
            return
        
        print ">>> Downloading inventory"
        inventory = self.client.getJobOutput(self.vault, job['JobId'])
        self.vaultStorage.setLastInventoryDate(inventory['InventoryDate'])
        self.vaultStorage.setCurrentUpdatedDate()
        for archive in inventory['ArchiveList']:
            localArchive = self.vaultStorage.getArchive(archive['ArchiveDescription'])
            if localArchive is None:
                localArchive = Archive()
                localArchive.archiveId = archive['ArchiveId']
                localArchive.description = archive['ArchiveDescription']
                localArchive.filename = archive['ArchiveDescription']
                localArchive.creationDate = archive['CreationDate']
                localArchive.checksum = archive['SHA256TreeHash']
                localArchive.size = archive['Size']
                self.vaultStorage.updateArchive(localArchive.filename, localArchive)
                print ">>> New file added into the local inventory `%s`" % localArchive.filename
            else:
                if localArchive.creationDate is None:
                    localArchive.creationDate = archive['CreationDate']
                    self.vaultStorage.updateArchive(localArchive.filename, localArchive)
                    print ">>> File updated in the local inventory `%s`" % localArchive.filename
        self.vaultStorage.setStatus(VaultStorage.STATUS_VALUE_READY)
        self.__saveProcessedJob(job)


    def requestInventory(self):
        jobsList = self.client.getJobs(self.vault, self.__JOB_ACTION_INVENTORY_RETRIEVAL)
        if len(jobsList) == 0:
            print "Requesting inventory"
            jobParameters = {
                'Type': 'inventory-retrieval',
                'Description': 'Inventory retrieval job'
            }
            try:
                self.client.initiateJob(self.vault, jobParameters)
                self.vaultStorage.setStatus(VaultStorage.STATUS_VALUE_WAITING_INVENTORY)
            except:
                self.vaultStorage.createStorage() ## TODO not sure
        else:
            print "WARNING: Wait until the inventory job(s) will complete"


    def showInventory(self):
        if not self.vaultStorage.isStatusReady():
            print "WARNING: The inventory is not ready"
            return

        archives = self.vaultStorage.getArchives()
        if len(archives) > 0:
            for archive in archives.itervalues():
                print "'%s' (%d) %s" % (archive.filename, archive.size, archive.creationDate)
        else:
            print "No files in the local storage. Perhaps you should request the inventory"

    ## TODO Issue #11: Download requested file
    def __downloadFile(self, job = {}):
        print ""

    ## TODO Issue #10: Request file (archive) for download
    def requestFile(self):
        print ""
#             jobParameters = {
#                 'Type': 'archive-retrieval',
#                 'Description': 'Inventory retrieval job'
#             }

