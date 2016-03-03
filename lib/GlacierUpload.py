'''
Created on Feb 6, 2016

@author: vagif
'''
import botocore
from ShelveStorage import ShelveStorage

class GlacierUpload:

    __DEFAULT_PART_SIZE = 128 * 1024 * 1024; # 128Mb
    __PATTERN_UPLOAD_STORAGE_FILE = 'upload_%s_%s_%s'
    
    __KEY_UPLOAD_ID = 'uploadId'
    __KEY_PART_SIZE = 'partSize'
    __KEY_CHECKSUM = 'checksum'
    __KEY_ARCHIVE_ID = 'archiveId'
    __KEY_PARTS = 'parts'
    __KEY_RANGE_FROM = 'rangeFrom'
    __KEY_RANGE_TO = 'rangeTo'

    def __init__(self, client, region, vault, archive, partSize = 0, storageFolder = "~/.aws"):
        self.client = client
        self.region = region
        self.vault = vault
        self.archive = archive

        self.uploadId = None
        self.checksum = None
        self.archiveId = None
        self.partSize = partSize
        if self.partSize < 1024 * 1024: # 1Mb
            self.partSize = self.__DEFAULT_PART_SIZE
        self.parts = {}

        self.shelveFile = self.__PATTERN_UPLOAD_STORAGE_FILE % (self.region, self.vault, self.archive.filename)
        self.storage = ShelveStorage(storageFolder, self.shelveFile)

        with self.storage as d:
            if not d.has_key(self.__KEY_PART_SIZE):
                d[self.__KEY_PART_SIZE] = self.partSize
            else:
                self.partSize = d[self.__KEY_PART_SIZE]
            if not d.has_key(self.__KEY_UPLOAD_ID):
                d[self.__KEY_UPLOAD_ID] = self.uploadId
            else:
                self.uploadId = d[self.__KEY_UPLOAD_ID]
            if not d.has_key(self.__KEY_CHECKSUM):
                d[self.__KEY_CHECKSUM] = self.checksum
            else:
                self.checksum = d[self.__KEY_CHECKSUM]
            if not d.has_key(self.__KEY_ARCHIVE_ID):
                d[self.__KEY_ARCHIVE_ID] = self.archiveId
            else:
                self.archiveId = d[self.__KEY_ARCHIVE_ID]
            if not d.has_key(self.__KEY_PARTS):
                d[self.__KEY_PARTS] = self.parts
            else:
                self.parts = d[self.__KEY_PARTS]

    def __store(self, key, value):
        with self.storage as d:
            d[key] = value

    def remove(self):
        if self.uploadId is not None and self.archiveId is None:
            self.abortUpload()
        self.storage.remove()

    def abortUpload(self):
        if self.uploadId is not None:
            return self.client.abortMultipartUpload(self.vault, self.uploadId)
        return False

    def upload(self):
        if self.archiveId is not None:
            print "This file already uploaded, skip"
            self.archive.checksum = self.checksum
            self.archive.archiveId = self.archiveId
            return self.archiveId

        if self.uploadId is None:
            self.__initiateMultipartUpload();

        if self.uploadId is not None:
            print "Upload ID %s" % self.uploadId
            self.__uploadParts();

            self.__completeMultiPartUpload()
            return self.archiveId
        else:
            print "ERROR: Couldn't get the upload ID"
            return None

    def __initiateMultipartUpload(self):
        self.uploadId = self.client.initiateMultipartUpload(self.vault, self.archive.description, self.partSize)
        self.__store(self.__KEY_UPLOAD_ID, self.uploadId)
        return self.uploadId

    def __uploadParts(self):
        with open(self.archive.path + "/" + self.archive.filename, "rb") as f:
            if self.checksum is None:
                print "Calculating the file's checksum... ",
                ## TODO calculate checksum while uploading
                self.checksum = botocore.utils.calculate_tree_hash(f)
                self.__store(self.__KEY_CHECKSUM, self.checksum)
            else:
                print "The file's checksum already calculated. ",

            print "Checksum = %s" % self.checksum
            self.archive.checksum = self.checksum

            filePos = 0
            while True:
                if filePos not in self.parts:
                    f.seek(filePos)
                    chunk = f.read(self.partSize)
                    if chunk:
                        part = dict()
                        part[self.__KEY_RANGE_FROM] = filePos
                        part[self.__KEY_RANGE_TO] = part[self.__KEY_RANGE_FROM] + chunk.__len__() - 1
                        rangeStr = "%d-%d" % (part[self.__KEY_RANGE_FROM], part[self.__KEY_RANGE_TO])
                        ## TODO add a timestamp
                        print "Uploading range %s... " % rangeStr, 
                        ## TODO Calculate a percentage from the file size

                        checksum = self.client.uploadMultipartPart(self.vault, self.uploadId, rangeStr, chunk)
                        ## TODO catch exception
                        if checksum is not None:
                            print "Checksum = %s" % checksum
                            ## TODO Calculate the speed (bps)
                            ## TODO Calculate ETA
                            part[self.__KEY_CHECKSUM] = checksum
                            self.parts[part[self.__KEY_RANGE_FROM]] = part
                            self.__store(self.__KEY_PARTS, self.parts)
                        else:
                            print "ERROR: Something went wrong, no checksum for this part"
                    else:
                        break
                else:
                    part = self.parts[filePos]
                    print ("Range %d-%d already uploaded (Checksum %s), skip it" % 
                                (part[self.__KEY_RANGE_FROM], part[self.__KEY_RANGE_TO], part[self.__KEY_CHECKSUM]))

                filePos += self.partSize

    def __completeMultiPartUpload(self):
        if self.uploadId is None:
            print "### ERROR: No uploadId"
            return
        if self.checksum is None:
            print "### ERROR: Checksum is not calculated"
            return

        ## TODO check if all parts were uploaded
        archiveId = self.client.completeMultipartUpload(self.vault, self.uploadId, self.archive.size, self.checksum)
        if archiveId is not None:
            print "ArchiveId = %s" % archiveId
            self.archiveId = archiveId
            self.archive.archiveId = archiveId
            self.__store(self.__KEY_ARCHIVE_ID, self.archiveId)
        else:
            print "ERROR: Something went wrong, no archiveId"
