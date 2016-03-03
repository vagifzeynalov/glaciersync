'''
Created on Feb 6, 2016

@author: vagif
'''

import boto3
import json

class GlacierClient(object):
    __KEY_GLACIER_SERVICE = 'glacier'
    __KEY_AWS_ACCESS_KEY_ID = 'awsAccessKeyId'
    __KEY_AWS_SECRET_ACCESS_KEY = 'awsSecretAccessKey'

    __KEY_UPLOAD_ID = 'uploadId'
    __KEY_CHECKSUM = 'checksum'
    __KEY_ARCHIVE_ID = 'archiveId'
    
    __KEY_VAULT_LIST = 'VaultList'
    
    def __init__(self, region=None, awsCredentials=None):
        '''
        Constructor
        '''
        if region is not None and awsCredentials is not None:
            self.client = boto3.client(
               self.__KEY_GLACIER_SERVICE,
               region_name = region, 
               aws_access_key_id = awsCredentials[self.__KEY_AWS_ACCESS_KEY_ID], 
               aws_secret_access_key = awsCredentials[self.__KEY_AWS_SECRET_ACCESS_KEY])
        # TODO throw exception


    ## ARCHIVE
    def abortMultipartUpload(self, vault=None, uploadId=None):
        '''
        Abort Multipart Upload
        Return: True or False
        '''
        if vault is not None and uploadId is not None:
            response = self.client.abort_multipart_upload(
                vaultName = self.vault,
                uploadId = self.uploadId)
            # TODO check the response code
            return True
        return False


    def initiateMultipartUpload(self, vaultName=None, description=None, partSize=None):
        '''
        Initiate Multipart Upload
        Return: uploadId
        '''
        if vaultName is not None and description is not None and partSize is not None:
            response = self.client.initiate_multipart_upload(
                vaultName = vaultName,
                archiveDescription = description,
                partSize = str(partSize)
            )
            # TODO check the response code
            if self.__KEY_UPLOAD_ID in response:
                return response[self.__KEY_UPLOAD_ID]
        return None


    def uploadMultipartPart(self, vaultName=None, uploadId=None, rangeStr=None, data=None):
        '''
        Upload Multipart Part
        Return: checksum
        '''
        if vaultName is not None and uploadId is not None and rangeStr is not None and data is not None:
            response = self.client.upload_multipart_part(
                vaultName = vaultName,
                uploadId = uploadId,
                range = "bytes %s/*" % rangeStr,
                body = data
            )
            # TODO check the response code
            if response is not None and self.__KEY_CHECKSUM in response:
                return response[self.__KEY_CHECKSUM]
            else:
                print "ERROR: Something went wrong, no checksum for this part"
                # TODO throw exception
        return None


    def completeMultipartUpload(self, vaultName=None, uploadId=None, archiveSize=None, checksum=None):
        '''
        Complete Multipart Upload
        Return: archiveId
        '''
        if vaultName is not None and uploadId is not None and archiveSize is not None and checksum is not None:
            response = self.client.complete_multipart_upload(
                vaultName = vaultName,
                uploadId = uploadId,
                archiveSize = str(archiveSize),
                checksum = checksum
            )
            # TODO check the response code
            if response is not None and self.__KEY_ARCHIVE_ID in response:
                return response[self.__KEY_ARCHIVE_ID]
            else:
                print "ERROR: Something went wrong, no archiveId"
                # TODO throw exception
        return None


    def deleteArchive(self, vaultName=None, archiveId=None):
        '''
        Delete Archive
        Return: True or False
        '''
        if vaultName is not None and archiveId is not None:
            response = self.client.delete_archive(
                vaultName = vaultName,
                archiveId = archiveId
            )
            # TODO check the response code
            return True
        return False


    ## VAULT
    def getVaultsList(self):
        '''
        Get Vaults List
        Return: vaultList or None
        '''
        response = self.client.list_vaults()
        if self.__KEY_VAULT_LIST in response:
            return response[self.__KEY_VAULT_LIST]
        return []


    def createVault(self, vault):
        self.client.create_vault(vaultName = vault)
        # TODO check the response code
        return True


    ## JOB
    def initiateJob(self, vaultName=None, jobParameters = {}):
        response = self.client.initiate_job(
            vaultName= vaultName,
            jobParameters= jobParameters
        )
        # TODO check the response code
        return True


    def getJobs(self, vaultName=None, action=None):
        response = self.client.list_jobs(
            vaultName= vaultName
        )
        if response is not None and response['ResponseMetadata']['HTTPStatusCode'] == 200:
            jobList = response['JobList']
            if len(jobList) == 0:
                return []
            if action is None:
                return jobList

            jobsForAction = []
            for job in jobList:
                if job['Action'] == action:
                    jobsForAction.append(job)
            return jobsForAction


    def getJobOutput(self, vaultName=None, jobId=None):
        response = self.client.get_job_output(
            vaultName= vaultName,
            jobId= jobId
        )
        # TODO check the response code
        return json.load(response['body'])

