#!/usr/bin/env python
'''
Created on Jan 18, 2016

@author: vagif
'''

import os
import time
import datetime
import argparse
from lib.config import Config
from lib.GlacierVault import GlacierVault
from lib.GlacierJob import GlacierJob

def sync(argLocation=None, argVault=None, argRegion=None, argConfigFile=None):
    config = Config(argConfigFile)
    
    locations = config.getLocations()
    for location, locationConfig in locations.iteritems():
        if argLocation is not None and location != argLocation:
            continue # SKIP
        if argRegion is not None and argRegion != locationConfig['region']:
            continue # SKIP
        if argVault is not None and argVault != locationConfig['vault']:
            continue # SKIP
        
        print "Processing %s" % location
        vault = GlacierVault(
                             locationConfig['region'], 
                             locationConfig['vault'], 
                             awsCredentials = config.getAWScredentials(), 
                             partSize = config.getAWSPartSize())

        for archiveFilename in os.listdir(location):
            filePath = os.path.join(location, archiveFilename)
            if os.path.isdir(filePath):
                # skip directories
                continue
            
            attempts = 5
            startTime = datetime.datetime.now()
            while attempts > 0:
                try:
                    vault.uploadArchive(location, archiveFilename)
                    break
                except Exception:
                    timediff = datetime.datetime.now() - startTime
                    print "### DEBUG: Timediff %d seconds" % timediff.total_seconds()
                    if timediff.total_seconds() > 60:
                        print "### DEBUG: reset attempts"
                        attempts = 5

                    attempts -= 1
                    print "Exception happened, repeat (%d attempts left)" % attempts
                    time.sleep(5) # delays for 5 seconds

def inventory(argOption=None, argLocation=None, argVault=None, argRegion=None, argConfigFile=None):
    if argLocation is None:
        argLocation = 'ALL'
    argOption = str.upper(argOption)
    print "### %s INVENTORY for '%s' location(s)" % (argOption, argLocation)
 
    config = Config(argConfigFile)
    locations = config.getLocations()
    for location, locationConfig in locations.iteritems():
        if argLocation != 'ALL' and location != argLocation:
            continue # SKIP
        if argRegion is not None and argRegion != locationConfig['region']:
            continue # SKIP
        if argVault is not None and argVault != locationConfig['vault']:
            continue # SKIP
          
        print "Processing %s" % location
        glacierJob = GlacierJob(
                             locationConfig['region'], 
                             locationConfig['vault'], 
                             awsCredentials = config.getAWScredentials())
        
        if 'SHOW' == argOption:
            glacierJob.showInventory()
        else:
            glacierJob.requestInventory()


def jobs(argLocation=None, argVault=None, argRegion=None, argConfigFile=None):
#         vault.checkJobs()
    config = Config(argConfigFile)

    locations = config.getLocations()
    for location, locationConfig in locations.iteritems():
        if argLocation is not None and location != argLocation:
            continue # SKIP
        if argRegion is not None and argRegion != locationConfig['region']:
            continue # SKIP
        if argVault is not None and argVault != locationConfig['vault']:
            continue # SKIP
        
        print "Processing %s" % location
        glacierJob = GlacierJob(
                             locationConfig['region'], 
                             locationConfig['vault'], 
                             awsCredentials = config.getAWScredentials())
        glacierJob.checkJobs()


###
### MAIN
###
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='AWS Glacier Sync')
    parser.add_argument('--path', default=None, help='Path to archives')
    parser.add_argument('--region', default=None, help='Region')
    parser.add_argument('--vault', default=None, help='Vault')
    parser.add_argument('--archive', default=None, help='Archive filename')
    parser.add_argument('--config', default=None, help='Configuration filename')

    subparsers = parser.add_subparsers(help='commands', dest='command')

    inventory_parser = subparsers.add_parser('inventory', help='{show|request} inventory (default: show all)')
    inventory_subparsers = inventory_parser.add_subparsers(help='commands', dest='option')
    inventory_show_parser = inventory_subparsers.add_parser('show', help='Show inventory')
    inventory_request_parser = inventory_subparsers.add_parser('request', help='Request inventory')

    remove_parser = subparsers.add_parser('remove', help='remove archive')

    sync_parser = subparsers.add_parser('sync', help='sync folders (default: sync all)')

    jobs_parser = subparsers.add_parser('jobs', help='Check jobs (default: jobs all)')

    args = parser.parse_args()
    print(args)
 
    if args.command == 'inventory':
        inventory(args.option, args.path, args.vault, args.region, args.config)
    elif args.command == 'remove':
        print "remove is not implemented yet" # TODO
    elif args.command == 'sync':
        sync(args.path, args.vault, args.region, args.config)
    elif args.command == 'jobs':
        jobs(args.path, args.vault, args.region, args.config)

    print "Done."
