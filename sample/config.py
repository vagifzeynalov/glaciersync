'''
Configuration file (DEV)
@author: Vagif Zeynalov
'''

# ##
# ## LOCATIONS
# ##
localStorageFolder = "~/.aws"
locations = {}

locations["/music"] = {
            "region": "us-west-2",
            "vault": "mediaMusic",
            "retain": "365d"}

locations["/pictures"] = {
            "region": "us-west-2",
            "vault": "mediaPictures",
            "retain": "365d"}

locations["/videos"] = {
        "region": "us-west-2",
        "vault": "mediaHomeVideo",
        "retain": "365d"}

locations["/archive"] = {
        "region": "us-west-2",
        "vault": "mediaArchive",
        "retain": "365d"}


# ##
# ## AWS
# ##
awsCredentials = {
        "awsAccountId": "00000000000", # put your account id here
        "awsAccessKeyId": "XXXXXXXXXXXXXXXXXXXXX", # put your AWS key ID here
        "awsSecretAccessKey": "YYYYYYYYYYYYYYYYY"} # put your AWS secret key here

partSize = 1024 * 1024 * 1024 # 1Gb

awsRegions = {}
awsRegions["us-west-1"] = {
                    "storageFee": 0.011,
                    "deletionFee": 0.033,
                    "deletionFeePeriod": "90d",
                    "retrievalFee": 0.011,
                    "requestFee": 0.055,
                    "requestFeePer": 1000
                }

awsRegions["us-west-2"] = {
                    "storageFee": 0.007,
                    "deletionFee": 0.021,
                    "deletionFeePeriod": "90d",
                    "retrievalFee": 0.01,
                    "requestFee": 0.050,
                    "requestFeePer": 1000
                }
