# UNDER DEVELOPMENT
## Not all features implemented yet

usage: glacier.py [-h] [--filesystem FILESYSTEM] [--region REGION]
                  [--vault VAULT] [--archive ARCHIVE]
                  {inventory,remove,sync,jobs} ...

AWS Glacier Sync

positional arguments:
  {inventory,remove,sync,jobs}
                        commands
    inventory           {show|request} inventory (default: show all)
    remove              remove archive
    sync                sync folders (default: sync all)
    jobs                Check jobs (default: jobs all)

optional arguments:
  -h, --help            show this help message and exit
  --filesystem FILESYSTEM
                        Filesystem
  --region REGION       Region
  --vault VAULT         Vault
  --archive ARCHIVE     Archive filename


% ./glacier.py inventory show

% ./glacier.py --filesystem=all inventory request

% ./glacier.py --filesystem=aaa/df/sf sync
