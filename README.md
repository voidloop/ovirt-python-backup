# Python oVirt backup script

A script to backup oVirt virtual machines.

# Requirements
* python3 (though python2 might keep working for a while)
* ovirtsdk4 python library (python-ovirt-engine-sdk4 on Centos, other distributions may use different package names)

# Setup
* Set up a VM to do the backups, give the VM a path to backup to (this could be an NFS share that is also used for oVirt exports).
* Clone this repository
* Set up '~/.ovirtshellrc', an example is at `example.ovirtshellrc` (https://access.redhat.com/documentation/en-us/red_hat_virtualization/4.0/html/rhevm_shell_guide/ovirtshellrc_configuration), at least the url, username and password (under ovirt-shell)
* Make sure that the directory ~/log/ exists and is writable by your user.
* If running as a non-root user, add your user to the "disk" group (on EL based distributions):
  usermod -a -G disk backupuser
* If you want to use the automatic linking of back-ups to the export-domain, make sure the files are readable by vdsm ( uid = 36, gid = 36 ), one way to accomplish this is using a backupuser with uid and gid 36. You could also use the all_squash option on your NFS server.

# Usage:
```
usage: backupvm.py [-h] [-b BASEDIR] [-a AGENTVM] [-e EXPORT_DOMAIN]
                   [-n VERSIONS] [-m]
                   vmname

positional arguments:
  vmname                Basedir for backups. (MANDATORY)

optional arguments:
  -h, --help            show this help message and exit
  -b BASEDIR, --basedir BASEDIR
                        Basedir for backups. (MANDATORY)
  -a AGENTVM, --agentvm AGENTVM
                        Name of the VM to attach the disks to (normally the VM
                        where this script is running). (MANDATORY)
  -e EXPORT_DOMAIN, --export-domain EXPORT_DOMAIN
                        UID of the export domain to link the backup to for
                        easy restores, this should be a directory in the root
                        of the basedir. (OPTIONAL)
  -n VERSIONS, --numkeep VERSIONS
                        Number of versions to keep. (OPTIONAL, default = 7)
  -m, --migrate-vm      Try to migrate the agent VM to the same host as the VM
                        to back-up, the VMs have to be in the same oVirt
                        cluster.
```
