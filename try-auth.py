#!/usr/bin/env python
# coding: utf-8

from __future__ import print_function

import ovirtsdk4 as sdk
import ovirtsdk4.types as types

import ConfigParser 


#------------------------------------------------------------------------------
if __name__ == '__main__':
    config = ConfigParser.ConfigParser()
    config.read('/root/.ovirtshellrc')

    url = config.get('ovirt-shell', 'url')
    username = config.get('ovirt-shell', 'username')
    password = config.get('ovirt-shell', 'password')

    connection = sdk.Connection(url=url,
                                username=username,
                                password=password,
                                insercure=True
                                debug=True)

    system_service = connection.system_service()
    events_service = system_service.events_service()
    vms_service = system_service.vms_service()

    vm_list = vms_service.list(all_content=True)
    print(vm_list)

