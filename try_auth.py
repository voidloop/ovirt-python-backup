#!/usr/bin/env python
# coding: utf-8

from __future__ import print_function

import ovirtsdk4 as sdk
import ovirtsdk4.types as types

import time
try:
    import ConfigParser as configparser
except ImportError:
    import configparser

if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('/root/.ovirtshellrc')

    url = config.get('ovirt-shell', 'url')
    username = config.get('ovirt-shell', 'username')
    password = config.get('ovirt-shell', 'password')

    # open a connection to the server
    connection = sdk.Connection(url=url,
                                username=username,
                                password=password,
                                insecure=True,
                                debug=True)

    # services that manage data and virtual machines
    system_service = connection.system_service()
    events_service = system_service.events_service()

    # use time as unique event id
    event_id = int(time.time())

    # send an event to the system
    events_service.add(
        event=types.Event(origin='test-application',
                          severity=types.LogSeverity.NORMAL,
                          custom_id=event_id,
                          description="Authentication test")
    )

    vms_service = system_service.vms_service()

    vms = vms_service.list(search='name=login*', all_content=True)

    for vm in vms:
        print(vm.name, vm.id)
    
    # close the connection to the server
    connection.close()

