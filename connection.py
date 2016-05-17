# -*- coding: utf-8 -*-
# Copyright (c) 2016, 2016 Datacratic Inc.  All rights reserved.
# @Author: Alexis Tremblay
# @Email: atremblay@datacratic.com
# @Date:   2016-05-16 16:53:40
# @Last Modified by:   Alexis Tremblay
# @Last Modified time: 2016-05-17 09:01:07
# @File Name: connection.py


class MLDB(object):
    """docstring for MLDB"""
    def __init__(self, connection):
        super(MLDB, self).__init__()
        self._connection = connection

    @property
    def connection(self):
        if self._connection is None:
            msg = "Connection to MLDB has not been set."
            msg += " You must call set_connection from the connection module"
            msg += " with an MLDB connection"
            raise ConnectionError(msg)
        return self._connection


conn = MLDB(None)


def set_connection(connection):
    global conn
    conn._connection = connection
