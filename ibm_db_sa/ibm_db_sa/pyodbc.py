# +--------------------------------------------------------------------------+
# |  Licensed Materials - Property of IBM                                    |
# |                                                                          |
# | (C) Copyright IBM Corporation 2008, 2013.                                |
# +--------------------------------------------------------------------------+
# | This module complies with SQLAlchemy 0.8 and is                          |
# | Licensed under the Apache License, Version 2.0 (the "License");          |
# | you may not use this file except in compliance with the License.         |
# | You may obtain a copy of the License at                                  |
# | http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable |
# | law or agreed to in writing, software distributed under the License is   |
# | distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY |
# | KIND, either express or implied. See the License for the specific        |
# | language governing permissions and limitations under the License.        |
# +--------------------------------------------------------------------------+
# | Authors: Jaimy Azle, Rahul Priyadarshi                                   |
# | Contributors: Mike Bayer                                                 |
# | Version: 0.3.x                                                           |
# +--------------------------------------------------------------------------+
from sqlalchemy import util
import urllib
from sqlalchemy.connectors.pyodbc import PyODBCConnector
from .base import _SelectLastRowIDMixin, DB2ExecutionContext, DB2Dialect



class DB2ExecutionContext_pyodbc(_SelectLastRowIDMixin, DB2ExecutionContext):
    pass

class DB2Dialect_pyodbc(PyODBCConnector, DB2Dialect):

    execution_ctx_cls = DB2ExecutionContext_pyodbc

    pyodbc_driver_name = "IBM DB2 ODBC DRIVER"

    def create_connect_args(self, url):
        opts = url.translate_connect_args(username='user')
        opts.update(url.query)

        keys = opts
        query = url.query

        connect_args = {}
        for param in ('ansi', 'unicode_results', 'autocommit'):
            if param in keys:
                connect_args[param] = util.asbool(keys.pop(param))

        if 'odbc_connect' in keys:
            connectors = [urllib.unquote_plus(keys.pop('odbc_connect'))]
        else:
            dsn_connection = 'dsn' in keys or \
                                    ('host' in keys and 'database' not in keys)
            if dsn_connection:
                connectors = ['dsn=%s' % (keys.pop('host', '') or \
                                            keys.pop('dsn', ''))]
            else:
                port = ''
                if 'port' in keys and not 'port' in query:
                    port = '%d' % int(keys.pop('port'))

                database = keys.pop('database', '')

                connectors = ["DRIVER={%s}" %
                                keys.pop('driver', self.pyodbc_driver_name),
                            'hostname=%s;port=%s' % (keys.pop('host', ''), port),
                            'database=%s' % database]

                user = keys.pop("user", None)
                if user:
                    connectors.append("uid=%s" % user)
                    connectors.append("pwd=%s" % keys.pop('password', ''))
                else:
                    connectors.append("trusted_connection=yes")

                # if set to 'yes', the odbc layer will try to automagically
                # convert textual data from your database encoding to your
                # client encoding.    this should obviously be set to 'no' if
                # you query a cp1253 encoded database from a latin1 client...
                if 'odbc_autotranslate' in keys:
                    connectors.append("autotranslate=%s" %
                                            keys.pop("odbc_autotranslate"))

                connectors.extend(['%s=%s' % (k, v)
                                        for k, v in keys.iteritems()])
        return [[";".join(connectors)], connect_args]

class AS400Dialect_pyodbc(PyODBCConnector, DB2Dialect):

    pyodbc_driver_name = "IBM DB2 ODBC DRIVER"


