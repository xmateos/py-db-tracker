# -*- coding: utf-8 -*-

__all__ = ["MySQLServer"]

__author__ = 'Xavi Mateos <xmateos(at)gmail.com>'
__license__ = 'GPL'
__version__ = '0.6.3'

import re
import mysql.connector
from mysql.connector import errorcode
from collections import defaultdict
from collections import OrderedDict
import Texttable


"""******************************************
    HERE'S THE SETUP
******************************************"""
# MySQL connection setup
userMySQL = 'root'
passwordMySQL = ''
hostMySQL = '127.0.0.1'

# Databases to be checked; other databases will be skipped; if set, it has priority over 'excludedDatabases'
checkOnlyDatabases = []
# Databases that won´t be checked
excludedDatabases = ['information_schema', 'performance_schema', 'mysql', 'test']
# Tables to be checked; other tables will be skipped; if set, it has priority over 'excludedTables'
checkOnlyTables = []
# Tables that won´t be checked
excludedTables = []
# Columns to be checked; other columns will be skipped; if set, it has priority over 'excludedColumns'
checkOnlyColumns = []
# Columns that won´t be checked
excludedColumns = []

# if False, it will prompt the user using Python wx.TextEntryDialog (Python wx module required)
consoleMode = False

"""******************************************************************************************************
    END SETUP (don't touch from this point!) :)

    .:: For MySQL database servers ONLY ::.
    .:: Tested with UTF-8 DB collation ::.

    Requirements:
        - Python >= 2.7
        - MySQL connector for Python (http://dev.mysql.com/downloads/connector/python/2.0.html)
        - TextTable Python script for table style output (http://foutaise.org/code/texttable/texttable)
******************************************************************************************************"""


class MySQLServer:
    def __init__(self, host='127.0.0.1', user='root', password=''):
        self.cnx = None
        self.cursor = None
        self.hostMySQL = host
        self.userMySQL = user
        self.passwordMySQL = password
        self.config = {
            'user': self.userMySQL,
            'password': self.passwordMySQL,
            'host': self.hostMySQL
        }
        self.checkOnlyDatabases = []
        self.checkOnlyTables = []
        self.checkOnlyColumns = []
        self.excludedDatabases = []
        self.excludedTables = []
        self.excludedColumns = []

    def setConnectionParams(self, data):
        self.config = data

    def __enter__(self):
        return self

    def __exit__(self):
        self.cnx.close()

    def connect(self):
        try:
            self.cnx = mysql.connector.connect(**self.config)
            self.cursor = self.cnx.cursor()
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your username or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exists")
            else:
                print(err)

    def query(self, sql):
        try:
            self.cursor.execute(sql)
            return self.cursor
        except mysql.connector.Error as err:
            print err, '\n', sql
            exit()

    def close(self):
        self.__exit__()

    def getPrimaryKeys(self, tableName):
        #sql = "SHOW KEYS FROM " + str(tableName) + " WHERE Key_name = 'PRIMARY'"
        sql = "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE table_name = '" + str(tableName) + "' AND COLUMN_KEY = 'PRI'"
        return self.getResults(sql)

    def getResults(self, query):
        result = self.query(query)
        return result.fetchall()

    def getDatabases(self):
        sql = "SHOW DATABASES;"
        return self.getResults(sql)

    def getTables(self):
        sql = "SHOW TABLES;"
        return self.getResults(sql)

    def getColumns(self, tableName):
        sql = "SHOW COLUMNS FROM " + str(tableName) + ";"
        return self.getResults(sql)

    def findMatches(self, tableName, primaryKeys, colName, searchTerm):
        #  'collate utf8_bin' makes the query case sensitive (accents)
        sql = "SELECT " + '`, `'.join(primaryKeys) + ", `" + colName + "` AS `" + colName + "` FROM `" + tableName + "` WHERE LOWER(`" + colName + "`) LIKE '%" + searchTerm + "%' collate utf8_bin"
        return self.getResults(sql)

    def setParam(self, param, value):
        setattr(self, param, value)

    def formatLargeTextResult(self, haystack, needle, charsBeforeAndAfter = 40):
        matches = [m.start() for m in re.finditer(needle, haystack.lower())]
        results = []
        for output in matches:
            begin = output - charsBeforeAndAfter
            end = output + len(needle) + charsBeforeAndAfter
            out = []
            if begin > 0:
                out.append('...')
            else:
                begin = 0
            if end < len(haystack):
                out.append(haystack[begin:end] + '...')
            else:
                out.append(haystack[begin:end])
            results.append(''.join(out))
        return '\n'.join(results)

    def track(self, searchTerm):
        self.connect()
        searchTerm = searchTerm.lower()

        #  Loop databases
        dbs = server.getDatabases()
        if len(self.checkOnlyDatabases) > 0:
            dbs = (db for db in dbs if db[0] in self.checkOnlyDatabases)
        else:
            dbs = (db for db in dbs if db[0] not in self.excludedDatabases)
        for db in dbs:
            dbName = db[0]
            config = {
                'user': self.userMySQL,
                'password': self.passwordMySQL,
                'host': self.hostMySQL,
                'database': dbName,
                'charset': 'utf8',
                'use_unicode': True
            }
            server.setConnectionParams(config)
            server.connect()

            #  Loop tables
            tables = server.getTables()
            if len(self.checkOnlyTables) > 0:
                tables = (table for table in tables if table[0] in self.checkOnlyTables)
            else:
                tables = (table for table in tables if table[0] not in self.excludedTables)
            for table in tables:
                tableName = table[0]

                resultRowValues = []

                # Get table primary keys
                primary_keys = []
                for pkey in self.getPrimaryKeys(tableName):
                    primary_keys.append(pkey[0])

                cols = server.getColumns(tableName)
                if len(self.checkOnlyColumns) > 0:
                    cols = (col for col in cols if col[0] in self.checkOnlyColumns)
                else:
                    cols = (col for col in cols if col[0] not in self.excludedColumns)

                # String types for MySQL
                stringVars = ['char', 'varchar', 'binary', 'varbinary', 'blob', 'text', 'enum', 'set']

                #  Loop string columns
                cols = (col for col in cols if any(stringVar in col[1].lower() for stringVar in stringVars))

                for col in cols:
                    colName = col[0]
                    results = server.findMatches(tableName, primary_keys, colName, searchTerm)
                    for r in results:
                        resultData = {}
                        resultData['db'] = dbName
                        resultData['table'] = tableName
                        resultData['primary_keys'] = {}

                        pKeysOutput = []
                        for id, pKey in enumerate(pKey for pKey in primary_keys if pKey != colName):
                            pKeyValue = r[id]
                            if type(pKeyValue) != 'str':
                                pKeyValue = str(pKeyValue)
                            pKey = str(pKey)
                            pKeysOutput.append(pKey + ': ' + pKeyValue)
                            resultData['primary_keys'][pKey] = pKeyValue

                        """ OLD RENDER
                        print dbName + ' > ' + tableName + ' > ' + colName + ' -> IDs: ' + ' / '.join(pKeysOutput)
                        match = r[(len(r)-1)]
                        if len(match) > 40:
                            print self.formatLargeTextResult(match, searchTerm, 40)
                        else:
                            print match
                        """

                        resultData['matches'] = {}
                        match = r[(len(r)-1)]
                        if len(match) > 40:
                            resultData['matches'][colName] = self.formatLargeTextResult(match, searchTerm, 40)
                        else:
                            resultData['matches'][colName] = match
                        resultRowValues.append(resultData)

                # Get primary keys all matching columns from table
                groupByTable = defaultdict(list)
                for result in resultRowValues:
                    for primary_key in result['primary_keys']:
                            if primary_key not in groupByTable[result['table']]:
                                groupByTable[result['table']].append(primary_key)
                    for matching_column in result['matches']:
                        if matching_column not in groupByTable[result['table']]:
                            groupByTable[result['table']].append(matching_column)

                tab = Texttable.Texttable()
                header = []
                resultsTable = OrderedDict()
                maxColWidths = OrderedDict()

                for g in groupByTable:
                    for col in groupByTable[g]:
                        header.append(col)
                        resultsTable[col] = ''
                        maxColWidths[col] = len(col)+2
                tab.header(header)

                groupByPrimaryKeys = defaultdict(list)
                for result in resultRowValues:
                    for primary_key in result['primary_keys']:
                        groupByPrimaryKeys[result['primary_keys'][primary_key]].append(result['matches'])
                        if not any(primary_key in id for id in groupByPrimaryKeys[result['primary_keys'][primary_key]]):
                            groupByPrimaryKeys[result['primary_keys'][primary_key]].append({
                                primary_key.decode('utf-8'): result['primary_keys'][primary_key]
                            })
                for g in groupByPrimaryKeys:
                    row2add = resultsTable
                    for r in resultsTable:
                        if any(r in d for d in groupByPrimaryKeys[g]):
                            for dict in groupByPrimaryKeys[g]:
                                if dict.get(r):
                                    row2add[r] = dict.get(r)
                                    if maxColWidths[r] < len(dict.get(r)):
                                        if len(dict.get(r)) < 50:
                                            maxColWidths[r] = len(dict.get(r))
                                        else:
                                            maxColWidths[r] = 50
                        else:
                            row2add[r] = '-'
                    row2addList = []
                    for key, value in row2add.iteritems():
                        row2addList.append(value.encode('utf-8'))
                    tab.add_row(row2addList)

                colWidths = []
                for key, value in maxColWidths.iteritems():
                    colWidths.append(value)
                if len(resultRowValues) > 0:
                    tab.set_cols_width(colWidths)
                    print '\nTABLE ---->>> ' + dbName + '.' + tableName
                    t = tab.draw()
                    print t

if consoleMode:
    # Request console search term input
    searchTerm = raw_input("Enter your search term: ").decode('utf8')
else:
    # Request GUI search term input
    import wx
    app = wx.App()
    app.MainLoop()
    dlg = wx.TextEntryDialog(None, "Enter your search term:", "dbTrAckEr", "")
    answer = dlg.ShowModal()
    if answer == wx.ID_OK:
        searchTerm = dlg.GetValue()
        dlg.Destroy()
    else:
        dlg.Destroy()
        exit()

# Force user input without prompt
#searchTerm = u'tá'

server = MySQLServer(hostMySQL, userMySQL, passwordMySQL)
server.setParam('checkOnlyDatabases', checkOnlyDatabases)
server.setParam('excludedDatabases', excludedDatabases)
server.setParam('checkOnlyTables', checkOnlyTables)
server.setParam('excludedTables', excludedTables)
server.setParam('checkOnlyColumns', checkOnlyColumns)
server.setParam('excludedColumns', excludedColumns)

server.track(searchTerm)