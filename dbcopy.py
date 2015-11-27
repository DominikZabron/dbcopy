#!/usr/bin/env python

import MySQLdb
import MySQLdb.cursors as cursors
from os import system
from sys import exit

#SETTINGS
SOURCE_TBL_NAME = 'titles'
SOURCE_DB_HOST = ''
SOURCE_DB_USER = ''
SOURCE_DB_PASSWORD = ''
SOURCE_DB_NAME = 'employees'

DESTINATION_DB_HOST = SOURCE_DB_HOST
DESTINATION_DB_USER = SOURCE_DB_USER
DESTINATION_DB_PASSWORD = SOURCE_DB_PASSWORD
DESTINATION_DB_NAME = SOURCE_DB_NAME + '_copy'

class CopyMysqlTbl(object):
    """Provide features for transferring data between tables.

    Require input values necessary for connection to source and destination
    servers: host, username, password and database name for each and table name
    which data are going to be transfered. They are made public arguments
    for further use in class methods.

    Attributes:
        src_conn : MySQLdb connection object to source database
        dest_conn : MySQLdb connection object to destination database
        sc : cursor for source connection
        dc : cursor for destination connection

    """

    def __init__(
        self,
        src_table=SOURCE_TBL_NAME,
        src_host=SOURCE_DB_HOST, 
        src_user=SOURCE_DB_USER,
        src_passwd=SOURCE_DB_PASSWORD,
        src_db=SOURCE_DB_NAME,
        dest_host=DESTINATION_DB_HOST,
        dest_user=DESTINATION_DB_USER,
        dest_passwd=DESTINATION_DB_PASSWORD,
        dest_db=DESTINATION_DB_NAME):
        
        #SSCursor class used for server-side cursor
        #This can prevent for using too much memory at once
        self.src_conn = MySQLdb.connect(
            host=src_host,
            user=src_user,
            passwd=src_passwd,
            db=src_db,
            cursorclass=cursors.SSCursor)

        self.dest_conn = MySQLdb.connect(
            host=dest_host,
            user=dest_user,
            passwd=dest_passwd,
            db=dest_db)

        self.sc = self.src_conn.cursor()
        self.dc = self.dest_conn.cursor()
        self.tbl_name = src_table
        self.src_user = src_user
        self.src_passwd = src_passwd
        self.src_db = src_db
        self.dest_host =  dest_host
        self.dest_user = dest_user
        self.dest_passwd = dest_passwd
        self.dest_db = dest_db

    def exit(self, arg=0):
        """Close open objects and call sys.exit"""
        self.sc.close()
        self.src_conn.close()
        self.dc.close()
        self.dest_conn.close()
        exit(arg)

    def dump_except(self):
        """Copy database content other then target table.

        Using passwords here is insecure, be sure you are safe.
        """
        instr = """
                mysqldump -u {0} -p{1} --ignore-table={2}.{3} {2} | mysql -u {4} -p{5} -h {6} {7}
                """.format(
                    self.src_user,
                    self.src_passwd,
                    self.src_db,
                    self.tbl_name,
                    self.dest_user,
                    self.dest_passwd,
                    self.dest_host,
                    self.dest_db)

        system(instr)

    def create_struct(self):
        """Create target table structure into destination database.

        Using passwords here is insecure, be sure you are safe.
        """ 
        instr = """
                mysqldump -u {0} -p{1} -d {2} {3} | mysql -u {4} -p{5} -h {6} {7}
                """.format(
                    self.src_user,
                    self.src_passwd,
                    self.src_db,
                    self.tbl_name,
                    self.dest_user,
                    self.dest_passwd,
                    self.dest_host,
                    self.dest_db)

        system(instr)

    def insert(self, limit=False):
        """Read table data and insert its content into destination database.

        Use standard SQL 'SELECT' and 'INSERT INTO' commands.
        """
        if limit:
            self.sc.execute("SELECT * FROM titles LIMIT %s", (limit,))
        else:
            self.sc.execute("SELECT * FROM titles")

        data = self.sc.fetchall()

        try:
            for row in data:
                self.dc.execute(
                    """
                    INSERT INTO titles 
                    (emp_no, title, from_date, to_date) 
                    VALUES (%s, %s, %s, %s)
                    """, row)
        except MySQLdb.Error, e:
            print e[0],e[1]
            self.dest_conn.rollback()
            self.exit(2)

        self.dest_conn.commit()

    def insert_many(self, limit=False, rows=10000):
        """Read table data and insert them into destination database.

        Use standard SQL 'SELECT' and 'INSERT INTO' commands as well as
        executemany method. With server-side cursor it is possible to 
        insert specified number of rows in bulk without worrying about 
        table size and memory.
        """
        if limit:
            self.sc.execute("SELECT * FROM titles LIMIT %s", (limit,))
        else:
            self.sc.execute("SELECT * FROM titles")

        #Just in case the data would not be ignored because of the size
        self.dc.execute("SET GLOBAL max_allowed_packet=1073741824")
        #Prevention of automated commits may increase execution speed
        self.dc.execute("SET autocommit = 0")
        #No foreign key checks for individual rows can increase performance
        self.dc.execute("SET foreign_key_checks = 0")
        #Disable unique values constraint while doing insert
        self.dc.execute("SET unique_checks = 0")
        self.dest_conn.commit()
        #It helps to start the while loop 
        data = True
        
        try:
            while data:
                #fetchmany method can be used to save memory and copy data
                #over @@max_allowed_packet limit
                data = self.sc.fetchmany(rows)
                #executemany is optimized for effective copying multiple rows
                self.dc.executemany(
                    """
                    INSERT INTO titles 
                    (emp_no, title, from_date, to_date) 
                    VALUES (%s, %s, %s, %s)
                    """, data)
            self.dest_conn.commit()
        except MySQLdb.Error, e:
            print e[0],e[1]
            self.dest_conn.rollback()
            self.exit(2)

        #Re-adjusting server settings
        self.dc.execute("SET autocommit = 1")
        self.dc.execute("SET foreign_key_checks = 1")
        self.dc.execute("SET unique_checks = 1")
        self.dest_conn.commit()

    def dump(self):
        """Generate MySQL table dump and put it into another database.

        Using passwords here is insecure, be sure you are safe.
        """
        instr = """
                mysqldump -u {0} -p{1} {2} {3} | mysql -u {4} -p{5} -h {6} {7}
                """.format(
                    self.src_user,
                    self.src_passwd,
                    self.src_db,
                    self.tbl_name,
                    self.dest_user,
                    self.dest_passwd,
                    self.dest_host,
                    self.dest_db)

        system(instr)

if __name__ == "__main__":
    table = CopyMysqlTbl()

    #Specify which of three methods to use
    #table.insert()
    table.insert_many()
    #table.dump()

    table.exit()
