#!/usr/bin/env python

import unittest
from dbcopy import CopyMysqlTbl

class TestCopyMysqlTbl(unittest.TestCase):
	
	def setUp(self):
		self.table = CopyMysqlTbl()

	def test_different_servers(self):
		self.table.sc.execute('SELECT 1;')
		self.assertEqual(self.table.sc.fetchone(), (1,))

		self.table.dc.execute('SELECT 1;')
		self.assertEqual(self.table.dc.fetchone(), (1,))

	def test_insert(self):
		self.table.insert(10)
		self.table.dc.execute(
			"SELECT COUNT(*) FROM {0};".format(self.table.tbl_name))

		self.assertEqual(self.table.dc.fetchone(), (10,))

	def test_insert_many(self):
		self.table.insert_many(10)
		self.table.dc.execute(
			"SELECT COUNT(*) FROM {0};".format(self.table.tbl_name))

		self.assertEqual(self.table.dc.fetchone(), (10,))

	def test_dump(self):
		self.table.dump()
		self.table.sc.execute(
			"SELECT COUNT(*) FROM {0};".format(self.table.tbl_name))
		self.table.dc.execute(
			"SELECT COUNT(*) FROM {0};".format(self.table.tbl_name))

		self.assertEqual(self.table.dc.fetchone(), self.table.sc.fetchone())

	def tearDown(self):
		self.table.dc.execute(
			"TRUNCATE TABLE {0};".format(self.table.tbl_name))

if __name__ == "__main__":
	unittest.main()
