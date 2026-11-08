#!/usr/bin/env python3
"""Native SQL regressions; run on the database host with Python 3 and pymysql.

python3 unittest/sql/engine/basic/test_file_sql_integration.py --socket /path/to/base/run/sql.sock
Uses an exclusive temporary directory and restores secure_file_priv afterwards.
"""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import tempfile
import threading
import time
import unittest
import uuid

import pymysql


class FileSqlIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.connection = pymysql.connect(host=ARGS.host, port=ARGS.port, user=ARGS.user,
            unix_socket=ARGS.socket,
            password=os.environ.get('SEEKDB_TEST_PASSWORD', ''), charset='utf8mb4',
            autocommit=True, read_timeout=120)
        cls.cursor = cls.connection.cursor()
        cls.root = tempfile.TemporaryDirectory(prefix='seekdb-file-sql-')
        cls.path = Path(cls.root.name)
        cls.database = 'file_reg_' + uuid.uuid4().hex[:10]
        cls.execute('create database `' + cls.database + '`')
        cls.execute('use `' + cls.database + '`')
        cls.previous_root = cls.query('select @@global.secure_file_priv')[0][0]
        cls.execute('set global secure_file_priv=%s', (cls.root.name,))
        cls.execute('set session enable_file_sql=ON')
        fixture = Path(__file__).resolve().parents[4] / 'tools/deploy/mysql_test/test_suite/file_sql/data'
        for name in ['basic.csv', 'basic.jsonl']:
            (cls.path / name).write_bytes((fixture / name).read_bytes())
        (cls.path / 'simple.parquet').write_bytes(base64.b64decode((fixture / 'simple.parquet.b64').read_bytes()))

    @classmethod
    def tearDownClass(cls):
        cls.execute('set global secure_file_priv=%s', (cls.previous_root,))
        cls.execute('drop database `' + cls.database + '`')
        cls.connection.close()
        cls.root.cleanup()

    @classmethod
    def execute(cls, sql, args=None):
        return cls.cursor.execute(sql, args)

    @classmethod
    def query(cls, sql, args=None):
        cls.execute(sql, args)
        return cls.cursor.fetchall()

    @classmethod
    def file(cls, name, contents):
        path = cls.path / name
        path.write_text(contents, encoding='utf8', newline='')
        return str(path)

    def test_default_and_runtime_switch(self):
        self.execute('set session enable_file_sql=OFF')
        with self.assertRaises(pymysql.MySQLError) as error:
            self.query('select count(*) from read_csv(%s)', (str(self.path / 'basic.csv'),))
        self.assertEqual(1235, error.exception.args[0])
        self.execute('set session enable_file_sql=ON')
        self.assertEqual(3, self.query('select count(*) from read_csv(%s)', (str(self.path / 'basic.csv'),))[0][0])

    def test_csv_projection_aggregation_and_dates(self):
        path = str(self.path / 'basic.csv')
        self.assertEqual(((1, 'hello,\nworld', 101), (2, 'plain', 102), (3, 'say "hi"', 103)),
            self.query('select id,note,id__2 from read_csv(%s) f order by id', (path,)))
        row = self.query('select count(*),sum(id),round(avg(score),2),sum(active),count(day) from read_csv(%s,header=true,sample_size=-1)', (path,))[0]
        self.assertEqual((3, 6, 91.92, 2, 2), tuple(float(v) for v in row))

    def test_describe_and_native_explain(self):
        path = str(self.path / 'basic.csv')
        schema = self.query('describe select * from read_csv(%s)', (path,))
        self.assertEqual(('active', 'BOOLEAN', 'NO', None, None, None), schema[3])
        self.assertEqual(('day', 'DATE', 'YES', None, None, None), schema[4])
        plan = self.query('explain select count(*) from read_csv(%s)', (path,))
        self.assertTrue(plan)
        self.assertNotEqual('column_name', self.cursor.description[0][0])

    def test_json_union_missing_and_string_conflicts(self):
        path = self.file('union.jsonl', '{"id":1,"x":true}\n{"id":"002","day":"2024-02-29"}\n{"id":3,"x":null}\n')
        self.assertEqual((('1', 1, None), ('002', None, '2024-02-29'), ('3', None, None)),
            self.query('select id,x,cast(day as char) from read_ndjson(%s)', (path,)))

    def test_parquet_projection_and_aggregate(self):
        path = str(self.path / 'simple.parquet')
        self.assertEqual((7, 0, 6, 21), tuple(self.query('select count(*),min(c1),max(c1),sum(c1) from read_parquet(%s)', (path,))[0]))
        self.assertEqual(((4,), (5,)), self.query('select c1 from read_parquet(%s) p where c1>=4 order by c1 limit 2', (path,)))

    def test_inner_left_join_group_having_and_rescan(self):
        left = self.file('join.csv', 'id,category\n1,a\n2,a\n3,b\n4,b\n')
        right = self.file('join.jsonl', '{"id":1,"v":10}\n{"id":3,"v":30}\n')
        self.assertEqual(((1, 10), (3, 30)), self.query('select c.id,j.v from read_csv(%s) c join read_json(%s) j on c.id=j.id order by c.id', (left, right)))
        self.assertEqual((('a', 2, 10), ('b', 2, 30)), self.query('select c.category,count(*),sum(j.v) from read_csv(%s) c left join read_json(%s) j on c.id=j.id group by c.category having count(*)=2 order by c.category', (left, right)))
        self.assertEqual(((1, 10), (2, None), (3, 30), (4, None)), self.query('select /*+ use_nl(j) */ c.id,j.v from read_csv(%s) c left join read_json(%s) j on c.id=j.id order by c.id', (left, right)))

    def test_large_batches_preserve_string_values(self):
        path = self.file('batches.csv', 'id,note\n' + ''.join(f'{i},row-{i}-中文\n' for i in range(10000)))
        rows = self.query('select id,note from read_csv(%s) where id%%3=0 order by id desc', (path,))
        self.assertEqual(tuple((i, f'row-{i}-中文') for i in reversed(range(0, 10000, 3))), rows)

    def test_all_null_and_numeric_boundaries(self):
        path = self.file('bounds.csv', 'n,big,date\n,9223372036854775807,2024-02-29\n,9223372036854775808,2023-02-29\n')
        rows = self.query('select n,big,date from read_csv(%s)', (path,))
        self.assertEqual(((None, '9223372036854775807', '2024-02-29'), (None, '9223372036854775808', '2023-02-29')), rows)
        schema = self.query('desc select * from read_csv(%s)', (path,))
        self.assertEqual('NULL', schema[0][1])

    def test_copy_query_and_table_roundtrip(self):
        source = self.file('copy.csv', 'id,note,missing\n1,"a,\nb",\n2,"say ""hi""",\n')
        output = str(self.path / 'copy-query.csv')
        self.execute("copy (select id,note,missing from read_csv(%s) order by id) to %s (format csv,header,nullstr 'NULL')", (source, output))
        self.assertEqual(((1, 'a,\nb', None), (2, 'say "hi"', None)), self.query("select * from read_csv(%s,nullstr='NULL') order by id", (output,)))
        self.execute('create table exported(id bigint,note varchar(100))')
        self.execute("insert into exported values(1,'NULL'),(2,'x;y'),(3,null)")
        output = str(self.path / 'copy-table.csv')
        self.execute("copy exported to %s (header true,delimiter ';',nullstr 'NULL')", (output,))
        self.assertEqual(((1, 'NULL'), (2, 'x;y'), (3, None)), self.query("select * from read_csv(%s,delim=';',nullstr='NULL') order by id", (output,)))

    def test_copy_existing_target_and_failure_cleanup(self):
        target = self.file('existing.csv', 'keep')
        with self.assertRaises(pymysql.MySQLError):
            self.execute('copy (select 1 as a) to %s (header)', (target,))
        self.assertEqual('keep', Path(target).read_text())
        source = self.file('bad-query.csv', 'note\n"{""a"":1}"\nbad-json\n')
        target = str(self.path / 'failed.csv')
        with self.assertRaises(pymysql.MySQLError):
            self.execute("copy (select json_extract(note,'$.a') from read_csv(%s)) to %s (header)", (source, target))
        self.assertFalse(Path(target).exists())
        self.assertEqual([], list(self.path.glob('.seekdb-copy-*.tmp')))

    def test_bad_options_and_bad_records(self):
        source = str(self.path / 'basic.csv')
        for statement, args, code in [
            ('select * from read_csv(%s, %s)', (source, 'xml'), 1064),
            ('select * from read_csv(%s,sample_size=10)', (source,), 1235),
            ('select * from read_csv(%s,unknown=true)', (source,), 1235),
        ]:
            with self.subTest(statement=statement), self.assertRaises(pymysql.MySQLError) as error:
                self.query(statement, args)
            self.assertEqual(code, error.exception.args[0])
        for name, contents, reader in [('bad.csv', 'a,b\n1\n', 'read_csv'), ('quotes.csv', 'a\n"unfinished', 'read_csv'), ('nested.jsonl', '{"x":[1]}\n', 'read_json')]:
            source = self.file(name, contents)
            with self.subTest(name=name), self.assertRaises(pymysql.MySQLError):
                self.query('select * from ' + reader + '(%s)', (source,))

    def test_secure_paths_symlinks_and_glob(self):
        with tempfile.TemporaryDirectory(prefix='seekdb-file-outside-') as outside:
            target = Path(outside) / 'outside.csv'
            target.write_text('id\n1\n')
            link = self.path / 'escape.csv'
            link.symlink_to(target)
            try:
                for path in [str(target), str(link), str(self.path / '..' / Path(outside).name / 'outside.csv')]:
                    with self.assertRaises(pymysql.MySQLError):
                        self.query('select * from read_csv(%s)', (path,))
                with self.assertRaises(pymysql.MySQLError):
                    self.query('select * from glob(%s)', (str(self.path / '*.csv'),))
                with self.assertRaises(pymysql.MySQLError):
                    self.execute('copy (select 1 as id) to %s', (str(Path(outside) / 'output.csv'),))
            finally:
                link.unlink()
        directory = self.path / 'glob'
        directory.mkdir()
        (directory / 'b.csv').write_text('a\n2\n')
        (directory / 'a.csv').write_text('a\n1\n')
        self.assertEqual((('a.csv',), ('b.csv',)), self.query("select substring_index(file,'/',-1) from glob(%s) order by file", (str(directory / '*.csv'),)))

    def test_runtime_file_mutation_is_rejected(self):
        path = self.file('mutation.csv', 'id\n' + ''.join(f'{i}\n' for i in range(100)))
        def mutate():
            time.sleep(0.3)
            with open(path, 'a') as output:
                output.write('100\n')
        thread = threading.Thread(target=mutate)
        thread.start()
        try:
            with self.assertRaises(pymysql.MySQLError):
                self.query('select id,sleep(0.01) from read_csv(%s)', (path,))
        finally:
            thread.join()

    def test_paths_and_changed_schemas_are_not_cached(self):
        first = self.file('cache-a.csv', 'id\n1\n')
        second = self.file('cache-b.csv', 'id\n2\n')
        self.assertEqual(((1,),), self.query('select * from read_csv(%s)', (first,)))
        self.assertEqual(((2,),), self.query('select * from read_csv(%s)', (second,)))
        self.file('cache-a.csv', 'name\nchanged\n')
        self.assertEqual((('changed',),), self.query('select * from read_csv(%s)', (first,)))
        self.assertEqual('name', self.cursor.description[0][0])

    def test_invalid_utf8_is_an_error_and_server_stays_ready(self):
        with self.assertRaises(pymysql.MySQLError):
            self.query('select * from read_csv(_binary 0xff)')
        self.assertEqual(((1,),), self.query('select 1'))

    def test_input_is_read_only_and_ordinary_sql_works(self):
        path = self.path / 'basic.csv'
        before = hashlib.sha256(path.read_bytes()).digest()
        self.query('select count(*) from read_csv(%s)', (str(path),))
        self.assertEqual(before, hashlib.sha256(path.read_bytes()).digest())
        self.assertEqual(((1,), (2,)), self.query("select j.id from json_table('[1,2]','$[*]' columns(id int path '$')) j order by id"))
        self.assertEqual(((3,),), self.query('select 1+2'))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=2881)
    parser.add_argument('--user', default='root')
    parser.add_argument('--socket', required=True, help='local socket required to configure secure_file_priv')
    ARGS, remaining = parser.parse_known_args()
    unittest.main(argv=[__file__] + remaining, verbosity=2)
