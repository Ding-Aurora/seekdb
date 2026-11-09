"""Extra native AI COLUMN checks against an isolated seekdb instance."""
import json
import os
import threading
import time
import unittest
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pymysql


class AiColumnIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.events = []
        cls.release_backfill = threading.Event()

        class Model(BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass

            def do_POST(self):
                request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                messages = {m['role']: m['content'] for m in request['messages']}
                user = messages['user']
                cls.events.append(user)
                if user == 'blocked backfill':
                    cls.release_backfill.wait(timeout=10)
                text = user if 'base-10 integer' in messages.get('system', '') else 'result:' + user
                reply = json.dumps({'choices': [{'message': {'role': 'assistant', 'content': text}}]}).encode()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(reply)))
                self.end_headers()
                self.wfile.write(reply)

        cls.server = ThreadingHTTPServer(('127.0.0.1', 0), Model)
        cls.addClassCleanup(cls.server.server_close)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.addClassCleanup(cls.server.shutdown)
        cls.connection = pymysql.connect(host=os.getenv('SEEKDB_TEST_HOST', '127.0.0.1'),
            port=int(os.getenv('SEEKDB_TEST_PORT', '2881')), user='root', autocommit=True,
            connect_timeout=5, read_timeout=20)
        cls.addClassCleanup(cls.connection.close)
        cls.database = 'ai_col_check_' + uuid.uuid4().hex[:12]
        cls.model = cls.database + '_model'
        cls.endpoint = cls.database + '_endpoint'
        cls.query('CREATE DATABASE ' + cls.database)
        cls.addClassCleanup(cls.query, 'DROP DATABASE ' + cls.database)
        cls.query('USE ' + cls.database)
        cls.query('CALL DBMS_AI_SERVICE.CREATE_AI_MODEL(%s,%s)',
                  (cls.model, json.dumps({'type': 'completion', 'model_name': 'echo-fixture'})))
        cls.addClassCleanup(cls.query, 'CALL DBMS_AI_SERVICE.DROP_AI_MODEL(%s)', (cls.model,))
        cls.query('CALL DBMS_AI_SERVICE.CREATE_AI_MODEL_ENDPOINT(%s,%s)', (cls.endpoint, json.dumps({
            'ai_model_name': cls.model, 'url': f'http://127.0.0.1:{cls.server.server_port}/v1/chat/completions',
            'access_key': 'fixture-key', 'provider': 'openai', 'request_model_name': 'echo-fixture'})))
        cls.addClassCleanup(cls.query, 'CALL DBMS_AI_SERVICE.DROP_AI_MODEL_ENDPOINT(%s)', (cls.endpoint,))

    @classmethod
    def query(cls, sql, arguments=None):
        with cls.connection.cursor() as cursor:
            cursor.execute(sql, arguments)
            return cursor.fetchall()

    def wait_result(self, sql, expected):
        deadline = time.monotonic() + 12
        while True:
            try:
                result = self.query(sql)
            except pymysql.MySQLError as error:
                if error.args[0] != 11120:
                    raise
                result = error.args
            if result == expected:
                return
            if time.monotonic() >= deadline:
                self.assertEqual(result, expected)
            time.sleep(.1)

    def table_id(self, table):
        return self.query('SELECT t.table_id FROM oceanbase.__all_table t '
                          'JOIN oceanbase.__all_database d ON t.database_id=d.database_id '
                          'WHERE d.database_name=%s AND t.table_name=%s', (self.database, table))[0][0]

    def test_add_modify_and_show(self):
        self.query('CREATE TABLE backfill(id BIGINT PRIMARY KEY, body VARCHAR(50))')
        self.query("INSERT INTO backfill VALUES(9007199254740993, 'existing row')")
        self.query(f"ALTER TABLE backfill ADD COLUMN result VARCHAR(100) AI COLUMN (AI_COMPLETE('{self.model}', body))")
        self.wait_result('SELECT result FROM backfill', (('result:existing row',),))
        before = self.query('SHOW AI COLUMNS FROM backfill')
        self.assertEqual(before[0][0], 'result')
        self.assertIn('AI COLUMN', self.query('SHOW CREATE TABLE backfill')[0][1])
        self.query(f"ALTER TABLE backfill MODIFY COLUMN result VARCHAR(100) AI COLUMN (AI_COMPLETE('{self.model}', concat('new: ',body)))")
        self.wait_result('SELECT result FROM backfill', (('result:new: existing row',),))
        after = self.query('SHOW AI COLUMNS FROM backfill')
        self.assertNotEqual(before[0][-1], after[0][-1])
        self.assertIn('new: ', after[0][1])

    def test_expired_lease_and_staged_outcome(self):
        self.query(f"CREATE TABLE recovery(id BIGINT PRIMARY KEY, body VARCHAR(50), result VARCHAR(100) AI COLUMN (AI_COMPLETE('{self.model}', body)))")
        table_id = self.table_id('recovery')
        self.query('START TRANSACTION')
        try:
            self.query("INSERT INTO recovery(id,body) VALUES(1,'expired lease'),(2,'staged before restart')")
            self.query("UPDATE oceanbase.__all_ai_column_task SET status='RUNNING',state=1,attempts=1,"
                       "lease_owner='retired-worker',lease_epoch=1,lease_until='2020-01-01' "
                       "WHERE table_id=%s AND rowkey_predicate='`id` = 1'", (table_id,))
            self.query("UPDATE oceanbase.__all_ai_column_task SET status='READY',state=2,result_is_null=0,"
                       "result_value='persisted outcome' WHERE table_id=%s AND rowkey_predicate='`id` = 2'", (table_id,))
            self.query('COMMIT')
        finally:
            self.query('ROLLBACK')
        self.wait_result('SELECT result FROM recovery ORDER BY id', (('result:expired lease',), ('persisted outcome',)))
        self.assertEqual(self.events.count('expired lease'), 1)
        self.assertNotIn('staged before restart', self.events)
        self.assertEqual(self.query('SELECT count(*) FROM oceanbase.__all_ai_column_task WHERE table_id=%s', (table_id,)), ((0,),))

    def test_backfill_binary_key_delete_cancels_running_task(self):
        self.query('CREATE TABLE binary_key(id VARBINARY(16) PRIMARY KEY, body VARCHAR(50))')
        self.query("INSERT INTO binary_key VALUES(X'ABCD', 'blocked backfill')")
        try:
            self.query(f"ALTER TABLE binary_key ADD COLUMN result VARCHAR(100) AI COLUMN (AI_COMPLETE('{self.model}', body))")
            deadline = time.monotonic() + 8
            while 'blocked backfill' not in self.events and time.monotonic() < deadline:
                time.sleep(.1)
            self.assertIn('blocked backfill', self.events)
            table_id = self.table_id('binary_key')
            self.assertEqual(self.query('SELECT count(*) FROM oceanbase.__all_ai_column_task WHERE table_id=%s', (table_id,)), ((1,),))
            self.query("DELETE FROM binary_key WHERE id=X'ABCD'")
            self.assertEqual(self.query('SELECT count(*) FROM oceanbase.__all_ai_column_task WHERE table_id=%s', (table_id,)), ((0,),))
        finally:
            self.release_backfill.set()

    def test_ready_null_and_prepared_strict_read(self):
        self.query(f"CREATE TABLE ready_null(id BIGINT PRIMARY KEY, body VARCHAR(50), result VARCHAR(100) AI COLUMN (AI_COMPLETE('{self.model}', body)))")
        self.query('INSERT INTO ready_null(id,body) VALUES(1,NULL)')
        self.query('SET SESSION ai_column_strict_read=ON')
        try:
            self.wait_result('SELECT result FROM ready_null', ((None,),))
            self.query("PREPARE ai_null_statement FROM 'SELECT result FROM ready_null'")
            try:
                self.query('START TRANSACTION')
                self.query("UPDATE ready_null SET body='never committed' WHERE id=1")
                with self.assertRaises(pymysql.MySQLError) as error:
                    self.query('EXECUTE ai_null_statement')
                self.assertEqual(error.exception.args[0], 11120)
                self.query('ROLLBACK')
                self.assertEqual(self.query('EXECUTE ai_null_statement'), ((None,),))
                self.assertNotIn('never committed', self.events)
            finally:
                self.query('ROLLBACK')
                self.query('DEALLOCATE PREPARE ai_null_statement')
        finally:
            self.query('SET SESSION ai_column_strict_read=OFF')

    def test_typed_failure_and_protected_result(self):
        self.query(f"CREATE TABLE typed(id BIGINT PRIMARY KEY, body VARCHAR(50), result BIGINT AI COLUMN (AI_COMPLETE('{self.model}', body)) WITH (max_retries=0))")
        self.query("INSERT INTO typed(id,body) VALUES(1,'7garbage'),(2,'7')")
        self.wait_result('SELECT result FROM typed ORDER BY id', ((None,), (7,)))
        self.assertEqual(self.events.count('7garbage'), 1)
        for sql in ['UPDATE typed SET result=42 WHERE id=1', 'INSERT INTO typed(id,result) VALUES(3,42)']:
            with self.assertRaises(pymysql.MySQLError) as error:
                self.query(sql)
            self.assertEqual(error.exception.args[0], 1235)
        self.query('CREATE VIEW typed_view AS SELECT id, result AS generated_value FROM typed')
        with self.assertRaises(pymysql.MySQLError):
            self.query('UPDATE typed_view SET generated_value=42 WHERE id=1')
        self.assertEqual(self.query('SELECT result FROM typed ORDER BY id'), ((None,), (7,)))


if __name__ == '__main__':
    unittest.main(verbosity=2)
