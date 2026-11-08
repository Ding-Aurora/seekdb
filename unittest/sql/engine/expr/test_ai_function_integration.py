#!/usr/bin/env python3
"""AI SQL regression tests against a running seekdb and a local mock service.

Run with Python 3 and pymysql:
  python3 unittest/sql/engine/expr/test_ai_function_integration.py --port 2881
The mock listens on 127.0.0.1:8000; no external model or API key is needed.
"""
import argparse
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import threading
import unittest
import uuid

import pymysql


def fingerprint(text):
    return int(hashlib.sha256(text.encode()).hexdigest()[:6], 16)


class MockService(BaseHTTPRequestHandler):
    requests = []
    mode = ''

    def log_message(self, *_):
        pass

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        self.requests.append((self.path, body, dict(self.headers)))
        mode = self.mode
        status = 200
        if self.path.endswith('/generate') or self.path.endswith('/completions'):
            text = body.get('prompt', body.get('messages', [{}])[-1].get('content'))
            if mode == 'empty_completion':
                text = ''
            elif mode == 'invalid_completion':
                text = {'unexpected': 'object'}
            response = {'response': text} if self.path.endswith('/generate') else {
                'choices': [{'message': {'content': text}}]}
        elif self.path.endswith('/embeddings') or self.path.endswith('/embed'):
            dimension = body.get('dimensions', 768)
            vectors = [[fingerprint(text)] + [0.0] * (dimension - 1) for text in body['input']]
            if mode == 'wrong_dimension':
                vectors[0].append(1)
            elif mode == 'invalid_element':
                vectors[0][0] = 'not a number'
            if self.path.endswith('/embed'):
                response = {'embeddings': vectors}
            else:
                # Deliberately reverse provider order to exercise index restoration.
                data = [{'index': i, 'embedding': vector} for i, vector in enumerate(vectors)]
                if mode == 'duplicate_index' and len(data) > 1:
                    data[-1]['index'] = 0
                if mode == 'missing_index':
                    del data[0]['index']
                response = {'data': list(reversed(data))}
        elif self.path.endswith('/rerank'):
            entries = [{'index': i, 'relevance_score': (int(text.split(':', 1)[0])
                        if text.split(':', 1)[0].isdigit() else int(body['query'] in text))}
                       for i, text in enumerate(body['documents'])]
            # Return unsorted data and ties in reverse order.
            if mode == 'rerank_missing':
                entries.pop()
            elif mode == 'rerank_duplicate':
                entries[-1]['index'] = 0
            elif mode == 'rerank_out_of_range':
                entries[0]['index'] = len(entries)
            elif mode == 'rerank_bad_score':
                entries[0]['relevance_score'] = 'invalid'
            response = {'results': list(reversed(entries))}
        else:
            status, response = 404, {'error': 'unknown endpoint'}
        if mode == 'http_error':
            status, response = 400, {'error': 'deliberate regression test failure'}
        elif mode == 'non_object_response':
            response = []
        data = json.dumps(response, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)


class AIFunctionIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = ThreadingHTTPServer(('127.0.0.1', 8000), MockService)
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()
        cls.connection = pymysql.connect(host=ARGS.host, port=ARGS.port, user=ARGS.user,
            password=os.environ.get('SEEKDB_TEST_PASSWORD', ''), charset='utf8mb4',
            autocommit=True, read_timeout=120)
        cls.cursor = cls.connection.cursor()
        cls.prefix = 'ai_reg_' + uuid.uuid4().hex[:10]
        cls.models = {}
        cls.execute('create database `' + cls.prefix + '`')
        cls.execute('use `' + cls.prefix + '`')
        for name, kind, provider, path in [
            ('complete', 'completion', 'openai', '/v1/chat/completions'),
            ('embed', 'dense_embedding', 'OPENAI', '/v1/embeddings'),
            ('rerank', 'rerank', 'siliconflow', '/v1/rerank'),
            ('ollama_complete', 'completion', 'ollama', '/api/generate'),
            ('ollama_embed', 'dense_embedding', 'OLLAMA', '/api/embed'),
        ]:
            model = cls.prefix + '_' + name
            cls.models[name] = model
            cls.execute('call DBMS_AI_SERVICE.CREATE_AI_MODEL(%s,%s)',
                (model, json.dumps({'type': kind, 'model_name': 'request-' + name})))
            cls.execute('call DBMS_AI_SERVICE.CREATE_AI_MODEL_ENDPOINT(%s,%s)',
                (model + '_ep', json.dumps({'ai_model_name': model, 'scope': 'all',
                    'provider': provider, 'url': 'http://127.0.0.1:8000' + path,
                    'access_key': '' if provider.lower() == 'ollama' else 'test-key'})))

    @classmethod
    def tearDownClass(cls):
        for model in cls.models.values():
            cls.execute('call DBMS_AI_SERVICE.DROP_AI_MODEL(%s)', (model,))
            cls.execute('call DBMS_AI_SERVICE.DROP_AI_MODEL_ENDPOINT(%s)', (model + '_ep',))
        cls.execute('drop database `' + cls.prefix + '`')
        cls.connection.close()
        cls.server.shutdown()
        cls.server.server_close()

    @classmethod
    def execute(cls, sql, params=None):
        cls.cursor.execute(sql, params)
        rows = cls.cursor.fetchall()
        while cls.cursor.nextset():
            pass
        return rows

    def setUp(self):
        MockService.mode = ''
        MockService.requests.clear()

    def scalar(self, sql, params=None):
        return self.execute(sql, params)[0][0]

    def test_prompt_json_nested_and_repeated(self):
        prompt = json.loads(self.scalar("select ai_prompt('{0}/{0}/{1}',json_array(1,true),json_object('k',2))"))
        self.assertEqual([[1, True], {'k': 2}], prompt['args'])
        value = self.scalar("select ai_complete(%s,ai_prompt('{0}|{0}|{1}',ai_prompt('你好 {0}','世界'),json_object('k',2)))",
                            (self.models['complete'],))
        self.assertEqual('你好 世界|你好 世界|', value[:len('你好 世界|你好 世界|')])
        self.assertEqual({'k': 2}, json.loads(value.split('|')[-1]))

    def test_prompt_longtext_and_escapes(self):
        self.execute('create table prompt_lob(t longtext,a longtext)')
        try:
            text = '中文\\"\n' * 18000
            self.execute('insert into prompt_lob values(%s,%s)', ('{0}/{0}/{0}', text))
            value = self.scalar('select ai_complete(%s,ai_prompt(t,a)) from prompt_lob', (self.models['complete'],))
            self.assertEqual('/'.join([text] * 3), value)
        finally:
            self.execute('drop table prompt_lob')

    def test_prompt_invalid_indexes(self):
        for template in ['{1}', '{999999999999999999999999999999999999999}']:
            with self.subTest(template=template), self.assertRaises(pymysql.MySQLError):
                self.scalar('select ai_complete(%s,ai_prompt(%s,%s))',
                            (self.models['complete'], template, 'value'))

    def test_completion_config_and_null(self):
        for config in [None, json.dumps({'temperature': 0.2, 'max_tokens': 17})]:
            value = self.scalar('select ai_complete(%s,%s,%s)', (self.models['complete'], '你好\n"\\', config))
            self.assertEqual('你好\n"\\', value)
            request = MockService.requests[-1][1]
            self.assertEqual('request-complete', request['model'])
            if config:
                self.assertEqual(0.2, request['temperature'])
                self.assertEqual(17, request['max_tokens'])
        with self.assertRaises(pymysql.MySQLError):
            self.scalar("select ai_complete(%s,'hello',json_array(1))", (self.models['complete'],))

    def test_completion_invalid_responses(self):
        for mode in ['empty_completion', 'invalid_completion', 'non_object_response', 'http_error']:
            MockService.mode = mode
            with self.subTest(mode=mode), self.assertRaises(pymysql.MySQLError):
                self.scalar("select ai_complete(%s,'hello')", (self.models['complete'],))

    def test_embed_dimensions_and_null(self):
        for dimension in [None, 3, 17]:
            args = (self.models['embed'], 'single') + (() if dimension is None else (dimension,))
            value = json.loads(self.scalar('select ai_embed(' + ','.join(['%s'] * len(args)) + ')', args))
            self.assertEqual(dimension or 768, len(value))
            self.assertEqual(fingerprint('single'), value[0])
        self.assertIsNone(self.scalar('select ai_embed(%s,null)', (self.models['embed'],)))
        for dimension in [0, -1, 1.5]:
            with self.subTest(dimension=dimension), self.assertRaises(pymysql.MySQLError):
                self.scalar("select ai_embed(%s,'single',%s)", (self.models['embed'], dimension))

    def test_embed_rejects_malformed_vectors(self):
        for mode in ['wrong_dimension', 'invalid_element', 'missing_index', 'non_object_response', 'http_error']:
            MockService.mode = mode
            with self.subTest(mode=mode), self.assertRaises(pymysql.MySQLError):
                self.scalar("select ai_embed(%s,'single',3)", (self.models['embed'],))

    def test_embed_batch_mapping_filters_and_multiple_batches(self):
        self.execute('create table batch_rows(id int primary key,content text)')
        try:
            self.cursor.executemany('insert into batch_rows values(%s,%s)',
                [(i, None if i % 7 == 0 else 'row-' + str(i)) for i in range(600)])
            for suffix in ['order by id', 'where id %% 2 = 0 order by id limit 37']:
                MockService.requests.clear()
                rows = self.execute('select id,ai_embed(%s,content,3) from batch_rows ' + suffix,
                                    (self.models['embed'],))
                self.assertEqual(37 if 'limit' in suffix else 600, len(rows))
                for index, raw in rows:
                    if index % 7 == 0:
                        self.assertIsNone(raw)
                    else:
                        vector = json.loads(raw)
                        self.assertEqual([fingerprint('row-' + str(index)), 0, 0], vector)
                self.assertTrue(any(len(body['input']) > 1 for _, body, _ in MockService.requests),
                                'table scan must combine rows into multi-input requests')
                self.assertTrue(all(text is not None for _, body, _ in MockService.requests for text in body['input']))
            MockService.mode = 'duplicate_index'
            with self.assertRaises(pymysql.MySQLError):
                self.execute('select ai_embed(%s,content,3) from batch_rows', (self.models['embed'],))
        finally:
            self.execute('drop table batch_rows')

    def test_ollama_endpoint_and_native_protocols(self):
        model = self.models['ollama_complete']
        self.execute('call DBMS_AI_SERVICE.ALTER_AI_MODEL_ENDPOINT(%s,%s)',
            (model + '_ep', json.dumps({'provider': 'Ollama'})))
        self.assertEqual('native', self.scalar("select ai_complete(%s,'native')", (model,)))
        path, body, headers = MockService.requests[-1]
        self.assertEqual('/api/generate', path)
        self.assertIs(body['stream'], False)
        self.assertEqual('native', body['prompt'])
        self.assertFalse(any(key.lower() == 'authorization' for key in headers))
        vector = json.loads(self.scalar("select ai_embed(%s,'native',5)", (self.models['ollama_embed'],)))
        self.assertEqual(5, len(vector))
        self.assertEqual(fingerprint('native'), vector[0])
        self.assertEqual('/api/embed', MockService.requests[-1][0])
        MockService.mode = 'invalid_element'
        with self.assertRaises(pymysql.MySQLError):
            self.scalar("select ai_embed(%s,'native',5)", (self.models['ollama_embed'],))

    def test_rerank_global_indexes_and_stable_ties(self):
        documents = [f'{i % 4}:document-{i}' for i in range(53)]
        value = json.loads(self.scalar('select ai_rerank(%s,%s,%s,null)',
            (self.models['rerank'], 'query', json.dumps(documents))))
        expected = sorted(range(len(documents)), key=lambda i: (-int(documents[i].split(':')[0]), i))
        self.assertEqual(expected, [item['index'] for item in value])
        self.assertEqual([20, 20, 13], [len(body['documents']) for _, body, _ in MockService.requests])
        objects = [{'id': 'id-' + str(i), 'body': text, 'extra': {'keep': True}}
                   for i, text in enumerate(documents)]
        value = json.loads(self.scalar('select ai_rerank(%s,%s,%s,%s)',
            (self.models['rerank'], 'query', json.dumps(objects), 'body')))
        self.assertEqual(['id-' + str(i) for i in expected], [item['id'] for item in value])
        self.assertTrue(all(item['extra'] == {'keep': True} and '_model_score' in item for item in value))

    def test_rerank_rejects_invalid_responses(self):
        for mode in ['rerank_missing', 'rerank_duplicate', 'rerank_out_of_range', 'rerank_bad_score', 'non_object_response']:
            MockService.mode = mode
            with self.subTest(mode=mode), self.assertRaises(pymysql.MySQLError):
                self.scalar('select ai_rerank(%s,%s,%s)',
                    (self.models['rerank'], 'query', '["1:a","2:b"]'))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=2881)
    parser.add_argument('--user', default='root')
    ARGS, remaining = parser.parse_known_args()
    unittest.main(argv=[__file__] + remaining, verbosity=2)
