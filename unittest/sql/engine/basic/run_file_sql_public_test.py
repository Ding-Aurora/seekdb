#!/usr/bin/env python3
"""Run the original file_sql mysqltest against a local native seekdb.

Only the cluster deployment/cleanup includes are handled locally. The test SQL
and expected result are read unchanged from the official mysqltest suite.
Run on the database host; requires Python 3, pymysql, and mysqltest.
"""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

import pymysql


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=2881)
    parser.add_argument('--user', default='root')
    parser.add_argument('--socket', required=True, help='local socket required to configure secure_file_priv')
    parser.add_argument('--logdir', required=True, type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[4]
    suite = root / 'tools/deploy/mysql_test/test_suite/file_sql'
    mysqltest = root / 'deps/3rd/u01/obclient/bin/mysqltest'
    args.logdir.mkdir(parents=True, exist_ok=True)
    connection = pymysql.connect(host=args.host, port=args.port, user=args.user,
        unix_socket=args.socket,
        password=os.environ.get('SEEKDB_TEST_PASSWORD', ''), autocommit=True, charset='utf8mb4')
    with connection.cursor() as cursor:
        cursor.execute('create database if not exists test')
        cursor.execute('select @@global.secure_file_priv')
        previous = cursor.fetchone()[0]
        with tempfile.TemporaryDirectory(prefix='seekdb-public-file-sql-') as temporary:
            base = Path(temporary)
            data = base / 'data_for_mysqltest_file_sql'
            data.mkdir()
            for source in (suite / 'data').iterdir():
                if source.name == 'simple.parquet.b64':
                    (data / 'simple.parquet').write_bytes(base64.b64decode(source.read_bytes()))
                else:
                    shutil.copyfile(source, data / source.name)
            source = suite / 't/file_sql_basic.test'
            original = source.read_text()
            body = ''.join(line for line in original.splitlines(keepends=True)
                if not line.startswith('--source mysql_test/test_suite/file_sql/include/'))
            test = base / 'file_sql_basic.test'
            test.write_text('let $OBSERVER_DIR = ' + str(base) + ';\n' + body)
            cursor.execute('set global secure_file_priv=%s', (str(base),))
            try:
                command = [str(mysqltest), '--no-defaults', '--host=' + args.host,
                    '--port=' + str(args.port), '--user=' + args.user, '--database=test',
                    '--default-character-set=utf8mb4', '--logdir=' + str(args.logdir),
                    '--test-file=' + str(test), '--result-file=' + str(suite / 'r/mysql/file_sql_basic.result')]
                environment = dict(os.environ)
                if os.environ.get('SEEKDB_TEST_PASSWORD'):
                    environment['MYSQL_PWD'] = os.environ['SEEKDB_TEST_PASSWORD']
                with (args.logdir / 'run.log').open('w') as log:
                    result = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, env=environment)
            finally:
                cursor.execute('set global secure_file_priv=%s', (previous,))
    connection.close()
    report = {'name': 'file_sql_basic', 'exit_code': result.returncode,
        'official_test_sha256': hashlib.sha256(source.read_bytes()).hexdigest(),
        'official_expected_sha256': hashlib.sha256((suite / 'r/mysql/file_sql_basic.result').read_bytes()).hexdigest(),
        'adaptation': 'native local fixture setup and cleanup; original SQL and expected output unchanged'}
    (args.logdir / 'result.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))
    print((args.logdir / 'run.log').read_text())
    return result.returncode


if __name__ == '__main__':
    raise SystemExit(main())
