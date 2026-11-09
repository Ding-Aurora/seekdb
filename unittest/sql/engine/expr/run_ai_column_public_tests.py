"""Run the original AI COLUMN mysqltests against a locally built seekdb."""
import argparse
from pathlib import Path
import subprocess
import time

import pymysql


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=2881)
    parser.add_argument('--user', default='root')
    parser.add_argument('--mysqltest', required=True)
    parser.add_argument('--logdir', type=Path, required=True)
    parser.add_argument('cases', nargs='*', default=[
        'ai_column_ddl', 'ai_column_t1_dml', 'ai_column_worker',
        'ai_column_retry_strict', 'ai_column_vector_index'])
    args = parser.parse_args()
    repository = Path(__file__).resolve().parents[4]
    deploy = repository / 'tools/deploy'
    suite = Path('mysql_test/test_suite/ai_column')
    args.logdir.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + 60
    while True:
        try:
            with pymysql.connect(host=args.host, port=args.port, user=args.user,
                                 connect_timeout=2, read_timeout=2) as connection:
                with connection.cursor() as cursor:
                    cursor.execute('SELECT 1')
                    assert cursor.fetchone() == (1,)
            break
        except (OSError, pymysql.MySQLError):
            if time.monotonic() >= deadline:
                raise
            time.sleep(.25)
    for case in args.cases:
        if case not in {'ai_column_ddl', 'ai_column_t1_dml', 'ai_column_worker',
                        'ai_column_retry_strict', 'ai_column_vector_index'}:
            parser.error('Unknown original test case: ' + case)
        logdir = args.logdir / case
        logdir.mkdir(exist_ok=True)
        command = [args.mysqltest, '--no-defaults', '--host=' + args.host,
                   '--port=' + str(args.port), '--user=' + args.user,
                   '--database=test', '--default-character-set=utf8mb4',
                   '--logdir=' + str(logdir),
                   '--test-file=' + str(suite / 't' / (case + '.test')),
                   '--result-file=' + str(suite / 'r/mysql' / (case + '.result'))]
        result = subprocess.run(command, cwd=deploy, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, timeout=300)
        (logdir / 'runner.log').write_bytes(result.stdout)
        print(case + ': ' + ('PASS' if result.returncode == 0 else 'FAIL'), flush=True)
        if result.returncode:
            print(result.stdout.decode('utf-8', errors='replace')[-10000:], flush=True)
            raise SystemExit(result.returncode)


if __name__ == '__main__':
    main()
