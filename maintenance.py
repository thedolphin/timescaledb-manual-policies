#!/usr/bin/python3

import re
import sys
import datetime
import logging

import yaml
import psycopg2


class Timing:

    def __init__(self, context_name):
        self.context_name = context_name
        self.start = datetime.datetime.min

    def __enter__(self):
        self.start = datetime.datetime.now()
        log.info('begin %s',
            self.context_name)

    def __exit__(self, exc_type, exc_value, traceback):
        log.info('finish %s: %s',
            self.context_name,
            datetime.datetime.now() - self.start)


def main():

    for policy in settings['policies']:
        if isinstance(policy['tables'], list):
            policy['tables_regex'] = re.compile('(' + '|'.join(policy['tables']) + ')')
        else:
            policy['tables_regex'] = re.compile(policy['tables'])

    conn = psycopg2.connect(' '.join([f'{k}={v}' for k, v in settings['database'].items()]))
    conn.set_session(autocommit=True)

    hypertables = []

    with conn.cursor() as cur:
        cur.execute('select schema_name, table_name, compression_state '
                    'from _timescaledb_catalog.hypertable')

        for row in cur:

            if not row[2]:
                log.info('WARNING: table %s does not have compression enabled', row[1])

            for policy in settings['policies']:
                if policy['tables_regex'].match(row[1]):
                    hypertables.append({
                        'name': f'{row[0]}."{row[1]}"',
                        'policy': policy,
                        'compress': row[2]})
                    break
            else:
                log.info('WARNING: table %s does not match any policy', row[1])

    for hypertable in hypertables:

        log.info('hypertable cleanup: %s [%s retention, %s compression]',
            hypertable['name'],
            hypertable['policy']['retention'],
            hypertable['policy']['compression'] if hypertable['compress'] else 'no')

        with conn.cursor() as cur:

            cur.execute('SELECT drop_chunks(%s, INTERVAL %s)', (
                hypertable['name'], hypertable['policy']['retention'] ))

            for row in cur:
                log.info('dropped chunk: %s', row[0])

        if not hypertable['compress']:
            continue

        with conn.cursor() as cur:

            try:
                cur.execute("SELECT s.chunk "
                            "FROM show_chunks(%s, older_than => INTERVAL %s) as s(chunk) "
                            "JOIN chunk_compression_stats(%s) c "
                            "ON ((c.chunk_schema || '.' || chunk_name)::regclass = s.chunk) "
                            "WHERE compression_status = 'Uncompressed'", (
                                hypertable['name'],
                                hypertable['policy']['compression'],
                                hypertable['name'] ))

            except (psycopg2.ProgrammingError, psycopg2.InternalError) as e:
                log.info('error getting chunk list for compression for table %s: %s', hypertable['name'], str(e))

            for row in cur:
                try:
                    with conn.cursor() as altcur:
                        altcur.execute('SELECT compress_chunk(%s)', (row[0],))
                    log.info('compresed chunk: %s', row[0])

                except (psycopg2.ProgrammingError, psycopg2.InternalError) as e:
                    log.info('error compressing chunk for table %s: %s', row[0], str(e))


if __name__ == '__main__':

    with open(sys.argv[1], mode='r', encoding='utf-8') as f:
        settings = yaml.load(f, Loader=yaml.Loader)

    log = logging.getLogger()
    log.setLevel(logging.INFO)
    log.addHandler(logging.StreamHandler())

    with Timing('maintenance'):
        main()
