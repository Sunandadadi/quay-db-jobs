import argparse
import concurrent.futures
import logging
import random
import re
import time

from functools import partial
from multiprocessing.pool import ThreadPool
from threading import Event, Thread

from bintrees import RBTree

from prometheus_client import (
    push_to_gateway,
    REGISTRY,
    Histogram,
    Gauge,
    Counter,
    start_http_server,
)
import pymysql.cursors


logger = logging.getLogger(__name__)
LOG_LEVELS = {
    "critical": logging.CRITICAL,
    "error": logging.ERROR,
    "warn": logging.WARNING,
    "warning": logging.WARNING,
    "info": logging.INFO,
    "debug": logging.DEBUG,
}


# Metrics
table_rows_copied = Counter(
    "migration_job_table_rows_copied", "number of table rows copied", labelnames=["table"]
)

table_rows_copy_duration = Histogram(
    "migration_job_batch_copy_duration_seconds",
    "seconds taken to copy batch of rows",
    labelnames=["table", "batch_size"],
)


# Util
VALID_IDENTIFIER_REGEX = re.compile(r"[0-9a-zA-Z$_]{1,64}")


def valid_identifier(identifier):
    return VALID_IDENTIFIER_REGEX.fullmatch(identifier) is not None


# Actual backfill script
def connect(host, port, user, password, database, cursorclass=pymysql.cursors.DictCursor):
    """Not thread-safe."""
    return pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        ssl={"fake_flag_to_enable_tls": True},  # Blankly trust any certs, or add the RDS one
        cursorclass=cursorclass,
    )


def get_table_column_names(conn, tablename):
    assert valid_identifier(tablename)

    with conn.cursor() as cursor:
        sql = """SELECT group_concat(COLUMN_NAME)
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s;
        """
        cursor.execute(sql, args=(conn.db.decode(), tablename))
        result = cursor.fetchone()
        return result["group_concat(COLUMN_NAME)"].split(",")


def check_table_empty(conn, tablename):
    assert valid_identifier(tablename)

    with conn.cursor() as cursor:
        sql = """SELECT id FROM `%s` LIMIT 1""" % tablename
        cursor.execute(sql, args=(tablename,))
        result = cursor.fetchone()
        return result is None


def get_min_id_for_table(conn, tablename):
    assert valid_identifier(tablename)

    with conn.cursor() as cursor:
        sql = """SELECT Min(id) FROM `%s`""" % tablename
        cursor.execute(sql)
        result = cursor.fetchone()
        return result["Min(id)"]


def get_max_id_for_table(conn, tablename):
    assert valid_identifier(tablename)

    with conn.cursor() as cursor:
        sql = """SELECT Max(id) FROM `%s`""" % tablename
        cursor.execute(sql)
        result = cursor.fetchone()
        return result["Max(id)"]


def perform_backfill(
    conn,
    tablename,
    new_tablename,
    primary_key_field="id",
    batch_size=500,
    skip_validation=False,
    min_start_id=-1,
):
    assert valid_identifier(tablename)
    assert valid_identifier(new_tablename)
    assert valid_identifier(primary_key_field)

    column_names = get_table_column_names(conn, tablename)

    def copy_rows(min_id, max_id):
        table_comma_separated_columns = ", ".join([f"{c}" for c in sorted(column_names)])

        try:
            with conn.cursor() as cursor:
                start_time = time.time()

                # If a min_start_id is specified, then skip the LEFT JOIN to only filters rows that haven't been copied.
                # This assumes that there are no rows havae been written after the given min_start_id to the new table.
                # Otherwise, scans from the beginning, looking only for rows that have not been copied.
                if min_start_id > -1:
                    sql_insert = """INSERT INTO {new_tablename} ({table_comma_separated_columns})
                    (
                    SELECT {table_comma_separated_columns} FROM {tablename} AS t1 
                    WHERE t1.{primary_key_field} >= %s AND t1.{primary_key_field} < %s
                    )
                    """
                    sql_insert = sql_insert.format(
                        new_tablename=new_tablename,
                        tablename=tablename,
                        table_comma_separated_columns=table_comma_separated_columns,
                        primary_key_field=primary_key_field,
                    )
                else:
                    first_table_comma_separated_columns = ", ".join(
                        [f"t1.{c}" for c in sorted(column_names)]
                    )
                    second_table_comma_separated_columns = ", ".join(
                        [f"t2.{c}" for c in sorted(column_names)]
                    )

                    sql_insert = """INSERT INTO {new_tablename} ({table_comma_separated_columns})
                    (
                    SELECT {first_table_comma_separated_columns} FROM {tablename} AS t1 
                    LEFT OUTER JOIN manifest_pythonupgrade AS t2 ON t1.{primary_key_field} = t2.{primary_key_field}
                    WHERE t2.{primary_key_field} IS NULL
                    AND t1.{primary_key_field} >= %s AND t1.{primary_key_field} < %s
                    )
                    """
                    sql_insert = sql_insert.format(
                        new_tablename=new_tablename,
                        tablename=tablename,
                        table_comma_separated_columns=table_comma_separated_columns,
                        first_table_comma_separated_columns=first_table_comma_separated_columns,
                        second_table_comma_separated_columns=second_table_comma_separated_columns,
                        primary_key_field=primary_key_field,
                    )

                result = cursor.execute(sql_insert, args=(min_id, max_id))

                time_elapsed = time.time() - start_time
                logger.info(
                    "%s rows copied from %s to %s, from %s to %s in %s seconds",
                    result,
                    tablename,
                    new_tablename,
                    min_id,
                    max_id,
                    time_elapsed,
                )

                if result > 0:
                    table_rows_copy_duration.labels(
                        table=new_tablename, batch_size=batch_size
                    ).observe(time_elapsed)
                    table_rows_copied.labels(table=new_tablename).inc(result)

            conn.commit()

            if not skip_validation:
                with conn.cursor() as cursor:
                    sql_select = """SELECT COUNT({primary_key_field}) FROM {tablename} WHERE {primary_key_field} >= %s and {primary_key_field} < %s ORDER BY {primary_key_field}"""
                    cursor.execute(
                        sql_select.format(
                            tablename=new_tablename,
                            column_names=", ".join(column_names),
                            primary_key_field=primary_key_field,
                        ),
                        args=(min_id, max_id),
                    )
                    copied_rows = cursor.fetchone()

                with conn.cursor() as cursor:
                    cursor.execute(
                        sql_select.format(
                            tablename=tablename,
                            column_names=", ".join(column_names),
                            primary_key_field=primary_key_field,
                        ),
                        args=(min_id, max_id),
                    )
                    old_rows = cursor.fetchone()

                assert copied_rows == old_rows

        except Exception as e:
            logger.error("Exception copying row from %s to %s: %s", min_id, max_id, e)
            raise

    min_id = max(get_min_id_for_table(conn, tablename), min_start_id)
    max_id = max(get_max_id_for_table(conn, tablename), 1)

    start_index = min_id
    end_index = min_id + batch_size

    while end_index < max_id + 1:
        logger.info("Copying rows with id from %s to %s", start_index, end_index - 1)
        copy_rows(start_index, end_index)
        start_index, end_index = end_index, end_index + batch_size


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-log",
        "--log-level",
        default="warning",
        choices=["critical", "error", "warn", "warning", "info", "debug"],
    )

    subparsers = parser.add_subparsers()
    table_copy_parser = subparsers.add_parser(
        "table_copy", help="Copy existing table over a new empty table"
    )
    table_copy_parser.add_argument("host")
    table_copy_parser.add_argument("port")
    table_copy_parser.add_argument("db")
    table_copy_parser.add_argument("user")
    table_copy_parser.add_argument("password")
    table_copy_parser.add_argument("old_table")
    table_copy_parser.add_argument("new_table")
    table_copy_parser.add_argument("--pk", type=str, default="id")
    table_copy_parser.add_argument("--chunk-size", type=int, default=1000)
    table_copy_parser.add_argument("--skip-validation", type=bool, default=False)
    table_copy_parser.add_argument("--check-new-table-empty", type=bool, default=False)
    table_copy_parser.add_argument("--metrics", type=bool, default=True)
    table_copy_parser.add_argument("--min_start_id", type=int, default=-1)

    args = parser.parse_args()

    logging.basicConfig(level=LOG_LEVELS[args.log_level])
    if args.metrics:
        start_http_server(9090)

    conn = connect(args.host, int(args.port), args.user, args.password, args.db)
    with conn:
        assert set(get_table_column_names(conn, args.old_table)).issubset(
            set(get_table_column_names(conn, args.new_table))
        )
        if args.check_new_table_empty:
            assert check_table_empty(conn, args.new_table)

        perform_backfill(
            conn,
            args.old_table,
            args.new_table,
            primary_key_field=args.pk,
            batch_size=args.chunk_size,
            skip_validation=args.skip_validation,
            min_start_id=args.min_start_id,
        )


if __name__ == "__main__":
    main()
