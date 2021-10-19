import argparse
import logging
import random
import re
from functools import partial
from threading import Event

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
    "migration_job_table_rows_deleted", "number of table rows copied", labelnames=["table"]
)


# Util
VALID_IDENTIFIER_REGEX = re.compile(r"[0-9a-zA-Z$_]{1,64}")


def valid_identifier(identifier):
    return VALID_IDENTIFIER_REGEX.fullmatch(identifier) is not None


# Adapted from util/migrate/allocator.py
class NoAvailableKeysError(ValueError):
    pass


class CompletedKeys(object):
    def __init__(self, max_index, min_index=0):
        self._max_index = max_index
        self._min_index = min_index
        self.num_remaining = max_index - min_index
        self._slabs = RBTree()

    def _get_previous_or_none(self, index):
        try:
            return self._slabs.floor_item(index)
        except KeyError:
            return None

    def is_available(self, index):
        logger.debug("Testing index %s", index)
        if index >= self._max_index or index < self._min_index:
            logger.debug("Index out of range")
            return False

        try:
            prev_start, prev_length = self._slabs.floor_item(index)
            logger.debug("Prev range: %s-%s", prev_start, prev_start + prev_length)
            return (prev_start + prev_length) <= index
        except KeyError:
            return True

    def mark_completed(self, start_index, past_last_index):
        logger.debug("Marking the range completed: %s-%s", start_index, past_last_index)
        num_completed = min(past_last_index, self._max_index) - max(start_index, self._min_index)

        # Find the item directly before this and see if there is overlap
        to_discard = set()
        try:
            prev_start, prev_length = self._slabs.floor_item(start_index)
            max_prev_completed = prev_start + prev_length
            if max_prev_completed >= start_index:
                # we are going to merge with the range before us
                logger.debug(
                    "Merging with the prev range: %s-%s", prev_start, prev_start + prev_length
                )
                to_discard.add(prev_start)
                num_completed = max(num_completed - (max_prev_completed - start_index), 0)
                start_index = prev_start
                past_last_index = max(past_last_index, prev_start + prev_length)
        except KeyError:
            pass

        # Find all keys between the start and last index and merge them into one block
        for merge_start, merge_length in self._slabs.iter_items(start_index, past_last_index + 1):
            if merge_start in to_discard:
                logger.debug(
                    "Already merged with block %s-%s", merge_start, merge_start + merge_length
                )
                continue

            candidate_next_index = merge_start + merge_length
            logger.debug("Merging with block %s-%s", merge_start, candidate_next_index)
            num_completed -= merge_length - max(candidate_next_index - past_last_index, 0)
            to_discard.add(merge_start)
            past_last_index = max(past_last_index, candidate_next_index)

        # write the new block which is fully merged
        discard = False
        if past_last_index >= self._max_index:
            logger.debug("Discarding block and setting new max to: %s", start_index)
            self._max_index = start_index
            discard = True

        if start_index <= self._min_index:
            logger.debug("Discarding block and setting new min to: %s", past_last_index)
            self._min_index = past_last_index
            discard = True

        if to_discard:
            logger.debug("Discarding %s obsolete blocks", len(to_discard))
            self._slabs.remove_items(to_discard)

        if not discard:
            logger.debug("Writing new block with range: %s-%s", start_index, past_last_index)
            self._slabs.insert(start_index, past_last_index - start_index)

        # Update the number of remaining items with the adjustments we've made
        assert num_completed >= 0
        self.num_remaining -= num_completed
        logger.debug("Total blocks: %s", len(self._slabs))

    def get_block_start_index(self, block_size_estimate):
        logger.debug("Total range: %s-%s", self._min_index, self._max_index)
        if self._max_index <= self._min_index:
            raise NoAvailableKeysError("All indexes have been marked completed")

        num_holes = len(self._slabs) + 1
        random_hole = random.randint(0, num_holes - 1)
        logger.debug("Selected random hole %s with %s total holes", random_hole, num_holes)

        hole_start = self._min_index
        past_hole_end = self._max_index

        # Now that we have picked a hole, we need to define the bounds
        if random_hole > 0:
            # There will be a slab before this hole, find where it ends
            bound_entries = self._slabs.nsmallest(random_hole + 1)[-2:]
            left_index, left_len = bound_entries[0]
            logger.debug("Left range %s-%s", left_index, left_index + left_len)
            hole_start = left_index + left_len

            if len(bound_entries) > 1:
                right_index, right_len = bound_entries[1]
                logger.debug("Right range %s-%s", right_index, right_index + right_len)
                past_hole_end, _ = bound_entries[1]
        elif not self._slabs.is_empty():
            right_index, right_len = self._slabs.nsmallest(1)[0]
            logger.debug("Right range %s-%s", right_index, right_index + right_len)
            past_hole_end, _ = self._slabs.nsmallest(1)[0]

        # Now that we have our hole bounds, select a random block from [0:len - block_size_estimate]
        logger.debug("Selecting from hole range: %s-%s", hole_start, past_hole_end)
        rand_max_bound = max(hole_start, past_hole_end - block_size_estimate)
        logger.debug("Rand max bound: %s", rand_max_bound)
        return random.randint(hole_start, rand_max_bound)


def yield_random_entries(
    conn, batch_query_sql_partial, primary_key_field, batch_size, max_id, min_id=0
):
    """
    This method will yield items from random blocks in the database.

    We will track metadata about which keys are available for work, and we will complete the
    backfill when there is no more work to be done. The method yields tuples of (candidate, Event),
    and if the work was already done by another worker, the caller should set the event. Batch
    candidates must have an "id" field which can be inspected.

    :param pymysql.connections.Connection conn: MySQL connection
    :param batch_query_sql_partial partial: Partially formatted SQL query string
    :param primary primary_key_field str: Table primary key
    :param batch_size int: SQL query batch size
    :param max_id int: Table max id
    :param min_id int: Table min id
    """
    assert valid_identifier(primary_key_field)

    min_id = max(min_id, 0)
    max_id = max(max_id, 1)
    allocator = CompletedKeys(max_id + 1, min_id)

    try:
        while True:
            start_index = allocator.get_block_start_index(batch_size)
            end_index = min(start_index + batch_size, max_id + 1)

            with conn.cursor() as cursor:
                sql = batch_query_sql_partial(
                    primary_key_field=primary_key_field,
                    max_id=end_index,
                    min_id=start_index,
                )
                cursor.execute(sql)

                all_candidates = cursor.fetchall()

                if len(all_candidates) == 0:
                    logger.info(
                        "No candidates, marking entire block completed %s-%s",
                        start_index,
                        end_index,
                    )
                    allocator.mark_completed(start_index, end_index)
                    continue

                logger.info("Found %s candidates, processing block", len(all_candidates))
                batch_completed = 0
                for candidate in all_candidates:
                    abort_early = Event()
                    yield candidate, abort_early, allocator.num_remaining - batch_completed
                    batch_completed += 1
                    if abort_early.is_set():
                        logger.info("Overlap with another worker, aborting")
                        break

                completed_through = candidate[primary_key_field] + 1
                logger.info(
                    "Marking id range as completed: %s-%s", start_index, completed_through
                )
                allocator.mark_completed(start_index, completed_through)

    except NoAvailableKeysError:
        logger.info("No more work")


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
    conn, tablename, new_tablename, primary_key_field, batch_size=500, skip_validation=False
):
    assert valid_identifier(tablename)
    assert valid_identifier(new_tablename)
    assert valid_identifier(primary_key_field)

    column_names = get_table_column_names(conn, tablename)

    def copy_row(row):
        assert len(row) == len(column_names)
        assert sorted(column_names) == sorted(row.keys())

        comma_separated_columns = ", ".join([f"{k}" for k, v in sorted(row.items())])

        try:
            with conn.cursor() as cursor:
                sql_insert = (
                    """INSERT INTO {new_tablename} ({comma_separated_columns}) VALUES ("""
                    + ", ".join(["%s" for x in range(len(column_names))])
                    + """)"""
                )
                sql_insert = sql_insert.format(
                    new_tablename=new_tablename,
                    comma_separated_columns=comma_separated_columns,
                )

                values = [v for k, v in sorted(row.items())]
                cursor.execute(sql_insert, args=values)
                table_rows_copied.labels(table=new_tablename).inc()

            conn.commit()

            if not skip_validation:
                with conn.cursor() as cursor:
                    sql_select = """SELECT {column_names} FROM {new_tablename} WHERE {primary_key_field}=%s""".format(
                        new_tablename=new_tablename,
                        column_names=", ".join(column_names),
                        primary_key_field=primary_key_field,
                    )
                    cursor.execute(sql_select, row[primary_key_field])
                    copied_row = cursor.fetchone()
                    assert sorted(copied_row) == sorted(row)
        except:
            logger.error("Exception copying row")
            raise

    min_id = get_min_id_for_table(conn, tablename)
    max_id = get_max_id_for_table(conn, tablename)

    sql = partial(
        """SELECT {columns} FROM {tablename} as `t1`
        LEFT OUTER JOIN {new_tablename} as `t2`
        ON `t1`.`{primary_key_field}` = `t2`.`{primary_key_field}`
        WHERE `t2`.`{primary_key_field}` IS NULL
        AND `t1`.`{primary_key_field}` >= {min_id}
        AND `t1`.`{primary_key_field}` < {max_id}
        ORDER BY `t1`.`{primary_key_field}`; 
        """.format,
        tablename=tablename,
        new_tablename=new_tablename,
        columns=", ".join([f"`t1`.`{c}`" for c in column_names]),
    )

    iterator = yield_random_entries(conn, sql, primary_key_field, batch_size, max_id, min_id=0)
    for candidate, abt, num_remaining in iterator:
        try:
            copy_row(candidate)
        except Exception as e:
            logger.error("Error copying row with id %s: %s", candidate[primary_key_field], e)
            break


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
    table_copy_parser.add_argument("--chunk-size", type=int, default=10000)
    table_copy_parser.add_argument("--skip-validation", type=bool, default=False)
    table_copy_parser.add_argument("--check-new-table-empty", type=bool, default=False)
    table_copy_parser.add_argument("--metrics", type=bool, default=True)
    args = parser.parse_args()

    logging.basicConfig(level=LOG_LEVELS[args.log_level])
    if args.metrics:
        start_http_server(9090)

    conn = connect(args.host, int(args.port), args.user, args.password, args.db)

    # Some basic checks
    with conn:
        assert set(get_table_column_names(conn, args.old_table)).issubset(
            set(get_table_column_names(conn, args.new_table))
        )
        if args.check_new_table_empty:
            assert check_table_empty(conn, args.new_table)

        perform_backfill(
            conn, args.old_table, args.new_table, args.pk, args.chunk_size, args.skip_validation
        )


if __name__ == "__main__":
    main()
