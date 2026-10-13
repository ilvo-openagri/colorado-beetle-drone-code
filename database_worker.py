from pathlib import Path
import argparse
import logging
import multiprocessing as mp
import os
import queue
import socket
import time

import psycopg2

POLL_INTERVAL = 0.05  # 50 ms
DB_SOCKET_TIMEOUT_SECONDS = 5
DB_STATEMENT_TIMEOUT_MS = 5000


logging.basicConfig(
    format="[%(levelname)s %(name)s @ %(asctime)s] %(message)s",
    level=logging.DEBUG,
)

logger = logging.getLogger("database-worker")


def connection_kwargs():
    return {
        "host": os.environ["PG_CLOUD_HOST"],
        "port": int(os.environ["PG_CLOUD_PORT"]),
        "database": os.environ["PG_CLOUD_DATABASE"],
        "user": os.environ["PG_CLOUD_USER"],
        "password": os.environ["PG_CLOUD_PASSWORD"],
        "connect_timeout": 5,
        "keepalives": 1,
        "keepalives_idle": 5,
        "keepalives_interval": 2,
        "keepalives_count": 3,
    }


def connect_db():
    connection = psycopg2.connect(**connection_kwargs())
    return configure_connection(connection)


def configure_connection(connection):
    connection.autocommit = True

    socket_obj = getattr(getattr(connection, "pgconn", None), "socket", None)
    if socket_obj is not None:
        socket_obj.settimeout(DB_SOCKET_TIMEOUT_SECONDS)

    try:
        with connection.cursor() as cursor:
            cursor.execute(f"SET statement_timeout = '{DB_STATEMENT_TIMEOUT_MS}ms'")
            cursor.execute(f"SET lock_timeout = '{DB_STATEMENT_TIMEOUT_MS}ms'")
    except psycopg2.Error:
        pass

    return connection


def get_connection(existing_connection=None, force_new=False):
    if existing_connection is not None:
        try:
            if not force_new and getattr(existing_connection, "closed", 0) == 0:
                return existing_connection
        except Exception:
            pass

        try:
            existing_connection.close()
        except Exception:
            pass

    connection = connect_db()
    return connection


def is_recoverable_db_error(exc):
    if isinstance(exc, (socket.timeout, TimeoutError)):
        return True

    if isinstance(exc, (psycopg2.OperationalError, psycopg2.InterfaceError)):
        return True

    text = str(exc).lower()
    return any(
        marker in text
        for marker in [
            "server closed the connection unexpectedly",
            "connection reset by peer",
            "broken pipe",
            "network is unreachable",
            "temporary failure in name resolution",
            "timeout",
            "connection refused",
            "connection aborted",
            "ssl error",
        ]
    )


def _execute_query_worker(connection_args, query, result_queue):
    conn = None
    try:
        conn = psycopg2.connect(**connection_args)
        conn = configure_connection(conn)
        with conn.cursor() as cursor:
            cursor.execute(query)
        result_queue.put(("ok", None))
    except Exception as exc:  # pragma: no cover - subprocess branch
        result_queue.put(("err", exc))
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


def execute_query_with_timeout(connection, query, timeout_seconds=2):
    q = mp.Queue()
    logger.info("Starting timed SQL attempt (timeout=%ss)", timeout_seconds)
    worker = mp.Process(
        target=_execute_query_worker,
        args=(connection_kwargs(), query, q),
    )
    worker.start()
    worker.join(timeout=timeout_seconds)

    if worker.is_alive():
        logger.warning(
            "Timed SQL attempt exceeded %ss; terminating worker process.",
            timeout_seconds,
        )
        worker.terminate()
        worker.join(timeout=1)
        raise TimeoutError(f"Query timed out after {timeout_seconds}s")

    try:
        status, payload = q.get_nowait()
    except queue.Empty:
        logger.warning(
            "Timed SQL attempt finished without a result; treating as timeout."
        )
        raise TimeoutError(f"Query timed out after {timeout_seconds}s")

    if status == "ok":
        logger.info("Timed SQL attempt completed successfully.")
        return connection

    logger.warning("Timed SQL attempt failed in child process: %s", payload)
    raise payload


def execute_query(connection, query, max_retries=None, timeout_seconds=2):
    attempt = 0

    while True:
        attempt += 1
        try:
            if getattr(connection, "closed", 0) != 0:
                logger.warning(
                    "Existing DB connection is closed; reconnecting before retry."
                )
                connection = get_connection(connection)

            connection = execute_query_with_timeout(connection, query, timeout_seconds)
            logger.info("Query executed successfully on attempt %s", attempt)
            return connection

        except (TimeoutError, psycopg2.Error) as exc:
            logger.warning(
                "Database error on attempt %s: %s",
                attempt,
                exc,
                exc_info=isinstance(exc, TimeoutError),
            )

            if not isinstance(exc, TimeoutError) and not is_recoverable_db_error(exc):
                raise

            try:
                connection.rollback()
            except psycopg2.Error as rollback_exc:
                logger.warning("Rollback failed after DB error: %s", rollback_exc)

            logger.warning(
                "Recoverable DB failure detected; forcing a fresh DB connection and retrying query."
            )
            connection = get_connection(connection, force_new=True)

            if max_retries is not None and attempt >= max_retries:
                logger.error(
                    "Query failed after %s attempts; giving up as requested.",
                    max_retries,
                )
                raise exc

            delay = min(0.25 * attempt, 2.0)
            logger.info("Retrying query in %.2fs", delay)
            time.sleep(delay)


def process_queries(query_file: Path):
    connection = get_connection()

    logger.info("Database worker started: %s", query_file)

    # Position in the file
    position = 0

    while True:
        if not query_file.exists():
            time.sleep(POLL_INTERVAL)
            continue

        with query_file.open("r", encoding="utf-8") as f:
            f.seek(position)

            while True:
                line = f.readline()

                if not line:
                    break

                position = f.tell()

                query = line.strip()

                if not query:
                    continue

                finished = False
                while not finished:
                    try:
                        connection = execute_query(connection, query)
                        logger.info("Executed query: %s", query)
                        finished = True
                    except psycopg2.Error as e:
                        logger.info("Database error: %s", e)

                    # Decide what you want here:
                    # - retry
                    # - skip
                    # - stop the worker

        time.sleep(POLL_INTERVAL)


def main():
    parser = argparse.ArgumentParser(description="Database query worker")

    parser.add_argument(
        "query_file",
        type=Path,
        help="Path to the query file",
    )

    args = parser.parse_args()

    process_queries(Path(args.query_file))


if __name__ == "__main__":
    main()
