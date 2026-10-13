"""API to perform interactions with the postgresql database in the context of the OpenAgri project"""

import logging
import multiprocessing
import time

import psycopg2
import psycopg2.extras


def _execute_query_process(config: dict, query: str, commit: bool, result_queue):
    """
    Runs in a separate process.

    Creates its own PostgreSQL connection, executes the query,
    and puts either the result or exception in result_queue.
    """
    connection = None
    cursor = None

    try:
        connection = psycopg2.connect(
            database=config["pg_database"],
            host=config["pg_host"],
            user=config["pg_user"],
            password=config["pg_password"],
            port=config["pg_port"],
            options="-c statement_timeout=250",
            keepalives=1,
            keepalives_idle=5,
            keepalives_interval=2,
            keepalives_count=3,
        )

        cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cursor.execute(query)
        values = cursor.fetchall()

        if commit:
            connection.commit()

        result_queue.put(("success", values))

    except Exception as ex:
        if connection is not None:
            try:
                connection.rollback()
            except Exception:
                pass

        result_queue.put(("error", ex))

    finally:
        if cursor is not None:
            cursor.close()

        if connection is not None:
            connection.close()


class DatabaseApi:

    _expected_config_keys = [
        "pg_database",
        "pg_host",
        "pg_user",
        "pg_password",
        "pg_port",
    ]

    def __init__(self, config: dict):
        self._logger = logging.getLogger(__name__)
        self._logger.setLevel(logging.INFO)

        if not all(
            config_key in self._expected_config_keys
            for config_key in list(config.keys())
        ):
            self._logger.error("Not all expected configuration options were provided.")
            self._logger.error(
                "Please pass: %s",
                self._expected_config_keys,
            )
            raise ValueError("Not all arguments were passed for the DatabaseApi.")

        self._config = config

    def _execute_query(self, query: str, commit: bool = False) -> list:
        """
        Execute a query in a separate process.

        The child process creates its own PostgreSQL connection.
        """

        result_queue = multiprocessing.Queue()

        process = multiprocessing.Process(
            target=_execute_query_process,
            args=(
                self._config,
                query,
                commit,
                result_queue,
            ),
        )

        start_time = time.time()

        self._logger.info(
            "Starting query process for: %s",
            query[:32].strip(),
        )

        process.start()
        process.join(0.5)

        if process.is_alive():
            self._logger.warning(
                "Query exceeded timeout of %.3f seconds, terminating process",
                0.5,
            )

            process.terminate()
            process.join()  # Wait until it is actually gone

            raise TimeoutError(f"Query exceeded timeout of {0.5} seconds")

        elapsed = time.time() - start_time

        self._logger.info(
            "Executed query in %f seconds",
            elapsed,
        )

        if result_queue.empty():
            raise RuntimeError("Query process terminated without returning a result")

        status, result = result_queue.get()

        if status == "error":
            raise RuntimeError("An error occurred during query execution") from result

        return result

    def enqueue_query(
        self,
        query: str,
        commit: bool = False,
        wait: bool = True,
    ):
        """
        Execute a query in a separate process.

        If wait=True, blocks until the query has completed and returns
        the result.

        If wait=False, starts the process and returns immediately.
        """

        if wait:
            return self._execute_query(query, commit)

        process = multiprocessing.Process(
            target=_execute_query_process,
            args=(
                self._config,
                query,
                commit,
                multiprocessing.Queue(),
            ),
        )

        process.start()

        return None

    def stop_worker(self):
        """
        Kept for backwards compatibility.

        There is no persistent worker anymore, so nothing needs
        to be stopped.
        """
        pass

    # /projects
    def get_project(self, **kwargs):
        """Get a project based on name or id"""
        where = ""
        if "id" in kwargs:
            where = f"WHERE id = '{kwargs['id']}'"
        elif "name" in kwargs:
            where = f"WHERE name = '{kwargs['name']}'"
        values = self.enqueue_query(f"SELECT * from projects project {where}")
        return values

    def get_project_summary(self, **kwargs):
        """Get a project based on name or id"""
        where = ""
        if "id" in kwargs:
            where = f"WHERE project.id = '{kwargs['id']}'"
        elif "name" in kwargs:
            where = f"WHERE project.name = '{kwargs['name']}'"

        if "start" in kwargs:
            where += f" AND flight.timestamp_epoch > {kwargs['start']}"
        if "end" in kwargs:
            where += f" AND flight.timestamp_epoch < {kwargs['end']}"

        values = self.enqueue_query(f"""SELECT 
            project.id as project_id,
            flight.id as flight_id,
            project.*,
            flight.timestamp_epoch,
            (
                SELECT COUNT(*)
                FROM images image	
                WHERE image.flight_id = flight.id
                GROUP BY flight.id
            ) as image_count,
            (
                SELECT COUNT(DISTINCT image.id)
                FROM images image
                INNER JOIN detections detection ON image.id = detection.image_id
                WHERE image.flight_id = flight.id
                GROUP BY flight.id
            ) as detection_count,
            (
                SELECT COUNT(*)
                FROM images image
                LEFT JOIN detections detection ON image.id = detection.image_id
                WHERE image.flight_id = flight.id
                AND detection.confidence > 0.5
                GROUP BY flight.id
            ) as beetle_count
            FROM flights flight
            INNER JOIN projects project on project.id = flight.project_id
            {where}""")
        return values

    def post_project(self, name: str) -> int:
        """Insert a new project into the database

        Args:
            name (str): Project name

        Returns:
            int: Project ID
        """
        values = self.enqueue_query(
            f"""
            INSERT INTO projects (name) 
            VALUES ('{name}')
            RETURNING id;
            """,
            commit=True,
        )
        return values[0]["id"]

    # ------------------------
    # /projects
    def get_flight(self, **kwargs):
        """Get a flight based on name or id"""
        where = ""
        if "id" in kwargs:
            where = f"WHERE id = '{kwargs['id']}'"
        values = self.enqueue_query(f"SELECT * from flights {where}")
        return values

    def post_flight(
        self, project_id: int, flight_name: str, timestamp_epoch: int = int(time.time())
    ) -> int:
        """
        Insert a new flight into the database

        Args:
            project_id (int): Project ID
            flight_name (str): Flight name
            timestamp_epoch (int): Timestamp in epoch format

        Returns:
            int: Flight ID
        """
        values = self.enqueue_query(
            f"""
            INSERT INTO flights (project_id, name, timestamp_epoch) 
            VALUES ('{project_id}', '{flight_name}', {timestamp_epoch})
            RETURNING id;
            """,
            commit=True,
        )
        return values[0]["id"]

    # ------------------------
    # /images
    def get_images(self, flight_id: int) -> list:
        """Get the images belonging to a flight

        Args:
            flight_id (int): flight ID

        Returns:
            List: Image descriptions
        """
        values = self.enqueue_query(f"""
            SELECT image.*, COUNT(detection.*) as detection_count
            FROM images image
            LEFT JOIN detections detection ON detection.image_id = image.id AND detection.confidence > 0.5
            WHERE image.flight_id = {flight_id}
            GROUP BY image.id
            ORDER BY image.id
            """)
        return values

    def post_image(self, flight_id: int, image: dict) -> int:
        """
        Inserts an image record into the database and returns the generated ID.

        Args:
            flight_id (int): The ID of the flight associated with the image.
            image (dict): A dictionary containing image details:
                - "storage" (str): The storage location of the image.
                - "gps" (dict): A dictionary with GPS coordinates:
                    - "latitude" (float): The latitude of the image's location.
                    - "longitude" (float): The longitude of the image's location.
                - "size" (dict): A dictionary with image dimensions:
                    - "width" (int): The width of the image.
                    - "height" (int): The height of the image.

        Returns:
            int: The ID of the newly inserted image record.

        Raises:
            psycopg2.Error: If there is an issue executing the database query.
        """
        measurements = self.enqueue_query(
            f"""
            INSERT INTO images (flight_id, storage_location, gps_location, width, height)
            values (
                {flight_id},
                '{image['storage']}',
                POINT({image['gps']['latitude']}, {image['gps']['longitude']}),
                {image['size']['width']},
                {image['size']['height']}
            )
            RETURNING ID""",
            commit=True,
            wait=False,
        )
        return -1
        if not measurements:
            return -1
        else:
            return measurements[0]["id"]

    # ------------------------
    # /detections
    def get_detections(self, image_id: int) -> list:
        """Get the detections belonging to an image

        Args:
            image_id (int): Image ID

        Returns:
            List: Detection descriptions
        """
        values = self.enqueue_query(f"""
            SELECT detection.*
            FROM detections detection
            WHERE detection.image_id = {image_id}
            ORDER BY detection.id
            """)
        return values
