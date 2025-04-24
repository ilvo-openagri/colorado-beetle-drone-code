"""API to perform interactions with the postgresql database in the context of the OpenAgri project"""

import logging
import psycopg2
import psycopg2.extras
import time


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
            self._logger.error("Please pass: %s", self._expected_config_keys)
            raise ValueError("Not all arguments were passed for the DatabaseApi.")

        self._config = config
        self._reconnect()
        self._logger.info("Established database connection")

    def _reconnect(self):
        """(Re-)connect to the database"""
        self._db_connection = psycopg2.connect(
            database=self._config["pg_database"],
            host=self._config["pg_host"],
            user=self._config["pg_user"],
            password=self._config["pg_password"],
            port=self._config["pg_port"],
        )

    def _execute_query(self, query: str) -> list:
        """Execute a query in the database

        Args:
            query (str): Query

        Raises:
            RuntimeError: When the database connector raises an exception

        Returns:
            list: Results
        """

        try:
            cursor = self._db_connection.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor
            )
        except psycopg2.InterfaceError as ex:
            self._reconnect()
            cursor = self._db_connection.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor
            )
        try:
            cursor.execute(query)
            values = cursor.fetchall()
            self._db_connection.commit()
        except psycopg2.errors.UniqueViolation as ex:
            self._db_connection.rollback()
            raise RuntimeError("Unique constraint violated") from ex
        except Exception as ex:
            self._db_connection.rollback()
            raise RuntimeError("An error occurred during query execution") from ex
        finally:
            cursor.close()
        return values

    # /projects
    def get_project(self, **kwargs):
        """Get a project based on name or id"""
        where = ""
        if "id" in kwargs:
            where = f"WHERE id = '{kwargs['id']}'"
        elif "name" in kwargs:
            where = f"WHERE name = '{kwargs['name']}'"

        values = self._execute_query(f"SELECT * from projects project {where}")
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

        values = self._execute_query(
            f"""SELECT 
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
            {where}"""
        )
        return values

    def post_project(self, name: str) -> int:
        """Insert a new project into the database

        Args:
            name (str): Project name

        Returns:
            int: Project ID
        """
        values = self._execute_query(
            f"""
            INSERT INTO projects (name) 
            VALUES ('{name}')
            RETURNING id;
            """
        )
        return values[0]["id"]

    # ------------------------
    # /projects
    def get_flight(self, **kwargs):
        """Get a flight based on name or id"""
        where = ""
        if "id" in kwargs:
            where = f"WHERE id = '{kwargs['id']}'"
        values = self._execute_query(f"SELECT * from flights {where}")
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
        values = self._execute_query(
            f"""
            INSERT INTO flights (project_id, name, timestamp_epoch) 
            VALUES ('{project_id}', '{flight_name}', {timestamp_epoch})
            RETURNING id;
            """
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
        values = self._execute_query(
            f"""
            SELECT image.*, COUNT(detection.*) as detection_count
            FROM images image
            LEFT JOIN detections detection ON detection.image_id = image.id AND detection.confidence > 0.5
            WHERE image.flight_id = {flight_id}
            GROUP BY image.id
            ORDER BY image.id
            """
        )
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

        query = "INSERT INTO images (flight_id, storage_location, gps_location, width, height) values (%s, %s, POINT(%s, %s), %s, %s) RETURNING ID"
        with self._db_connection.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor
        ) as cursor:
            cursor.execute(
                query,
                (
                    flight_id,
                    image["storage"],
                    image["gps"]["latitude"],
                    image["gps"]["longitude"],
                    image["size"]["width"],
                    image["size"]["height"],
                ),
            )
            measurement = cursor.fetchone()
        self._db_connection.commit()
        return measurement["id"]

    # ------------------------
    # /detections
    def get_detections(self, image_id: int) -> list:
        """Get the detections belonging to an image

        Args:
            image_id (int): Image ID

        Returns:
            List: Detection descriptions
        """
        values = self._execute_query(
            f"""
            SELECT detection.*
            FROM detections detection
            WHERE detection.image_id = {image_id}
            ORDER BY detection.id
            """
        )
        return values
