from database import DatabaseApi
from dotenv import load_dotenv
from s3 import MinioController
from threading import Thread
from timeseries import TimeseriesDbConnector
import argparse
import gphoto2 as gp
import imageio
import logging
import numpy as np
import os
import cv2
import requests
import sys
import time
from flask import Flask, jsonify, request, Response
from threading import Event

live_stream_image = None


class _Operations:
    """
    A class to handle operations based on a bitmap and a predefined set of allowed operations.
    """

    _allowed_operations = {}

    def __init__(self, bitmap: int):
        self.values = []
        for key, value in self._allowed_operations.items():
            if key & bitmap == 0:
                continue
            self.values.append(value)


class CameraOperations(_Operations):
    """
    A class to handle camera operations based on a bitmap of allowed operations.
    This class extends the `_Operations` base class and provides a mapping of
    operation constants to their corresponding string representations.
    """

    def __init__(self, bitmap: int):
        self._allowed_operations = {
            gp.GP_OPERATION_NONE: "NONE",
            gp.GP_OPERATION_CAPTURE_IMAGE: "CAPTURE_IMAGE",
            gp.GP_OPERATION_CAPTURE_VIDEO: "CAPTURE_VIDEO",
            gp.GP_OPERATION_CAPTURE_AUDIO: "CAPTURE_AUDIO",
            gp.GP_OPERATION_CAPTURE_PREVIEW: "CAPTURE_PREVIEW",
            gp.GP_OPERATION_CONFIG: "CONFIG",
            gp.GP_OPERATION_TRIGGER_CAPTURE: "TRIGGER_CAPTURE",
        }
        super().__init__(bitmap)


class CameraFileOperations(_Operations):
    """
    A class to handle file operations for a camera, extending the base `_Operations` class.
    """

    def __init__(self, bitmap: int):
        self._allowed_operations = {
            gp.GP_FILE_OPERATION_NONE: "NONE",
            gp.GP_FILE_OPERATION_DELETE: "DELETE",
            gp.GP_FILE_OPERATION_PREVIEW: "PREVIEW",
            gp.GP_FILE_OPERATION_RAW: "RAW",
            gp.GP_FILE_OPERATION_AUDIO: "AUDIO",
            gp.GP_FILE_OPERATION_EXIF: "EXIF",
        }
        super().__init__(bitmap)


class CameraFolderOperations(_Operations):
    """
    A class to handle camera folder operations based on a bitmap of allowed operations.
    This class extends the `_Operations` base class and provides a mapping of
    folder operation constants to their string representations.
    """

    def __init__(self, bitmap: int):
        self._allowed_operations = {
            gp.GP_FOLDER_OPERATION_NONE: "NONE",
            gp.GP_FOLDER_OPERATION_DELETE_ALL: "DELETE_ALL",
            gp.GP_FOLDER_OPERATION_PUT_FILE: "PUT_FILE",
            gp.GP_FOLDER_OPERATION_MAKE_DIR: "MAKE_DIR",
            gp.GP_FOLDER_OPERATION_REMOVE_DIR: "REMOVE_DIR",
        }
        super().__init__(bitmap)


def tree(camera: gp.Camera, folder: str, depth: int = 0):
    """
    Recursively prints the folder structure of a camera.
    Args:
        camera (gphoto2.Camera): Camera instance used to retrieve folder structure.
        folder (str): The folder path on the camera to start printing from.
        depth (int): The current depth in the folder structure for indentation.
    """
    for entry in gp.gp_camera_folder_list_files(camera, folder)[1]:
        file_entry = entry[0]
        print(f"{' '*depth*2}|-- {file_entry}")
    for entry in gp.gp_camera_folder_list_folders(camera, folder)[1]:
        folder_entry = entry[0]
        print(f"{' '*depth*2}\-- {folder_entry}")
        tree(camera, f"{folder}{folder_entry}/", depth + 1)


def get_files(camera: gp.Camera, folder: str, suffix: str) -> list:
    """
    Recursively retrieves files from a camera folder and its subfolders.
    Args:
        camera (gphoto2.Camera): Camera instance used to retrieve files.
        folder (str): The folder path on the camera to search for files.
        suffix (str): The file extension to filter files.
    Returns:
        list: A list of tuples containing the folder and file name of matching files.
    """
    matching_files = []
    for entry in gp.gp_camera_folder_list_files(camera, folder)[1]:
        file_entry = entry[0]
        if file_entry.endswith(suffix):
            matching_files.append(
                (
                    folder,
                    file_entry,
                )
            )
    for entry in gp.gp_camera_folder_list_folders(camera, folder)[1]:
        folder_entry = entry[0]
        matching_files.extend(get_files(camera, f"{folder}{folder_entry}/", suffix))
    return matching_files


def capture_loop_camera(
    logger,
    event_running: Event,
    camera,
    timeseries_db_controller,
    minio_controller: MinioController,
    database_controller: DatabaseApi,
):
    """
    Continuously captures images from a camera, logs the file path, and downloads
    the images in a separate thread while recording metadata in a timeseries database.

    Args:
        logger (logging.Logger): Logger instance for logging information about the
            captured images.
        event_running (Event): Event object to control the capture loop.
        camera (gphoto2.Camera): Camera instance used to capture images and retrieve
            files.
        timeseries_db_controller (object): Controller instance responsible for
            managing and queuing metadata related to the captured images.
        minio_controller (MinioController): Controller instance for managing
            MinIO uploads.
        database_controller (DatabaseApi): Controller instance for managing
            database operations.
    """
    global live_stream_image
    thread_data = {
        "project_id": -1,
        "flight_id": -1,
        "project_name": "",
        "flight_name": "",
    }

    while True:
        if event_running.is_set():
            # Determine project and flight:
            if "PROJECT_ID" not in os.environ:
                continue
            if "FLIGHT_ID" not in os.environ:
                continue

            if thread_data["project_id"] != int(
                os.environ["PROJECT_ID"]
            ) and thread_data["flight_id"] != int(os.environ["FLIGHT_ID"]):
                thread_data["project_id"] = int(os.environ["PROJECT_ID"])
                thread_data["flight_id"] = int(os.environ["FLIGHT_ID"])
                project = database_controller.get_project(id=thread_data["project_id"])
                flight = database_controller.get_flight(id=thread_data["flight_id"])
                thread_data["project_name"] = project[0]["name"]
                thread_data["flight_name"] = flight[0]["name"]
                logger.info(
                    "Project: %s, Flight: %s",
                    thread_data["project_name"],
                    thread_data["flight_name"],
                )

            # Start image capture:
            start_time = time.time()

            # Capture the image with error handling
            try:
                file_path = camera.capture(gp.GP_CAPTURE_IMAGE)
                logger.info(f"{file_path.folder}, {file_path.name}")
            except gp.GPhoto2Error as ex:
                logger.error(f"Failed to capture image: {ex}")
                time.sleep(1)  # Allow some delay before retrying
                continue

            picture_time = time.time() - start_time
            download_start_time = time.time()
            camera_file = camera.file_get(
                file_path.folder, file_path.name, gp.GP_FILE_TYPE_NORMAL
            )
            unique_filename = f"image_{int(time.time())}.JPG"
            s3_filename = f"{thread_data['project_id']}/{thread_data['flight_id']}/{unique_filename}"
            camera_file.save(unique_filename)
            image_metadata = imageio.get_reader(unique_filename).get_meta_data()
            picture_size = os.path.getsize(unique_filename)

            # Data for live stream:
            with open(unique_filename, "rb") as file:
                file_content = file.read()
            live_stream_image = np.asarray(bytearray(file_content), dtype=np.uint8)
            live_stream_image = cv2.imdecode(live_stream_image, cv2.IMREAD_COLOR)
            live_stream_image = cv2.resize(
                live_stream_image, (100, 100), interpolation=cv2.INTER_LINEAR
            )

            # Add the image reference to the database:
            image_id = database_controller.post_image(
                thread_data["flight_id"],
                {
                    "storage": s3_filename,
                    "gps": {
                        "latitude": 0,
                        "longitude": 0,
                    },
                    "size": {
                        "width": image_metadata["shape"][0],
                        "height": image_metadata["shape"][1],
                    },
                },
            )

            # Write to S3:
            minio_controller.upload_file(
                bucket_name=os.environ.get("MINIO_RAW_BUCKET"),
                file_path=unique_filename,
                object_name=s3_filename,
                delete=True,
                metadata={
                    "project_id": thread_data["project_id"],
                    "flight_id": thread_data["flight_id"],
                    "image_id": image_id,
                },
            )

            # Log statistics to the timeseries database
            timeseries_db_controller.add_to_queue(
                {
                    "picture_time": picture_time,
                    "download_time": time.time() - download_start_time,
                    "picture_size": picture_size,
                    "minio_queue_size": len(minio_controller),
                    "timeseries_queue_size": len(timeseries_db_controller) + 1,
                }
            )

            # Allow some delay to avoid overwhelming the camera
            time.sleep(0.1)


def capture_loop_test(
    logger,
    event_running: Event,
    timeseries_db_controller,
    minio_controller: MinioController,
    database_controller: DatabaseApi,
):
    """
    Continuously simulates capturing images, downloads them in a separate thread,
    and logs the download time to a timeseries database.
    Args:
        logger (logging.Logger): Logger instance for logging messages.
        event_running (Event): Event object to control the capture loop.
        timeseries_db_controller (object): Controller object responsible for
                                           managing the timeseries database queue.
        minio_controller (MinioController): Controller instance for managing
            MinIO uploads.
        database_controller (DatabaseApi): Controller instance for managing
            database operations.
    """
    global live_stream_image

    thread_data = {
        "project_id": -1,
        "flight_id": -1,
        "project_name": "",
        "flight_name": "",
    }

    images = [
        "https://i0.wp.com/www.potatonewstoday.com/wp-content/uploads/2023/06/CPB-Pavlo-from-Pixabay.png?fit=900%2C600&ssl=1",
        "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcTcGL80DcP5w0bNunIFBzkk_vyrXdYuK5Uklg&s",
        "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcSqYu_CbWGn3mKcOq4EBRVXFtBplz1-_vatRg&s",
    ]
    image_index = 0
    while True:
        try:
            if event_running.is_set():
                if "PROJECT_ID" not in os.environ:
                    continue
                if "FLIGHT_ID" not in os.environ:
                    continue

                if thread_data["project_id"] != int(
                    os.environ["PROJECT_ID"]
                ) or thread_data["flight_id"] != int(os.environ["FLIGHT_ID"]):
                    thread_data["project_id"] = int(os.environ["PROJECT_ID"])
                    thread_data["flight_id"] = int(os.environ["FLIGHT_ID"])
                    project = database_controller.get_project(
                        id=thread_data["project_id"]
                    )
                    flight = database_controller.get_flight(id=thread_data["flight_id"])
                    thread_data["project_name"] = project[0]["name"]
                    thread_data["flight_name"] = flight[0]["name"]
                    logger.info(
                        "Project: %s, Flight: %s",
                        thread_data["project_name"],
                        thread_data["flight_name"],
                    )

                start_time = time.time()

                # Simulate capturing the image
                logger.info(
                    "Simulated capture [PROJECT %s FLIGHT %s]",
                    thread_data["project_name"],
                    thread_data["flight_name"],
                )

                picture_time = time.time() - start_time
                download_start_time = time.time()
                url = images[image_index % len(images)]
                image_index += 1
                unique_filename = f"image_capture_{time.time_ns()}.png"
                s3_filename = f"{thread_data['project_id']}/{thread_data['flight_id']}/{unique_filename}"
                response = requests.get(url, timeout=10)
                with open(unique_filename, "wb") as file:
                    file.write(response.content)

                image_metadata = imageio.get_reader(unique_filename).get_meta_data()

                # Data for live stream:
                live_stream_image = np.asarray(
                    bytearray(response.content), dtype=np.uint8
                )
                live_stream_image = cv2.imdecode(live_stream_image, cv2.IMREAD_COLOR)
                live_stream_image = cv2.resize(
                    live_stream_image, (100, 100), interpolation=cv2.INTER_LINEAR
                )

                # Add the image reference to the database:
                image_id = database_controller.post_image(
                    thread_data["flight_id"],
                    {
                        "storage": s3_filename,
                        "gps": {
                            "latitude": 0,
                            "longitude": 0,
                        },
                        "size": {
                            "width": image_metadata["shape"][0],
                            "height": image_metadata["shape"][1],
                        },
                    },
                )

                # Write to S3:
                minio_controller.upload_file(
                    bucket_name=os.environ.get("MINIO_RAW_BUCKET"),
                    file_path=unique_filename,
                    object_name=s3_filename,
                    delete=True,
                    metadata={
                        "project_id": thread_data["project_id"],
                        "flight_id": thread_data["flight_id"],
                        "image_id": image_id,
                    },
                )

                # Log statistics to the timeseries database
                timeseries_db_controller.add_to_queue(
                    {
                        "picture_time": picture_time,
                        "download_time": time.time() - download_start_time,
                        "picture_size": len(response.content),
                        "minio_queue_size": len(minio_controller),
                        "timeseries_queue_size": len(timeseries_db_controller) + 1,
                    }
                )
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download image: {e}")
            continue

        # Allow some delay to avoid overwhelming the camera
        time.sleep(1)


def gen_video_feed():
    global live_stream_image
    while True:
        if live_stream_image is not None:
            try:
                ret, buffer = cv2.imencode(".jpg", live_stream_image)
                yield (
                    b"--frame\r\n"
                    b"Content-Type: image/jpeg\r\n\r\n" + buffer.tobytes() + b"\r\n"
                )
            except AttributeError as e:
                time.sleep(1)  # Adjust for frame rate
                continue
        time.sleep(1)  # Adjust for frame rate


def main(bypass_camera: bool = False):
    """
    Main function to initialize the camera, connect to a timeseries database, and continuously capture and save images.
    It captures images from the camera, saves them to the local filesystem, and logs the time taken for capturing and downloading each image.

    Args:
        bypass_camera (bool): If True, bypasses the camera initialization and runs in test mode.

    Raises:
        gp.GPhoto2Error: If an unhandled gphoto2 error occurs during camera initialization.
    """
    logger = logging.getLogger(__name__)

    # Initialize the camera
    if bypass_camera:
        camera_model = "Simulated Camera"
        logger.info("Bypassing camera initialization for testing.")
    else:
        camera_initialized = False
        camera = gp.Camera()
        while not camera_initialized:
            try:
                camera.init()
                camera_initialized = True
            except gp.GPhoto2Error as ex:
                if ex.code == gp.GP_ERROR_MODEL_NOT_FOUND:
                    print(f"Please connect and switch on your camera [{ex}]")
                    time.sleep(2)
                    continue
                # some other error we can't handle here
                raise

        camera_abilities = gp.gp_camera_get_abilities(camera)

        if isinstance(camera_abilities, (tuple, list)) and camera_abilities[0] == 0:
            camera_abilities = camera_abilities[1]

        camera_model = camera_abilities.model

        # Print camera information:
        logger.debug("Camera summary: %s", camera.get_summary())
        logger.debug("Camera model %s", camera_abilities.model)
        logger.debug(
            "Camera allowed operations %s",
            CameraOperations(camera_abilities.operations).values,
        )
        logger.debug(
            "Camera allowed file operations %s",
            CameraFileOperations(camera_abilities.file_operations).values,
        )
        logger.debug(
            "Camera allowed folder operations %s",
            CameraFolderOperations(camera_abilities.folder_operations).values,
        )

    # Connect to the timeseries database
    timeseries_db_controller = TimeseriesDbConnector(
        measurement="raspi-camera",
        telegraf_ip=os.environ.get("TELEGRAF_IP"),
        telegraf_port=os.environ.get("TELEGRAF_PORT"),
        tags={
            "device": camera_model,
        },
    )
    timeseries_db_controller.start()

    # Initialize MinIO controller:
    minio_controller = MinioController(
        minio_url=os.environ.get("MINIO_URL"),
        access_key=os.environ.get("MINIO_ACCESS_KEY"),
        secret_key=os.environ.get("MINIO_SECRET_KEY"),
    )
    minio_controller.start()

    # Initialize database controller:
    database_controller = DatabaseApi(
        config={
            "pg_host": os.environ.get("PG_CLOUD_HOST"),
            "pg_port": int(os.environ.get("PG_CLOUD_PORT")),
            "pg_database": os.environ.get("PG_CLOUD_DATABASE"),
            "pg_user": os.environ.get("PG_CLOUD_USER"),
            "pg_password": os.environ.get("PG_CLOUD_PASSWORD"),
        }
    )

    # Create Flask REST API:
    app = Flask(__name__)

    # Event to control the capture loop
    event_running = Event()
    event_running.clear()
    if bypass_camera:
        app_thread = Thread(
            target=capture_loop_test,
            args=(
                logger,
                event_running,
                timeseries_db_controller,
                minio_controller,
                database_controller,
            ),
            daemon=True,
        )
    else:
        app_thread = Thread(
            target=capture_loop_camera,
            args=(
                logger,
                event_running,
                camera,
                timeseries_db_controller,
                minio_controller,
                database_controller,
            ),
            daemon=True,
        )
    app_thread.start()

    @app.route("/start", methods=["POST"])
    def start_capture():
        """
        Start the camera capture operation.
        """
        if event_running.is_set():
            return jsonify({"message": "Capture operation already running"}), 400

        # Read JSON payload
        payload = request.get_json()
        if "project_id" not in payload:
            return jsonify({"error": "Missing project_id in payload"}), 400
        if "flight_id" not in payload:
            return jsonify({"error": "Missing flight_id in payload"}), 400

        # Expose project_id and flight_id to capture_loop_camera
        os.environ["PROJECT_ID"] = str(payload["project_id"])
        os.environ["FLIGHT_ID"] = str(payload["flight_id"])
        event_running.set()
        return jsonify({"message": "Capture operation started"}), 200

    @app.route("/stop", methods=["POST"])
    def stop_capture():
        """
        Stop the camera capture operation.
        """
        if not event_running.is_set():
            return jsonify({"message": "Capture operation is not running"}), 400

        event_running.clear()
        return jsonify({"message": "Capture operation stopped"}), 200

    @app.route("/status", methods=["GET"])
    def status():
        """
        Get the current status of the capture operation.
        """
        response = {
            "status": "running" if event_running.is_set() else "stopped",
        }
        if event_running.is_set():
            if "PROJECT_ID" in os.environ:
                response["project_id"] = os.environ["PROJECT_ID"]
            if "FLIGHT_ID" in os.environ:
                response["flight_id"] = os.environ["FLIGHT_ID"]

        return jsonify(response), 200

    @app.route("/video_feed")
    def video_feed():
        return Response(
            gen_video_feed(), mimetype="multipart/x-mixed-replace; boundary=frame"
        )

    # Start the Flask app in a separate thread
    app.run("192.168.255.5", 5000)

    # # Take a picture and download:
    # if bypass_camera:
    #     capture_loop_test(logger, timeseries_db_controller, minio_controller)
    # else:
    #     capture_loop_camera(logger, camera, timeseries_db_controller, minio_controller)


if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Camera capture script")
    parser.add_argument(
        "--test",
        action=argparse.BooleanOptionalAction,
        help="Run the script in test mode (no actual camera interaction)",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Set the logging level (default: DEBUG or LOG_LEVEL env variable)",
    )
    parser.add_argument(
        "--env",
        type=str,
        default=".env.testing",
        help="Set the environment file to load (default: .env.testing)",
    )

    args = parser.parse_args()

    # Set up logging
    logging.basicConfig(
        format="[%(levelname)s %(name)s @ %(asctime)s] %(message)s",
        level=getattr(logging, args.log_level, logging.DEBUG),
    )

    # Load environment variables from the specified file
    load_dotenv(args.env)

    # Run the main function
    sys.exit(main(args.test))
