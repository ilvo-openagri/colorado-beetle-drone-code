from dotenv import load_dotenv
from s3 import MinioController
from threading import Thread
from timeseries import TimeseriesDbConnector
import argparse
import gphoto2 as gp
import logging
import os
import requests
import sys
import time
from flask import Flask, jsonify, request
from threading import Event

if os.path.exists(".env.testing"):
    load_dotenv(".env.testing")


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
    """

    while True:
        if event_running.is_set():
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
            camera_file.save(unique_filename)

            # Write to S3:
            minio_controller.upload_file(
                bucket=os.environ.get("MINIO_RAW_BUCKET"),
                file_path=unique_filename,
                object_name=unique_filename,
                delete=True,
            )

            timeseries_db_controller.add_to_queue(
                {
                    "picture_time": picture_time,
                    "download_time": time.time() - download_start_time,
                }
            )

            # Allow some delay to avoid overwhelming the camera
            time.sleep(0.1)


def capture_loop_test(
    logger,
    event_running: Event,
    timeseries_db_controller,
    minio_controller: MinioController,
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
    """
    while True:
        if event_running.is_set():
            start_time = time.time()

            # Simulate capturing the image
            logger.info("Simulated capture")

            picture_time = time.time() - start_time
            download_start_time = time.time()
            url = "https://www.google.be/images/branding/googlelogo/1x/googlelogo_light_color_272x92dp.png"
            unique_filename = f"image_capture_{time.time_ns()}.png"
            response = requests.get(url, timeout=10)
            with open(unique_filename, "wb") as file:
                file.write(response.content)

            # Write to S3:
            minio_controller.upload_file(
                bucket_name=os.environ.get("MINIO_RAW_BUCKET"),
                file_path=unique_filename,
                object_name=unique_filename,
                delete=True,
            )

            # Log statistics to the timeseries database
            timeseries_db_controller.add_to_queue(
                {
                    "picture_time": picture_time,
                    "download_time": time.time() - download_start_time,
                }
            )

            # Allow some delay to avoid overwhelming the camera
            time.sleep(1)


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

    # Create Flask REST API:
    app = Flask(__name__)

    # Event to control the capture loop
    event_running = Event()
    event_running.clear()
    Thread(
        target=capture_loop_test if bypass_camera else capture_loop_camera,
        args=(logger, event_running, timeseries_db_controller, minio_controller),
        daemon=True,
    ).start()

    @app.route("/start", methods=["POST"])
    def start_capture():
        """
        Start the camera capture operation.
        """
        if event_running.is_set():
            return jsonify({"message": "Capture operation already running"}), 400

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
        status = "running" if event_running.is_set() else "stopped"
        return jsonify({"status": status}), 200

    # Start the Flask app in a separate thread
    app.run("0.0.0.0", 5000)

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
    args = parser.parse_args()

    # Set up logging
    logging.basicConfig(
        format="[%(levelname)s %(name)s @ %(asctime)s] %(message)s",
        level=getattr(logging, args.log_level, logging.DEBUG),
    )

    # Run the main function
    sys.exit(main(args.test))
