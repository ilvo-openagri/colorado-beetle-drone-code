from database import DatabaseApi
from dotenv import load_dotenv
from flask import Flask, jsonify, request, Response
from s3 import MinioController
from threading import Event
from threading import Thread
from timeseries import TimeseriesDbConnector
import argparse
import cv2
import gphoto2 as gp
import imageio
import logging
import numpy as np
import os
import re
import requests
import subprocess
import sys
import threading
import time

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


class ThreadState:
    """
    This class provides a simple interface to track the running state of a thread
    and store a message associated with that state.
    """

    def __init__(self):
        self.is_running: bool = False
        self.message: str = ""


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


def handle_gphoto2_output(
    logger: logging.Logger,
    params: dict,
    process: subprocess.Popen,
    event_running: Event,
    timeseries_db_controller: TimeseriesDbConnector,
    minio_controller: MinioController,
    database_controller: DatabaseApi,
):
    """
    Handles the output of a gphoto2 process.
    Args:
        process (subprocess.Popen): The gphoto2 process to handle.
    """
    re_saving_file = re.compile(r"Saving file as.*")
    re_delete_file = re.compile(r"Deleting file /(.*.jpg) on the camera")
    re_capture = re.compile(r"Capturing frame.*")
    ts_capture = 0
    ts_saving = 0
    while event_running.is_set():
        output = process.stdout.readline()
        if output == b"" and process.poll() is not None:
            break
        if not output:
            continue
        output = output.strip().decode("utf-8")

        # Check for the capture start message:
        match_capture = re_capture.match(output)
        if match_capture:
            ts_capture = time.time()
            continue

        # Check for the saving file message:
        match_saving_file = re_saving_file.match(output)
        if match_saving_file:
            ts_saving = time.time()
            continue

        # Check for the delete file message:
        match_delete_file = re_delete_file.match(output)
        if match_delete_file:
            ts_deleting = time.time()
            picture_file = match_delete_file.group(1)
            logger.info("Handling file %s", picture_file)
            s3_filename = f"{params['project_id']}/{params['flight_id']}/image_{int(time.time())}.JPG"
            logger.info("Writing to %s", s3_filename)
            image_metadata = imageio.get_reader(picture_file).get_meta_data()
            picture_size = os.path.getsize(picture_file)

            # Data for live stream:
            with open(picture_file, "rb") as file:
                file_content = file.read()
            live_stream_image = np.asarray(bytearray(file_content), dtype=np.uint8)
            live_stream_image = cv2.imdecode(live_stream_image, cv2.IMREAD_COLOR)
            live_stream_image = cv2.resize(
                live_stream_image, (100, 100), interpolation=cv2.INTER_LINEAR
            )
            try:
                # Add the image reference to the database:
                image_id = database_controller.post_image(
                    params["flight_id"],
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
                    file_path=picture_file,
                    object_name=s3_filename,
                    delete=True,
                    metadata={
                        "project_id": params["project_id"],
                        "flight_id": params["flight_id"],
                        "image_id": image_id,
                    },
                )

                # Log statistics to the timeseries database
                timeseries_db_controller.add_to_queue(
                    {
                        "picture_time": ts_saving - ts_capture,
                        "download_time": ts_deleting - ts_saving,
                        "picture_size": picture_size,
                        "minio_queue_size": len(minio_controller),
                        "timeseries_queue_size": len(timeseries_db_controller) + 1,
                    }
                )
            except RuntimeError as ex:
                pass

        # Allow some delay to not lock the thread:
        time.sleep(0.1)

    rc = process.poll()
    return rc


def capture_loop_gphoto2_cli(
    logger: logging.Logger,
    app_state: ThreadState,
    event_running: Event,
    timeseries_db_controller: TimeseriesDbConnector,
    minio_controller: MinioController,
    database_controller: DatabaseApi,
):
    """
    Continuously captures images from a camera, logs the file path, and downloads
    the images in a separate thread while recording metadata in a timeseries database.

    Args:
        logger (logging.Logger): Logger instance for logging information about the
            captured images.
        debug (bool): If True, enables debug mode for the GPHOTO2 library.
        event_running (Event): Event object to control the capture loop.
        timeseries_db_controller (TimeseriesDbConnector): Controller instance responsible for
            managing and queuing metadata related to the captured images.
        minio_controller (MinioController): Controller instance for managing
            MinIO uploads.
        database_controller (DatabaseApi): Controller instance for managing
            database operations.
    """

    params = {
        "project_id": -1,
        "flight_id": -1,
        "project_name": "",
        "flight_name": "",
    }

    while True:
        # Check if the event is set to run:
        app_state.is_running = event_running.is_set()
        if not app_state.is_running:
            time.sleep(1)
            continue
        app_state.message = "Running"

        # Determine project and flight:
        if "PROJECT_ID" not in os.environ:
            continue
        if "FLIGHT_ID" not in os.environ:
            continue

        if params["project_id"] != int(os.environ["PROJECT_ID"]) and params[
            "flight_id"
        ] != int(os.environ["FLIGHT_ID"]):
            params["project_id"] = int(os.environ["PROJECT_ID"])
            params["flight_id"] = int(os.environ["FLIGHT_ID"])
            project = database_controller.get_project(id=params["project_id"])
            flight = database_controller.get_flight(id=params["flight_id"])
            params["project_name"] = project[0]["name"]
            params["flight_name"] = flight[0]["name"]
            logger.info(
                "Project: %s, Flight: %s",
                params["project_name"],
                params["flight_name"],
            )

        # Check if the camera is connected:
        logger.info("Capturing images using gphoto2 CLI")
        camera = {}
        while len(camera) == 0:
            gphoto2_detect = (
                subprocess.run(["gphoto2", "--auto-detect"], capture_output=True)
                .stdout.decode()
                .splitlines()
            )
            if len(gphoto2_detect) < 3:
                logger.error("No camera detected")
                time.sleep(2)
                continue
            model_offset = re.search(" *usb", gphoto2_detect[2]).start()
            port_offset = re.search("usb", gphoto2_detect[2]).start()
            camera = {
                "model": gphoto2_detect[2][0:model_offset].strip(),
                "port": gphoto2_detect[2][port_offset:].strip(),
            }
        logger.info("Camera detected: %s", camera["model"])
        timeseries_db_controller.set_tags(camera)
        timeseries_db_controller.start()

        # Take a focus shot:
        if False:
            logger.info("Focusing camera")
            subprocess.run(
                [
                    "gphoto2",
                    "--set-config",
                    "/main/capturesettings/focusmode=1",
                    "--trigger-capture",
                ]
            )

        # Start capture loop:
        time.sleep(1)
        logger.info("Starting capture loop")
        capture_process = subprocess.Popen(
            [
                "gphoto2",
                "--set-config",
                "/main/capturesettings/focusmode=0",
                "--capture-image-and-download",
                "--interval=5",
                "--force-overwrite",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,  # Optional: merge stderr with stdout
            bufsize=1,
        )
        # Start a thread to read the output
        capture_read_thread = threading.Thread(
            target=handle_gphoto2_output,
            args=(
                logger,
                params,
                capture_process,
                event_running,
                timeseries_db_controller,
                minio_controller,
                database_controller,
            ),
            daemon=True,
        )

        capture_read_thread.start()

        # Keep running until the event is cleared:
        while event_running.is_set():
            time.sleep(1)

        capture_process.terminate()
        capture_read_thread.join()


        timeseries_db_controller.stop()

        # Complete:
        logger.info("Clean-up completed")


def capture_loop_test(
    logger: logging.Logger,
    app_state: ThreadState,
    event_running: Event,
    timeseries_db_controller: TimeseriesDbConnector,
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
    """
    params = {
        "project_id": -1,
        "flight_id": -1,
        "project_name": "",
        "flight_name": "",
        "download": False,
    }
    timeseries_db_controller.set_tags(
        {
            "model": "test_model",
            "port": "test_port",
        }
    )
    timeseries_db_controller.start()

    while True:
        # Check if the event is set to run:
        app_state.is_running = event_running.is_set()
        if not app_state.is_running:
            time.sleep(1)
            continue
        app_state.message = "Running"

        # Determine project and flight:
        if "PROJECT_ID" not in os.environ:
            continue
        if "FLIGHT_ID" not in os.environ:
            continue

        if params["project_id"] != int(os.environ["PROJECT_ID"]) and params[
            "flight_id"
        ] != int(os.environ["FLIGHT_ID"]):
            params["project_id"] = int(os.environ["PROJECT_ID"])
            params["flight_id"] = int(os.environ["FLIGHT_ID"])
            project = database_controller.get_project(id=params["project_id"])
            flight = database_controller.get_flight(id=params["flight_id"])
            params["project_name"] = project[0]["name"]
            params["flight_name"] = flight[0]["name"]
            logger.info(
                "Project: %s, Flight: %s",
                params["project_name"],
                params["flight_name"],
            )

        if event_running.is_set():
            start_time = time.time()

            # Simulate capturing the image
            logger.info("Simulated capture")

            picture_time = time.time() - start_time
            download_start_time = time.time()
            if params["download"]:
                url = "https://www.google.be/images/branding/googlelogo/1x/googlelogo_light_color_272x92dp.png"
                unique_filename = f"image_capture_{time.time_ns()}.png"
                response = requests.get(url, timeout=10)
                s3_filename = f"{params['project_id']}/{params['flight_id']}/image_{int(time.time())}.JPG"
                with open(unique_filename, "wb") as file:
                    file.write(response.content)
            else:
                unique_filename = "test_image.JPG"
                s3_filename = f"{params['project_id']}/{params['flight_id']}/image_{int(time.time())}.JPG"

            # Determine image metadata:
            image_metadata = imageio.get_reader(unique_filename).get_meta_data()

            # Add the image reference to the database:
            image_id = database_controller.post_image(
                params["flight_id"],
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
                delete=params["download"],
                metadata={
                    "project_id": params["project_id"],
                    "flight_id": params["flight_id"],
                    "image_id": image_id,
                },
            )

            # Log statistics to the timeseries database
            timeseries_db_controller.add_to_queue(
                {
                    "picture_time": picture_time,
                    "download_time": time.time() - download_start_time,
                }
            )

            # Allow some delay to avoid overwhelming the camera
            time.sleep(5)


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


def main(bypass_camera: bool = False, debug: bool = False):
    """
    Main function to initialize the camera, connect to a timeseries database, and continuously capture and save images.
    It captures images from the camera, saves them to the local filesystem, and logs the time taken for capturing and downloading each image.

    Args:
        bypass_camera (bool): If True, bypasses the camera initialization and runs in test mode.
        debug (bool): If True, enables debug mode for the GPHOTO2 library.

    Raises:
        gp.GPhoto2Error: If an unhandled gphoto2 error occurs during camera initialization.
    """
    logger = logging.getLogger(__name__)

    # Main state to keep track of the thread state:
    main_state = ThreadState()

    # Connect to the timeseries database
    timeseries_db_controller = TimeseriesDbConnector(
        measurement="raspi-camera",
        telegraf_ip=os.environ.get("TELEGRAF_IP"),
        telegraf_port=os.environ.get("TELEGRAF_PORT"),
    )

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
    app_thread = Thread(
        target=capture_loop_gphoto2_cli,
        args=(
            logger,
            main_state,
            event_running,
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
            "status": main_state.is_running,
            "message": main_state.message,
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
    app.run("10.8.0.9", 5000)

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
        "--debug",
        type=str,
        action=argparse.BooleanOptionalAction,
        help="Run the GPHOTO2 library in debug mode for extra output.",
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
    sys.exit(main(args.test, args.debug))
