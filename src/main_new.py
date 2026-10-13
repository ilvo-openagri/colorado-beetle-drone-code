from gevent import monkey

monkey.patch_all()

from camera import Gphoto2Camera, TestCamera
from database import DatabaseApi, QueryWriter
from dotenv import dotenv_values
from flask import Flask, jsonify, request, Response
from gevent.pywsgi import WSGIServer
from PIL import Image, ExifTags
from s3 import MinioController
from dji.controller import DjiController
from threading import Event, Thread
from timeseries import TimeseriesDbConnector
from typing import Optional
import argparse
import logging
import os
import queue
import sys
import time


class CloudConnector:
    """
    Class to handle cloud operations such as uploading images to S3 and logging statistics.
    """

    def __init__(self, name: str, cloud_config: dict, delete_after_upload: bool = True):
        self._logger = logging.getLogger(name)
        self._event_running = Event()
        self._cloud_config = cloud_config
        self._delete_after_upload = delete_after_upload
        self.image_queue: Optional[queue.Queue] = None
        self.project_id = -1
        self.flight_id = -1
        self._dji_controller = None

        gps_url = cloud_config.get("DJI_GPS_URL")
        if gps_url:
            gps_interval_s = float(cloud_config.get("DJI_GPS_INTERVAL_S", 0.25))
            gps_timeout_s = float(cloud_config.get("DJI_GPS_TIMEOUT_S", 2.0))
            self._dji_controller = DjiController(
                gps_url=gps_url,
                sample_interval_s=gps_interval_s,
                timeout_s=gps_timeout_s,
            )

        # Connect to the timeseries database
        self._timeseries_db_controller = TimeseriesDbConnector(
            measurement="raspi-camera",
            telegraf_ip=cloud_config["TELEGRAF_IP"],
            telegraf_port=cloud_config["TELEGRAF_PORT"],
        )
        self._timeseries_db_controller.start()

        # Initialize MinIO controller:
        self._minio_controller = MinioController(
            minio_url=cloud_config["MINIO_URL"],
            access_key=cloud_config["MINIO_ACCESS_KEY"],
            secret_key=cloud_config["MINIO_SECRET_KEY"],
        )
        self._minio_controller.start()

        # Initialize database controller:
        self._database_controller = DatabaseApi(
            config={
                "pg_host": cloud_config["PG_CLOUD_HOST"],
                "pg_port": int(cloud_config["PG_CLOUD_PORT"]),
                "pg_database": cloud_config["PG_CLOUD_DATABASE"],
                "pg_user": cloud_config["PG_CLOUD_USER"],
                "pg_password": cloud_config["PG_CLOUD_PASSWORD"],
            }
        )
        self._database_query_writer = QueryWriter(cloud_config["QUERY_FILE"])

    def start(self):
        """
        Start the cloud connector to handle image processing and uploading.
        """
        if not self._event_running.is_set():
            if self._dji_controller is not None:
                self._dji_controller.start()
            self._event_running.set()
            self._thread = Thread(target=self._worker, daemon=True)
            self._thread.start()
            self._logger.info("Cloud connector started.")

    def stop(self):
        """
        Stop the cloud connector and its worker thread.
        """
        if self._event_running.is_set():
            self._event_running.clear()
            self._thread.join()
            if self._dji_controller is not None:
                self._dji_controller.stop()
            self._logger.info("Cloud connector stopped.")

    def _get_dji_lat_lon(self) -> tuple:
        """Read the latest sampled DJI GPS location, if available."""
        if self._dji_controller is None:
            return None, None

        location = self._dji_controller.get_location()
        if location is None:
            return None, None

        return location.latitude, location.longitude

    def _get_lat_lon(self, exif_data: Optional[dict]) -> tuple:
        """
        Extract latitude and longitude from EXIF data if available.
        Args:
            exif_data (dict): EXIF data from the image.
        Returns:
            tuple: Latitude and longitude as floats, or None if not available.
        """
        if not exif_data:
            return None, None

        gps_info = {}
        for key, val in exif_data.items():
            tag = ExifTags.TAGS.get(key)
            if tag == "GPSInfo":
                for t in val:
                    sub_tag = ExifTags.GPSTAGS.get(t)
                    gps_info[sub_tag] = val[t]

        def convert_to_degrees(value):
            d, m, s = value
            return float(d) + float(m) / 60.0 + float(s) / 3600.0

        lat, lon = None, None
        if "GPSLatitude" in gps_info and "GPSLongitude" in gps_info:
            lat = convert_to_degrees(gps_info["GPSLatitude"])
            if gps_info.get("GPSLatitudeRef") == "S":
                lat = -lat
            lon = convert_to_degrees(gps_info["GPSLongitude"])
            if gps_info.get("GPSLongitudeRef") == "W":
                lon = -lon

        return lat, lon

    def write_image_id(self, image_id: int, path: str):
        """
        Write a single image_id to a file.
        The file will always contain exactly one line.
        """
        with open(path, "w") as f:
            f.write(str(image_id).strip() + "\n")

    def read_image_id(self, path: str) -> int:
        """
        Read the single image_id from a file.
        Returns:
            int: The stored image_id
            None: If the file is empty or invalid
        """
        try:
            with open(path, "r") as f:
                line = f.readline().strip()
                if not line:
                    return -1
                return int(line)
        except (FileNotFoundError, ValueError):
            return -1

    def _worker(self):
        """
        Worker function to handle image processing and uploading.
        This function runs in a separate thread and processes images from the queue.
        """
        while self._event_running.is_set():
            if self.image_queue is None:
                self._logger.debug("Image queue is not initialized.")
                time.sleep(1)
                continue

            # Wait for an image to be available in the queue
            try:
                image_description = self.image_queue.get(block=True, timeout=1)
            except queue.Empty:
                self._logger.info("No images in queue, waiting...")
                continue
            self._logger.info("Processing image: %s", image_description.image_path)

            # Process the image:
            self._logger.info("Handling file %s", image_description.image_path)
            s3_filename = (
                f"{self.project_id}/{self.flight_id}/image_{int(time.time())}.JPG"
            )
            self._logger.info("Writing to %s", s3_filename)

            img = Image.open(image_description.image_path)
            img_width, img_height = img.size
            if False:
                # Extract from EXIF
                exif_data = dict(img.getexif()) if hasattr(img, "getexif") else None
                img_lat, img_lon = self._get_lat_lon(exif_data)
            else:
                # extract from DJI service
                img_lat, img_lon = self._get_dji_lat_lon()

            if img_lat is None or img_lon is None:
                img_lat, img_lon = 0.0, 0.0

            picture_size = os.path.getsize(image_description.image_path)

            try:
                # Add the image reference to the database:
                image_id = self.read_image_id("./image.id")
                self._database_query_writer.post_image(
                    image_id,
                    self.flight_id,
                    {
                        "storage": s3_filename,
                        "gps": {
                            "latitude": img_lat,
                            "longitude": img_lon,
                        },
                        "size": {
                            "width": img_width,
                            "height": img_height,
                        },
                    },
                )
                self.write_image_id(image_id + 1, "./image.id")
                self._logger.info("Captured image with id %d", image_id)

                # Write to S3:
                self._minio_controller.upload_file(
                    bucket_name=self._cloud_config["MINIO_RAW_BUCKET"],
                    file_path=image_description.image_path,
                    object_name=s3_filename,
                    delete=self._delete_after_upload,
                    metadata={
                        "project_id": self.project_id,
                        "flight_id": self.flight_id,
                        "image_id": image_id,
                    },
                )

                # Log statistics to the timeseries database
                self._timeseries_db_controller.add_to_queue(
                    {
                        "fields": {
                            "picture_time": image_description.picture_duration,
                            "download_time": image_description.download_duration,
                            "picture_size": picture_size,
                            "minio_queue_size": len(self._minio_controller),
                            "timeseries_queue_size": len(self._timeseries_db_controller)
                            + 1,
                        },
                        "tags": {
                            "model": image_description.camera_description.name,
                            "port": image_description.camera_description.port,
                        },
                    }
                )
            except RuntimeError as ex:
                self._logger.error(
                    "Error while processing image %s: %s",
                    image_description.image_path,
                    str(ex),
                )


def main(
    bypass_camera: bool,
    test_image_dir: str,
    edge_config: dict,
    cloud_config: dict,
    flask_ip: str,
    flask_port: int,
    debug: bool = False,
    delete_after_upload: bool = True,
):
    """
    Main function to initialize the camera, connect to a timeseries database, and continuously capture and save images.
    It captures images from the camera, saves them to the local filesystem, and logs the time taken for capturing and downloading each image.

    Args:
        bypass_camera (bool): If True, bypasses the camera initialization and runs in test mode.
        test_image_dir (str): Directory containing test images for the TestCamera.
        edge_config (dict): Configuration for the edge environment.
        cloud_config (dict): Configuration for the cloud environment.
        flask_ip (str): IP address for the Flask app.
        flask_port (int): Port for the Flask app.
        debug (bool): If True, enables debug mode for the GPHOTO2 library.
        delete_after_upload (bool): If True, deletes images after uploading to S3.

    Raises:
        gp.GPhoto2Error: If an unhandled gphoto2 error occurs during camera initialization.
    """
    logger = logging.getLogger(__name__)

    # Create Flask REST API:
    app = Flask(__name__)

    # Keep track of flight information:
    flight_info = {
        "project_id": -1,
        "flight_id": -1,
    }

    # Load edge and cloud connectors:
    edge_connector = CloudConnector(
        "cloudconnector-edge", edge_config, delete_after_upload
    )
    edge_connector.start()
    cloud_connector = CloudConnector(
        "cloudconnector-cloud", cloud_config, delete_after_upload
    )
    cloud_connector.start()

    # Camera logic
    if bypass_camera:
        logger.info("Running in test mode, no camera interaction.")
        camera = TestCamera(test_image_dir)
    else:
        logger.info("Running with GPHOTO2 camera.")
        camera = Gphoto2Camera()

    @app.route("/start", methods=["POST"])
    def start_capture():
        """
        Start the camera capture operation.
        """
        if camera.is_running():
            return jsonify({"message": "Capture operation already running"}), 400

        # Read JSON payload
        payload = request.get_json()
        if "project_id" not in payload:
            return jsonify({"error": "Missing project_id in payload"}), 400
        if "flight_id" not in payload:
            return jsonify({"error": "Missing flight_id in payload"}), 400
        if "ads" not in payload:
            return jsonify({"error": "Missing ads in payload"}), 400

        # Store flight information:
        flight_info["project_id"] = payload["project_id"]
        flight_info["flight_id"] = payload["flight_id"]

        # Start the flight:
        camera.start()

        # Initiate the cloud connector with the flight information:
        if payload["ads"] == "edge":
            edge_connector.project_id = flight_info["project_id"]
            edge_connector.flight_id = flight_info["flight_id"]
            edge_connector.image_queue = camera.image_queue
        else:
            cloud_connector.project_id = flight_info["project_id"]
            cloud_connector.flight_id = flight_info["flight_id"]
            cloud_connector.image_queue = camera.image_queue

        return jsonify({"message": "Capture operation started"}), 200

    @app.route("/stop", methods=["POST"])
    def stop_capture():
        """
        Stop the camera capture operation.
        """
        print("Stop capture called")
        if not camera.is_running():
            return jsonify({"message": "Capture operation is not running"}), 400

        camera.stop()
        edge_connector.image_queue = None
        cloud_connector.image_queue = None
        print("Stop capture DONE")
        return jsonify({"message": "Capture operation stopped"}), 200

    @app.route("/status", methods=["GET"])
    def status():
        """
        Get the current status of the capture operation.
        """
        response = {
            "status": camera.is_running(),
            "message": camera.state,
            "project_id": flight_info["project_id"],
            "flight_id": flight_info["flight_id"],
            "captures": camera.number_of_captures,
            "camera": camera.description,
            "location": edge_connector._get_dji_lat_lon(),
        }

        return jsonify(response), 200

    @app.route("/video_feed")
    def video_feed():
        return Response(
            camera.gen_video_feed(),
            mimetype="multipart/x-mixed-replace; boundary=frame",
        )

    # Start the Flask app in a separate thread
    http_server = WSGIServer((flask_ip, flask_port), app)
    logger.info("Starting Flask app on %s:%d", flask_ip, flask_port)
    http_server.serve_forever()


if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Camera capture script")
    parser.add_argument(
        "--test",
        action=argparse.BooleanOptionalAction,
        help="Run the script in test mode (no actual camera interaction)",
    )
    parser.add_argument(
        "--images",
        type=str,
        help="Image directory for test mode",
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
        "--edge-env",
        type=str,
        default=".env.edge",
        help="Set the environment file to load (default: .env.edge)",
    )

    parser.add_argument(
        "--cloud-env",
        type=str,
        default=".env.cloud",
        help="Set the environment file to load (default: .env.cloud)",
    )

    parser.add_argument(
        "--app-ip",
        type=str,
        default="192.168.50.84",
        help="IP address for the Flask app (default: 192.168.50.84)",
    )

    parser.add_argument(
        "--app-port",
        type=int,
        default="5000",
        help="Port for the Flask app (default: 5000)",
    )

    args = parser.parse_args()

    # Set up logging
    logging.basicConfig(
        format="[%(levelname)s %(name)s @ %(asctime)s] %(message)s",
        level=getattr(logging, args.log_level, logging.DEBUG),
    )

    # Run the main function
    sys.exit(
        main(
            args.test,
            args.images,
            dotenv_values(args.edge_env),
            dotenv_values(args.cloud_env),
            args.app_ip,
            args.app_port,
            args.debug,
            delete_after_upload=not args.test,
        )
    )
