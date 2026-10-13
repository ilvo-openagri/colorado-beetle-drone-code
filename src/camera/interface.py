from threading import Event, Thread, current_thread
import cv2
import logging
import numpy as np
import queue


class CameraDescription:
    def __init__(self, name: str, port: str):
        self.name = name
        self.port = port


class CameraImage:
    """
    Represents an image captured by the camera.
    """

    def __init__(
        self,
        image_path: str,
        capture_time: float,
        picture_duration: float,
        download_duration: float,
        camera_description: CameraDescription,
    ):
        self.image_path = image_path
        self.capture_time = capture_time
        self.picture_duration = picture_duration
        self.download_duration = download_duration
        self.camera_description = camera_description


class _Camera:

    def __init__(self):
        self.image_queue = queue.Queue()
        self.state = "idle"
        self.number_of_captures = 0
        self.description = None
        self._event_running = Event()
        self._logger = logging.getLogger(__name__)

    def start(self):
        """
        Start the camera operations.
        """
        if not self._event_running.is_set():
            self._event_running.set()
            self._thread = Thread(target=self._loop, daemon=True)
            self._thread.start()
            self.state = "running"
            self._logger.info("Camera started.")

    def stop(self, message: str = "Camera stopped."):
        """
        Stop the camera operations.
        """
        if self._event_running.is_set():
            self.state = "stopped"
            self._event_running.clear()
            if current_thread() is not self._thread:
                self._thread.join()
            self._logger.info(message)

    def is_running(self) -> bool:
        """
        Check if the camera is currently running.

        Returns:
            bool: True if the camera is running, False otherwise.
        """
        return self._event_running.is_set()

    def gen_video_feed(self):
        while True:
            if self._live_stream_image is not None:
                try:
                    ret, buffer = cv2.imencode(".jpg", self._live_stream_image)
                    yield (
                        b"--frame\r\n"
                        b"Content-Type: image/jpeg\r\n\r\n" + buffer.tobytes() + b"\r\n"
                    )
                except AttributeError as e:
                    time.sleep(1)  # Adjust for frame rate
                    continue
            time.sleep(1)  # Adjust for frame rate

    def _loop(self):
        """
        Main loop for camera operations.
        This method should be overridden by subclasses.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    def _update_stream_image(self, image_path: str):
        """
        Update the live stream image from the given image path.
        Args:
            image_path (str): Path to the image file.
        """
        # Data for live stream:
        with open(image_path, "rb") as file:
            file_content = file.read()
        self._live_stream_image = np.asarray(bytearray(file_content), dtype=np.uint8)
        self._live_stream_image = cv2.imdecode(
            self._live_stream_image, cv2.IMREAD_COLOR
        )
        self._live_stream_image = cv2.resize(
            self._live_stream_image, (100, 100), interpolation=cv2.INTER_LINEAR
        )
