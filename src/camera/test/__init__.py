from ..interface import _Camera, CameraImage, CameraDescription
import time
import os
import glob


class TestCamera(_Camera):

    def __init__(self, test_image_dir: str, focus_shot: bool = False):
        super().__init__()
        self.description = CameraDescription(name="Test Camera", port="test_port")
        self._focus_shot = focus_shot

        self._image_files = []
        if test_image_dir:
            self._image_files = sorted(
                glob.glob(os.path.join(test_image_dir, "*.jpg"))
                + glob.glob(os.path.join(test_image_dir, "*.jpeg"))
                + glob.glob(os.path.join(test_image_dir, "*.png"))
                + glob.glob(os.path.join(test_image_dir, "*.JPG"))
                + glob.glob(os.path.join(test_image_dir, "*.JPEG"))
                + glob.glob(os.path.join(test_image_dir, "*.PNG"))
            )
            if not self._image_files:
                self._logger.warning("No image files found in %s", test_image_dir)

        self._logger.info("Initializing TestCamera with images from %s", test_image_dir)
        for image_file in self._image_files:
            self._logger.debug("- %s", image_file)
        self._logger.info("TestCamera initialized.")

    def _loop(self):
        """
        Main loop for Test camera operations.
        """
        # Check if the camera is connected:
        self._logger.info("Capturing images using Test CLI")

        for image_file in self._image_files:
            start_time = time.time()
            if not self._event_running.is_set():
                self._logger.info("Stopping TestCamera loop.")
                return
            self.number_of_captures += 1
            # self._update_stream_image(image_file)
            self.image_queue.put(
                CameraImage(
                    image_path=image_file,
                    capture_time=int(time.time()),
                    picture_duration=0.1,
                    download_duration=0.1,
                    camera_description=self.description,
                )
            )
            self._logger.info(
                "Captured image %s, total captures: %d",
                image_file,
                self.number_of_captures,
            )

            # Allow some delay to not lock the thread:
            current_time = time.time()
            if current_time - start_time >= 1.5:
                continue
            time.sleep(1.5 - (current_time - start_time))
        self.stop("TestCamera loop completed, all images processed.")
