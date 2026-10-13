from ..interface import _Camera, CameraImage, CameraDescription
import re
import subprocess
import time


class Gphoto2Camera(_Camera):

    def __init__(self, focus_shot: bool = False):
        super().__init__()
        self._focus_shot = focus_shot
        self._logger.info("Gphoto2Camera initialized.")
        # One shot camera detection
        reset_output = self.reset()
        self._logger.info("First performing reset: %s", reset_output)
        camera = self._camera_detect_iteration()
        self.description = camera.name if camera is not None else "NO_DETECTED"

    def reset(self):
        return subprocess.run(["gphoto2", "--reset"], capture_output=True)

    def _camera_detect_iteration(self) -> CameraDescription | None:
        gphoto2_detect = (
            subprocess.run(["gphoto2", "--auto-detect"], capture_output=True)
            .stdout.decode()
            .splitlines()
        )
        if len(gphoto2_detect) < 3:
            self._logger.error("No camera detected")
            time.sleep(2)
            return None
        model_offset = re.search(" *usb", gphoto2_detect[2]).start()
        port_offset = re.search("usb", gphoto2_detect[2]).start()
        camera = CameraDescription(
            name=gphoto2_detect[2][0:model_offset].strip(),
            port=gphoto2_detect[2][port_offset:].strip(),
        )
        return camera

    def _loop(self):
        """
        Main loop for Gphoto2 camera operations.
        """
        # Check if the camera is connected:
        self._logger.info("Capturing images using gphoto2 CLI")
        self.state = "Detecting camera"
        camera = None
        while camera is None and self._event_running.is_set():
            camera = self._camera_detect_iteration()

        if not camera:
            self._logger.error("Loop stopped before camera was detected")
            return

        self._logger.info("Camera detected: %s", camera.name)
        self.description = camera.name

        # Take a focus shot:
        if self._focus_shot:
            self.state = "Focusing camera"
            self._logger.info("Focusing camera")
            subprocess.run(
                [
                    "gphoto2",
                    "--set-config",
                    "/main/capturesettings/focusmode=1",
                    "--trigger-capture",
                ]
            )

        # Define variables:
        re_saving_file = re.compile(r"Saving file as.*")
        re_delete_file = re.compile(r"Deleting file /(.*.jpg) on the camera")
        re_capture = re.compile(r"Capturing frame.*")
        ts_capture = 0
        ts_saving = 0

        # Define the capture process:
        capture_process = subprocess.Popen(
            [
                "gphoto2",
                "--set-config",
                "/main/capturesettings/focusmode=2",
                "--capture-image-and-download",
                "--interval=2",
                "--force-overwrite",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,  # Optional: merge stderr with stdout
        )

        while self._event_running.is_set():
            output = capture_process.stdout.readline()
            if output == b"" and capture_process.poll() is not None:
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
                self.number_of_captures += 1
                self._update_stream_image(picture_file)
                self.image_queue.put(
                    CameraImage(
                        image_path=picture_file,
                        capture_time=ts_capture,
                        picture_duration=ts_saving - ts_capture,
                        download_duration=ts_deleting - ts_saving,
                        camera_description=camera,
                    )
                )

            # Allow some delay to not lock the thread:
            time.sleep(0.1)

        capture_process.terminate()
        self._logger.info("Capture process terminated.")
