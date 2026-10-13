from dataclasses import dataclass
from threading import Event, Lock, Thread
from typing import Optional
import logging
import time

import requests


@dataclass
class GpsLocation:
    """Simple value type containing the latest known GPS coordinates."""

    latitude: float
    longitude: float
    timestamp: float


class DjiController:
    """Polls a DJI endpoint and stores the latest GPS location in memory."""

    def __init__(
        self, gps_url: str, sample_interval_s: float = 0.25, timeout_s: float = 2.0
    ):
        if sample_interval_s <= 0:
            raise ValueError("sample_interval_s must be greater than 0")
        if timeout_s <= 0:
            raise ValueError("timeout_s must be greater than 0")

        self._gps_url = gps_url
        self._sample_interval_s = sample_interval_s
        self._timeout_s = timeout_s
        self._logger = logging.getLogger(self.__class__.__name__)

        self._event_running = Event()
        self._thread: Optional[Thread] = None
        self._lock = Lock()
        self._latest_location: Optional[GpsLocation] = None

    @staticmethod
    def _parse_location(payload: dict) -> Optional[tuple[float, float]]:
        """Extract latitude/longitude from common GPS response shapes."""
        if not isinstance(payload, dict):
            return None

        gps = payload.get("gps")
        if isinstance(gps, dict) and "lat" in gps and "lon" in gps:
            return float(gps["lat"]), float(gps["lon"])

        return (None, None)

    def start(self):
        """Start polling the configured GPS URL."""
        if self._event_running.is_set():
            return

        self._event_running.set()
        self._thread = Thread(target=self._worker, daemon=True)
        self._thread.start()

    def stop(self):
        """Stop polling and wait for the worker thread to finish."""
        if not self._event_running.is_set():
            return

        self._event_running.clear()
        if self._thread is not None:
            self._thread.join(timeout=self._timeout_s + self._sample_interval_s)
            self._thread = None

    def get_location(self) -> Optional[GpsLocation]:
        """Return the latest sampled location, if available."""
        with self._lock:
            return self._latest_location

    def _worker(self):
        while self._event_running.is_set():
            loop_start = time.monotonic()
            try:
                response = requests.get(self._gps_url, timeout=self._timeout_s)
                response.raise_for_status()
                coordinates = self._parse_location(response.json())
                if coordinates is None:
                    self._logger.warning(
                        "No GPS location found in response payload from %s",
                        self._gps_url,
                    )
                else:
                    latitude, longitude = coordinates
                    with self._lock:
                        self._latest_location = GpsLocation(
                            latitude=latitude / 10000000,
                            longitude=longitude / 10000000,
                            timestamp=time.time(),
                        )
            except (requests.RequestException, ValueError, TypeError) as ex:
                self._logger.warning("Failed to retrieve GPS location: %s", ex)

            elapsed = time.monotonic() - loop_start
            sleep_s = self._sample_interval_s - elapsed
            if sleep_s > 0:
                time.sleep(sleep_s)
