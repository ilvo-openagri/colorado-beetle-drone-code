import logging
import socket
import time
from multiprocessing import Process, Queue


class TimeseriesDbConnector:
    """
    TimeseriesDbConnector is a class designed to facilitate the logging of time-series data
    to a Telegraf instance using the InfluxDB line protocol. It manages a queue of data
    samples and spawns a separate process to handle the logging.
    """

    def __init__(
        self,
        measurement: str,
        tags: dict,
        telegraf_ip: str = "",
        telegraf_port: int = 8094,
    ):
        self._timeseries_queue = Queue()
        self._telegraf_ip = telegraf_ip
        self._telegraf_port = telegraf_port
        self._measurement = measurement
        self._telegraf_tags = []
        self._process = None
        self._telegraf_tags = [
            (
                f"{tag}={value.replace(' ','-')}"
                if isinstance(value, str)
                else f"{tag}={value}"
            )
            for tag, value in tags.items()
        ]

    def start(self):
        """
        Starts the data logger process.

        This method initializes and starts a new process for logging data.
        If the process is already running, a RuntimeError is raised.

        Raises:
            RuntimeError: If the data logger process is already running.
        """
        if self._process is not None:
            raise RuntimeError("Data logger process is already running.")

        self._process = Process(
            target=self._data_logger,
            args=(
                self._measurement,
                ",".join(self._telegraf_tags),
                self._timeseries_queue,
                self._telegraf_ip,
                self._telegraf_port,
            ),
        )
        self._process.start()

    def stop(self):
        """
        Stops the data logger process.

        This method terminates the data logger process and cleans up the queue.
        If the process is not running, a RuntimeError is raised.

        Raises:
            RuntimeError: If the data logger process is not running.
        """
        if self._process is None:
            raise RuntimeError("Data logger process is not running.")
        self._process.terminate()
        self._process.join()
        self._process = None

    def add_to_queue(self, data: dict):
        """
        Adds a data sample to the queue.

        This method enqueues a dictionary representing a data sample to be logged.
        If the process is not running, a RuntimeError is raised.

        Args:
            data (dict): A dictionary containing key-value pairs representing the fields of the measurement.

        Raises:
            RuntimeError: If the data logger process is not running.
        """
        if self._process is None:
            raise RuntimeError("Data logger process is not running.")
        self._timeseries_queue.put(data)
        
    def __len__(self) -> int:
        """
        Returns the number of items in the queue.

        This method returns the current size of the queue.

        Returns:
            int: The number of items in the queue.
        """
        return self._timeseries_queue.qsize()

    @staticmethod
    def _data_logger(
        measurement: str,
        tag_set: str,
        queue: Queue,
        telegraf_ip: str = "",
        telegraf_port: int = 8094,
    ):
        """
        Logs data samples from a queue to a Telegraf instance using UDP.

        This function connects to a Telegraf instance via a UDP socket and sends
        formatted data samples retrieved from the provided queue. Each sample is
        expected to be a dictionary containing key-value pairs representing the
        fields of the measurement. The function runs in a loop until a sentinel
        value (None) is encountered in the queue or a KeyboardInterrupt is received.

        Args:
            measurement (str): The name of the measurement to log.
            tag_set (str): A string representing the tags to associate with the measurement.
            queue (Queue): A queue containing data samples to be logged. Each sample
                should be a dictionary of key-value pairs.
            telegraf_ip (str, optional): The IP address of the Telegraf instance. Defaults to an empty string.
            telegraf_port (int, optional): The port number of the Telegraf instance. Defaults to 8094.

        Raises:
            KeyboardInterrupt: Stops the logging process when a keyboard interrupt is received.

        Notes:
            - The function formats the data in the InfluxDB line protocol format.
            - The sentinel value `None` in the queue signals the function to stop logging.
            - The function sets a 1-second timeout on the UDP socket.
            - The socket will always be closed, even in case of an error, ensuring proper resource cleanup.
        """
        logger = logging.getLogger("_data_logger")
        logger.info(f"Connecting to Telegraf at {telegraf_ip}:{telegraf_port}")
        sock = socket.create_connection((telegraf_ip, telegraf_port), timeout=5)
        logger.info(f"Connected")
        stop_logging = False
        while not stop_logging or not queue.empty():
            try:
                sample = queue.get()
                if sample is None:
                    break  # Sentinel to exit
                field_set = ",".join(
                    [
                        (
                            f"{key}={value.replace(' ','-')}"
                            if isinstance(value, str)
                            else f"{key}={value}"
                        )
                        for key, value in sample.items()
                    ]
                )
                data = f"{measurement},{tag_set} {field_set} {int(time.time_ns())}\n"
                sock.send(data.encode("utf-8"))
                logger.info(f"Sent data: {data}")
            except KeyboardInterrupt:
                stop_logging = True
                logger.info("Keyboard interrupt received, stopping data logger...")
        sock.close()
        logger.info("Socket closed.")
        logger.info("Data logger stopped.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Example usage of TimeseriesDbConnector
    tags = {"host": "drone1", "location": "fieldA"}
    connector = TimeseriesDbConnector(
        measurement="sensor_data",
        tags=tags,
        telegraf_ip="127.0.0.1",
        telegraf_port=8094,
    )

    try:
        connector.start()
        logging.info("TimeseriesDbConnector started.")

        # Simulate adding data to the queue
        for i in range(10):
            sample = {"temperature": 20 + i, "humidity": 50 - i}
            connector._timeseries_queue.put(sample)
            time.sleep(0.5)

        # Signal the logger to stop
        connector._timeseries_queue.put(None)
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        connector.stop()
        logging.info("TimeseriesDbConnector stopped.")
