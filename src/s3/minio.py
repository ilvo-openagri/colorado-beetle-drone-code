from time import time
import boto3
import logging
import os
import urllib.parse
import time
from dataclasses import dataclass
from multiprocessing import Process, Queue


@dataclass
class MinioUploadData:
    """
    MinioUploadData is a class that represents the data required to upload a file to a MinIO bucket.
    Attributes:
        source (str): The file path or source of the data to be uploaded.
        name (str): The name of the file to be stored in the bucket.
        bucket (str): The name of the target bucket where the file will be uploaded.
        metadata (dict): Metadata belonging to the file
        delete (bool): Indicates whether to delete the file after upload.
    """

    source: str
    name: str
    bucket: str
    metadata: dict
    delete: bool


class MinioController:
    """
    MinioController is a class that manages the uploading of files to a MinIO server
    using a separate process to handle the uploads asynchronously.

    Note:
        The `start` method must be called before using the `upload_file` method
        to ensure the upload process is running.
    """

    def __init__(self, minio_url: str, access_key: str, secret_key: str):
        self._minio_url = minio_url
        self._access_key = access_key
        self._secret_key = secret_key
        self._minio_queue = Queue()
        self._process = None

    @staticmethod
    def _minio_writer(
        data_queue: Queue, minio_url: str, access_key: str, secret_key: str
    ):
        """
        Handles uploading data to a MinIO server using a queue-based approach.
        This function continuously retrieves data from a queue and uploads it to a
        specified MinIO bucket. It stops processing when a sentinel value (None)
        is encountered or when a keyboard interrupt is received.
        Args:
            data_queue (Queue): A thread-safe queue containing data to be uploaded.
                Each item in the queue should be an instance of `MinioUploadData`.
            minio_url (str): The URL of the MinIO server.
            access_key (str): The access key for authenticating with the MinIO server.
            secret_key (str): The secret key for authenticating with the MinIO server.
        Raises:
            KeyboardInterrupt: Stops the upload process when a keyboard interrupt is received.
        Notes:
            - The function logs errors for invalid data types in the queue.
            - The function uses a sentinel value (None) to signal the end of the queue.
            - The `boto3` client is used for interacting with the MinIO server.
        """

        logger = logging.getLogger("_minio_writer")
        s3_client = boto3.client(
            "s3",
            endpoint_url=minio_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            use_ssl=False,
            verify=False,
        )

        stop_writing = False
        try:
            while not stop_writing or not data_queue.empty():
                try:
                    sample = data_queue.get()
                    if sample is None:
                        break  # Sentinel to exit

                    if not isinstance(sample, MinioUploadData):
                        logger.error(f"Invalid data type: {type(sample)}")
                        continue

                    logger.info(
                        "Uploading %s to bucket %s [%s] @ %s",
                        sample.source,
                        sample.bucket,
                        minio_url,
                        sample.name,
                    )
                    start_time = time.time()
                    with open(sample.source, "rb") as file_obj:
                        # Ensure metadata values are ASCII-encoded
                        ascii_metadata = {
                            key: str(value).encode("ascii", "ignore").decode("ascii")
                            for key, value in sample.metadata.items()
                        }
                        s3_client.upload_fileobj(
                            Fileobj=file_obj,
                            Bucket=sample.bucket,
                            Key=sample.name,
                            ExtraArgs={
                                "Tagging": urllib.parse.urlencode(ascii_metadata)
                            },
                        )

                    if sample.delete:
                        try:
                            os.remove(sample.source)
                            logger.debug("Deleted source file: %s", sample.source)
                        except OSError as e:
                            logger.error("Error deleting file %s: %s", sample.source, e)

                    logger.info(
                        "Uploaded %s to bucket %s in %f seconds",
                        sample.source,
                        sample.bucket,
                        time.time() - start_time,
                    )
                except KeyboardInterrupt:
                    stop_writing = True
                    logger.info("Keyboard interrupt received, stopping data logger...")
        finally:
            s3_client.close()
            logger.info("Socket closed.")
            logger.info("Data logger stopped.")

    def start(self):
        """
        Starts the MinIO writer process.
        This method initializes and starts a separate process to handle
        writing data to MinIO.

        Raises:
            RuntimeError: If the data logger process is already running.
        """

        if self._process is not None:
            raise RuntimeError("Data logger process is already running.")

        self._process = Process(
            target=self._minio_writer,
            args=(
                self._minio_queue,
                self._minio_url,
                self._access_key,
                self._secret_key,
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
        self._minio_queue.put(None)  # Send sentinel value to stop the process
        self._process.join()  # Wait for the process to finish
        self._process = None

    def upload_file(
        self,
        file_path: str,
        bucket_name: str,
        object_name: str,
        metadata: dict,
        delete: bool = False,
    ):
        """
        Uploads a file to the MinIO server.

        Args:
            file_path (str): The path to the file to be uploaded.
            bucket_name (str): The name of the target bucket.
            object_name (str): The name of the object in the bucket.
            metadata (dict): Dictionary containing metadata
            delete (bool): Whether to delete the file after upload.

        Raises:
            RuntimeError: If the data logger process is not running.

        Notes:
            - The method puts the upload request into a queue for processing.
            - The file will be uploaded asynchronously in the background.
            - The `delete` parameter indicates whether to delete the file after upload.
        """
        if self._process is None:
            raise RuntimeError("Data logger process is not running.")
        self._minio_queue.put(
            MinioUploadData(
                source=file_path,
                name=object_name,
                bucket=bucket_name,
                metadata=metadata,
                delete=delete,
            )
        )

    def __len__(self) -> int:
        """
        Returns the number of items in the MinIO queue.

        Returns:
            int: The number of items in the MinIO queue.
        """
        return self._minio_queue.qsize()


def main():
    import os

    # Example usage
    minio = MinioController(
        os.getenv("MINIO_URL", "http://localhost:9000"),
        os.getenv("MINIO_ACCESS_KEY", "default_access_key"),
        os.getenv("MINIO_SECRET_KEY", "default_secret_key"),
    )
    minio.start()
    minio.upload_file(
        "/home/jbauwens/Code/openagri_drone/test_image.JPG",
        "openagri-raw",
        "test_image.JPG",
    )
    minio.stop()


if __name__ == "__main__":
    main()
