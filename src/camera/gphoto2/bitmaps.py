"""
This module defines classes to handle operations for a camera based on a bitmap of allowed operations.
"""

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