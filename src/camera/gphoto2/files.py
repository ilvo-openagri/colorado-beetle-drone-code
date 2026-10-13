
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