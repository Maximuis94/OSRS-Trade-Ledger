from src.core.file_handling.file import File

class LocalFile(File):
    """
    Abstract class for local files.
    """
    def __init__(self, path: str, **kwargs):
        super().__init__(path, **kwargs)
        # ... rest of existing implementation ... 