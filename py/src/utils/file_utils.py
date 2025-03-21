from typing import List, Union, Container
import os
import shutil
from datetime import datetime

def get_files(src: str, ext: (str or Container) = None, full_path: bool = True, 
              add_folders: bool = False) -> List[str]:
    """
    Fetch the contents of `src_folder` and return the subset of files that meets 
    the specified requirements as a list
    """
    if not os.path.isdir(src):
        raise FileNotFoundError(f'The specified src_folder {src} is not an existing directory')
    if isinstance(ext, str):
        ext = [ext]
    
    def include(el: str) -> bool:
        return (ext is None or ext is not None and os.path.splitext(el)[1][1:] in ext) or \
               add_folders and os.path.isdir(el)
        
    return list([(src + f if full_path else f) for f in os.listdir(src) if include(src + f)]) 

def get_file_info(path: str) -> dict:
    """Get comprehensive file information"""
    return {
        'size': os.path.getsize(path),
        'created': datetime.fromtimestamp(os.path.getctime(path)),
        'modified': datetime.fromtimestamp(os.path.getmtime(path)),
        'is_file': os.path.isfile(path),
        'extension': os.path.splitext(path)[1][1:] if os.path.splitext(path)[1] else None
    }

def ensure_directory(path: str) -> None:
    """Ensure directory exists, create if it doesn't"""
    if not os.path.exists(path):
        os.makedirs(path)

def safe_file_operation(func):
    """Decorator for safe file operations"""
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (IOError, OSError) as e:
            print(f"Error during file operation: {e}")
            return None
    return wrapper 