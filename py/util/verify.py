"""
Module with various verification methods. All methods return either True or False, indicating whether the operation can
proceed or not.

Module should be imported as 'import util.verify as verify'

"""
import os
import shutil


def disk_space(path: str, space_needed: int) -> bool:
    """ Return True if `space_needed` does not exceed free disk space on the disk of `path` """
    return space_needed < shutil.disk_usage(os.path.split(path)[0]).free


def extension(path: str, extension: str):
    return os.path.splitext(path)[1][1:] == extension
    