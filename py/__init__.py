"""
Initializer that ensures code is executed via the virtual environment
"""
import os
import sys

_venv_folder = ".venv"
_venv_exe = os.path.join(_venv_folder, "Scripts", "python.exe")

_cur_dir = os.getcwd()
_venv = os.path.join(_cur_dir, _venv_folder)
while len(_cur_dir) > 3 and not os.path.exists(_venv):
    _cur_dir = os.path.split(_cur_dir)[0]
    _venv = os.path.join(_cur_dir, _venv_folder)

if not os.path.exists(_venv):
    raise RuntimeError(f"Virtual environment not found! Setup a virtual environment first named after the configured "
                       f"venv folder (or alternatively, change the name of the configured venv folder)")
    exit(-1)

if not sys.executable.endswith(_venv_exe) and os.path.exists(_venv_exe):
    os.system(f"""{_venv_exe} {sys.argv[0]}""")
    exit(1)

