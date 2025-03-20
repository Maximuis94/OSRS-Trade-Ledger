import sys
import os
from venv_auto_loader import active_venv

# parent = os.path.split(sys.path[0])[0]
# if parent != sys.path[-1]:
#     sys.path.append(parent)

# while str(os.getcwd())[-2:] != 'py':
#     os.chdir(os.path.split(os.getcwd())[0])
# print('cwd', os.getcwd())
if not os.path.exists('.venv'):
    active_venv.setup_venv(root=os.path.split(__file__)[0], requirements_txt='setup/requirements.txt')
    
