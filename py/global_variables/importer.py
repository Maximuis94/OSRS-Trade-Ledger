"""
Centralized importer for global_variables modules, designed to help enforce consistency wrt import aliases.
Aside from local modules, this module also contains some external module import that the majority of the modules rely on
Use "from global_variables.import import *" to import all global_variables modules with.
"""
import time

from venv_auto_loader.active_venv import *
import global_variables.path as gp
import global_variables.local_file as gl
import global_variables.variables as gd
import global_variables.gui as gg
import global_variables.osrs as go
import global_variables.configurations as gc
import global_variables.values as gv
import global_variables.variables as var

import util.str_formats as fmt
import util.unix_time as ut
import util.osrs as uo
import util.file as uf
import util.data_structures as ud
__t0__ = time.perf_counter()

# print(f'Setup Global Values in {1000*(t1 - gp.setup_start):.1f}ms')


# class ModuleManager(metaclass=model.local_file.SingletonMeta):
#     """
#     Singleton class with global values imports. Modules are set to singleton class attributes, i.e.
#
#     """
#
#     def __init__(self):
#         self.p = gp
#         self.l = gl
#         self.d = gd
#         self.g = gg
#         self.o = go
#         self.c = gc
#         self.fmt = fmt
#         self.ut = ut

# g = ModuleManager()


def import_stuff():
    ...
    
    
print(f'global_variables importer setup time: {1000*(time.perf_counter()-gp.setup_start):.1f}ms\n\n')

if __name__ == '__main__':
    ...
