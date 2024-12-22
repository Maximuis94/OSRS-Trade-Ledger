"""
This is an executable module used to run setup methods, along with hard-coded inputs to generate files that do not
exist.

Setup files are configured in other modules within the setup package.
Executing this package should only result in generating listed files that do not exist.
"""
import os
import sys

wd = os.path.commonpath([sys.prefix, os.getcwd()])
print(f"Executing setup with working directory {wd}")

def execute_setup(packages: bool = True):
    # Packages
    if packages:
        req_file = os.path.join(wd, "setup", "requirements.txt")
        print(f"Attempting to install packages from requirements file located at {req_file}")
        if input("Do you wish to proceed? [y/n] ").lower() == 'y':
            os.system(f"""{sys.executable} -m pip install -r {req_file} """)
        else:
            print('Skipped package install')
    
    # Timeseries db
    
    
    # Local db
    
    
    # Test db
    
    
    # Npy db




if __name__ == '__main__':
    import global_variables.path as gp
    print(gp.f_db_local)
    import sqlite3
    con = sqlite3.connect(gp.f_db_local)
    
    print(sys.executable)
    exit(1)
    execute_setup(packages=True)



