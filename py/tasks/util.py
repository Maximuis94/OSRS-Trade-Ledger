"""
Module with various task-related helper functions


"""
import time


def finish_execution(exit_code: int = 1, wait_time: int = 30):
    """
    Final call before shutting down execution, primarily designed for running on Windows as to prevent shutting down the
    console window. Display in how many seconds the console window will close, or show that the shutdown timer is
    overridden.
    
    Parameters
    ----------
    exit_code : int, optional, 1 by default
        Integer to pass on to exit()
    wait_time : int, optional, 30 by default
        Amount of seconds before terminating execution
    """
    print("\n")
    try:
        remaining_wait_time = wait_time
        max_time = time.time() + remaining_wait_time
        while time.time() < max_time:
            print(f" Done! This screen will close in {max_time-time.time():.0f} seconds. "
                  f"Press ctrl+c to keep the screen open indefinitely  ", end='\r')
            time.sleep(min(1, remaining_wait_time))
            
        print("")
    except KeyboardInterrupt:
        _ = input(" Screen is locked, preventing it from closing automatically. Press ENTER to close.               \n")
    exit(exit_code)
    