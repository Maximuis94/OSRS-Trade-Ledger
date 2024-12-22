from venv_auto_loader.active_venv import *
from runelite_plugin_scripts.bank_memory import export_to_csv


if __name__ == "__main__":
    try:
        print('\n'*50)
        output_file = export_to_csv()
    except RuntimeError as e:
        print \
            ("Unable to export the clipboard contents to a CSV file, as the contents are not identified as a bank memory export."
              "Copy a bank memory to the clipboard via Runelite, and rerun the script...")
    else:
        print("Successfully exported bank memory contents to", output_file)
    
    input('Press ENTER to close this screen')
    # time.sleep(15)
    exit(1)
