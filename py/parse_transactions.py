"""
Executable module for parsing transactions. Executes on idle and is restricted to parsing logs of previous days.
Once set, do not modify this file
"""
from tasks.parse_transactions import parse_logs

if __name__ == '__main__':
    
    parse_logs()
