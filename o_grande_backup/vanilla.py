## Watchdog system to check if something is not working as expected
import psutil
import time
import telegram_tools.telegram_tools as telegram_tools
import os
import socket


EXPECTED_COMMANDS = os.environ.get("EXPECTED_COMMANDS").split(",")


def check_if_process_is_running():
    commands = []
    for proc in psutil.process_iter():
        if "python" in proc.name():
            try:
                command = proc.cmdline()[1]
                commands.append(command)
            except IndexError:
                pass
    return commands


def check_if_is_same(commands):
    """
    Checks if the amount of commands is the same as the amount of expected commands
    """
    is_same = set(EXPECTED_COMMANDS) <= set(commands)
    return is_same


def check_fault_command(commands):
    fault_command = set(EXPECTED_COMMANDS).difference(commands)
    return fault_command


while True:
    commands = check_if_process_is_running()
    is_subset = check_if_is_same(commands)
    if is_subset == False:
        fault_command = check_fault_command(commands)
        telegram_tools.send_message(
            f"The process {fault_command} is not running in the machine {socket.gethostname()}"
        )
        print(f"The process {fault_command} is not running")
    else:
        print("Everything is running")
    time.sleep(360)
