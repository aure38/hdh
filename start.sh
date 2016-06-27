#!/usr/bin/env bash
gnome-terminal --tab --working-directory=./ -e "bash -c 'export PYTHONPATH=./:$PYTHONPATH ; python3.5 ./daem/monitor_rdb.py ; echo ; echo ; read -p PressEnterToClose'" --tab --working-directory=./ -e "bash -c 'echo Starting ; sleep 5 ; export PYTHONPATH=./:$PYTHONPATH ; python3.5 ./fetch/homwiz.py ; echo ; echo ; read -p PressEnterToClose'"
