#!/bin/bash
cd "$(dirname "$0")"

# 激活虚拟环境
source ./venv/bin/activate

# 运行 main.py
python3 ./csv_merger.py

# 停用虚拟环境
deactivate