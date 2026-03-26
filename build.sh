#!/bin/bash
# Render 构建脚本 - 初始化数据库
python3 -c "from knowledge_graph_api import init_db; init_db()"
