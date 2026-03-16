@echo off
cd /d %~dp0
if not exist logs mkdir logs
python main.py >> logs\fetch-news.log 2>&1
