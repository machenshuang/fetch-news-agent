#!/usr/bin/env python3
"""发送最新 analysis 报告到邮箱，验证 Markdown→HTML 渲染效果"""
import glob
import os
import yaml
from datetime import datetime
from notifier.email_notifier import EmailNotifier

with open("config/config.yaml", "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

# 找最新的 analysis 报告
reports = sorted(glob.glob("reports/*-analysis.md"))
if not reports:
    print("❌ 没有找到任何 analysis 报告")
    exit(1)

report_path = reports[-1]
print(f"发送报告: {report_path}")

# 从文件名解析日期
date_str = os.path.basename(report_path).split("-analysis")[0]  # e.g. "2026-03-16"
date = datetime.strptime(date_str, "%Y-%m-%d")

notifier = EmailNotifier(config)
notifier.notify([report_path], date)
print(f"✓ 已发送至 {notifier.to_addresses}")
