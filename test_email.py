#!/usr/bin/env python3
"""独立测试：发送一封简单邮件验证 SMTP 配置"""
import smtplib
import ssl
import yaml
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

with open("config/config.yaml", "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

cfg = config["notifier"]
host     = cfg["smtp_host"]
port     = int(cfg["smtp_port"])
user     = cfg["smtp_user"]
password = cfg["smtp_password"]
from_addr = cfg["from_address"]
to_addrs  = cfg["to_addresses"]

subject = f"[测试] SMTP 连通性测试 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
body    = "这是一封测试邮件，用于验证 QQ SMTP SSL/465 配置是否正常。"

msg = MIMEMultipart("alternative")
msg["Subject"] = subject
msg["From"]    = from_addr
msg["To"]      = ", ".join(to_addrs)
msg.attach(MIMEText(body, "plain", "utf-8"))

context = ssl.create_default_context()
print(f"连接 {host}:{port} ...")
with smtplib.SMTP_SSL(host, port, context=context, timeout=30) as server:
    server.login(user, password)
    server.sendmail(from_addr, to_addrs, msg.as_string())
print(f"✓ 邮件已发送至 {to_addrs}")
