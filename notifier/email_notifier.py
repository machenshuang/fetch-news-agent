import logging
import os
import smtplib
import ssl
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List

logger = logging.getLogger(__name__)


class EmailNotifier:
    def __init__(self, config: dict):
        cfg = config.get("notifier", {})
        self.smtp_host = cfg.get("smtp_host", "smtp.gmail.com")
        self.smtp_port = int(cfg.get("smtp_port", 587))
        self.smtp_user = cfg.get("smtp_user", "")
        self.smtp_password = cfg.get("smtp_password", "")
        self.from_address = cfg.get("from_address", self.smtp_user)
        self.to_addresses: List[str] = cfg.get("to_addresses", [])
        self.subject_template = cfg.get("subject_template", "每日财经分析报告 - {date}")

    def notify(self, report_paths: List[str], date: datetime,
               window_name: str | None = None,
               time_start: datetime | None = None,
               time_end: datetime | None = None) -> None:
        if not self.to_addresses:
            logger.warning("No recipients configured, skipping email")
            return

        time_range = ""
        if time_start and time_end:
            time_range = f"{time_start.strftime('%H:%M')}-{time_end.strftime('%H:%M')}"

        subject = self.subject_template.format(
            date=date.strftime("%Y-%m-%d"),
            window=window_name or "",
            time_range=time_range,
        ).strip()
        plain_body = self._build_body(report_paths)
        html_body = self._build_html_body(report_paths)

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = self.from_address
        msg["To"] = ", ".join(self.to_addresses)
        msg.attach(MIMEText(plain_body, "plain", "utf-8"))
        msg.attach(MIMEText(html_body, "html", "utf-8"))

        context = ssl.create_default_context()
        if self.smtp_port == 465:
            with smtplib.SMTP_SSL(self.smtp_host, self.smtp_port,
                                   context=context, timeout=30) as server:
                server.login(self.smtp_user, self.smtp_password)
                server.sendmail(self.from_address, self.to_addresses, msg.as_string())
        else:
            with smtplib.SMTP(self.smtp_host, self.smtp_port, timeout=30) as server:
                server.ehlo()
                server.starttls(context=context)
                server.ehlo()
                server.login(self.smtp_user, self.smtp_password)
                server.sendmail(self.from_address, self.to_addresses, msg.as_string())
        logger.info(f"Email sent to {self.to_addresses}")

    MAX_BODY_BYTES = 100 * 1024  # 100KB

    def _build_body(self, report_paths: List[str]) -> str:
        parts = []
        total = 0
        for path in report_paths:
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f:
                    content = f.read()
                encoded_len = len(content.encode("utf-8"))
                if total + encoded_len > self.MAX_BODY_BYTES:
                    remaining = self.MAX_BODY_BYTES - total
                    content = content.encode("utf-8")[:remaining].decode("utf-8", errors="ignore")
                    content += "\n\n[... 内容过长，已截断 ...]"
                    parts.append(f"{'='*60}\n{path}\n{'='*60}\n{content}")
                    break
                total += encoded_len
                parts.append(f"{'='*60}\n{path}\n{'='*60}\n{content}")
        return "\n\n".join(parts)

    def _build_html_body(self, report_paths: List[str]) -> str:
        import markdown as md_lib
        parts = []
        total = 0
        for path in report_paths:
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f:
                    content = f.read()
                encoded_len = len(content.encode("utf-8"))
                if total + encoded_len > self.MAX_BODY_BYTES:
                    remaining = self.MAX_BODY_BYTES - total
                    content = content.encode("utf-8")[:remaining].decode("utf-8", errors="ignore")
                    content += "\n\n[... 内容过长，已截断 ...]"
                    parts.append(md_lib.markdown(content, extensions=["tables"]))
                    break
                total += encoded_len
                parts.append(md_lib.markdown(content, extensions=["tables"]))
        body_html = "<hr>".join(parts)
        return f"""<html><body style="font-family:sans-serif;max-width:800px;margin:0 auto;padding:20px">
{body_html}
</body></html>"""
