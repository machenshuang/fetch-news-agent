import os
import logging
from datetime import datetime, timezone
from typing import Dict, List
from zoneinfo import ZoneInfo

from fetchers.base import Article

logger = logging.getLogger(__name__)

CST = ZoneInfo("Asia/Shanghai")


def _time_range_str(time_start, time_end):
    """返回 '09:00-10:30 CST' 或 None"""
    if not time_start and not time_end:
        return None
    s = time_start.astimezone(CST).strftime("%H:%M") if time_start else "?"
    e = time_end.astimezone(CST).strftime("%H:%M") if time_end else "?"
    return f"{s}-{e} CST"


SOURCE_SLUG = {
    "东方财富": "eastmoney",
    "同花顺": "tonghuashun",
    "Reuters": "reuters",
    "CNBC": "cnbc",
}


class MarkdownReporter:
    def __init__(self, config: dict):
        self.output_dir = config.get("reporter", {}).get("output_dir", "reports")

    def generate(self, articles: List[Article], date: datetime,
                 output_dir: str | None = None,
                 window_name: str | None = None,
                 time_start=None, time_end=None) -> List[str]:
        """Generate per-source Markdown files. Returns list of written file paths."""
        out_dir = output_dir or self.output_dir
        os.makedirs(out_dir, exist_ok=True)

        date_str = date.strftime("%Y-%m-%d")
        now_str = datetime.now(tz=timezone.utc).astimezone(CST).strftime("%Y-%m-%d %H:%M:%S CST")

        # Group by source
        by_source: Dict[str, List[Article]] = {}
        for article in articles:
            by_source.setdefault(article.source, []).append(article)

        written_paths = []

        # Per-source files
        for source, arts in by_source.items():
            slug = SOURCE_SLUG.get(source, source.lower().replace(" ", "_"))
            prefix = f"{date_str}-{window_name}-" if window_name else f"{date_str}-"
            filename = f"{prefix}{slug}.md"
            path = os.path.join(out_dir, filename)

            time_range = _time_range_str(time_start, time_end)
            lines = []
            lines.append(f"# {source} 新闻 - {date_str}{' ' + time_range if time_range else ''}\n")
            lines.append(f"> 抓取时间: {now_str}")
            period_str = time_range or "过去24小时"
            lines.append(f"> 共 {len(arts)} 篇文章（{period_str}）\n")
            lines.append("---\n")

            for art in arts:
                pub_cst = art.published.astimezone(CST)
                pub_str = pub_cst.strftime("%Y-%m-%d %H:%M CST")
                lines.append(f"## {art.title}\n")
                lines.append(f"**链接**: {art.url}")
                lines.append(f"**时间**: {pub_str}\n")
                if art.content:
                    lines.append(art.content)
                elif art.summary:
                    lines.append(art.summary)
                lines.append("\n---\n")

            with open(path, "w", encoding="utf-8") as f:
                f.write("\n".join(lines))
            logger.info(f"Report written: {path}")
            written_paths.append(path)

        return written_paths
