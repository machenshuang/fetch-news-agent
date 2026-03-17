import asyncio
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import yaml

from analyzer.gemini_analyzer import GeminiAnalyzer
from fetchers.base import Article
from fetchers.eastmoney_fetcher import EastMoneyFetcher
from reporter.markdown_reporter import MarkdownReporter

logger = logging.getLogger(__name__)


@dataclass
class PipelineResult:
    date: datetime
    report_paths: List[str]
    total_articles: int
    source_article_counts: Dict[str, int]
    errors: List[str] = field(default_factory=list)


def load_config(config_path: str = "config/config.yaml") -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        content = f.read()

    import re
    def replace_env(match):
        var = match.group(1)
        return os.environ.get(var, match.group(0))
    content = re.sub(r"\$\{([^}]+)\}", replace_env, content)

    return yaml.safe_load(content)


async def run_pipeline(
    date: Optional[datetime] = None,
    config_path: str = "config/config.yaml",
    output_dir: Optional[str] = None,
    time_start: Optional[datetime] = None,
    time_end: Optional[datetime] = None,
    window_name: Optional[str] = None,
) -> PipelineResult:
    if date is None:
        date = datetime.now(tz=timezone.utc)

    logger.info(f"Starting pipeline for {date.strftime('%Y-%m-%d')}")

    config = load_config(config_path)

    errors = []
    fetchers = [
        EastMoneyFetcher(config),
    ]

    results = await asyncio.gather(
        *[f.fetch(time_start=time_start, time_end=time_end) for f in fetchers],
        return_exceptions=True,
    )

    all_articles: List[Article] = []
    source_article_counts: Dict[str, int] = {}
    fetcher_names = ["EastMoney"]

    for name, result in zip(fetcher_names, results):
        if isinstance(result, Exception):
            msg = f"Fetcher [{name}] error: {result}"
            logger.error(msg)
            errors.append(msg)
        else:
            all_articles.extend(result)
            for article in result:
                source_article_counts[article.source] = source_article_counts.get(article.source, 0) + 1
            logger.info(f"Fetcher [{name}]: {len(result)} articles")

    logger.info(f"Total articles fetched: {len(all_articles)}")

    if time_start is not None or time_end is not None:
        start_utc = time_start.astimezone(timezone.utc) if time_start else None
        end_utc   = time_end.astimezone(timezone.utc)   if time_end   else None
        all_articles = [
            a for a in all_articles
            if a.published
            and (start_utc is None or a.published.astimezone(timezone.utc) >= start_utc)
            and (end_utc   is None or a.published.astimezone(timezone.utc) <= end_utc)
        ]
        logger.info(f"Articles within window: {len(all_articles)}")
    else:
        cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=24)
        all_articles = [
            a for a in all_articles
            if a.published and a.published.astimezone(timezone.utc) >= cutoff
        ]
        logger.info(f"Articles within last 24h: {len(all_articles)}")

    reporter = MarkdownReporter(config)
    paths = reporter.generate(all_articles, date, output_dir,
                              window_name=window_name,
                              time_start=time_start, time_end=time_end)

    analyzer_cfg = config.get("analyzer", {})
    if analyzer_cfg.get("enabled", False):
        analyzer = GeminiAnalyzer(config)
        try:
            analysis_md = analyzer.analyze(all_articles, date,
                                           window_name=window_name,
                                           time_start=time_start, time_end=time_end)
            out = output_dir or config.get("reporter", {}).get("output_dir", "reports")
            analysis_path = analyzer.write_report(analysis_md, date, out,
                                                  window_name=window_name,
                                                  time_start=time_start, time_end=time_end)
            paths.append(analysis_path)
            logger.info(f"Analysis written: {analysis_path}")
        except Exception as e:
            msg = f"Analyzer error: {e}"
            logger.error(msg)
            errors.append(msg)

    notifier_cfg = config.get("notifier", {})
    if notifier_cfg.get("enabled", False):
        from notifier.email_notifier import EmailNotifier
        notifier = EmailNotifier(config)
        try:
            analysis_paths = [p for p in paths if "analysis" in p]
            notifier.notify(analysis_paths if analysis_paths else paths, date, window_name=window_name)
        except Exception as e:
            msg = f"Notifier error: {e}"
            logger.exception(msg)
            errors.append(msg)

    return PipelineResult(
        date=date,
        report_paths=paths,
        total_articles=len(all_articles),
        source_article_counts=source_article_counts,
        errors=errors,
    )
