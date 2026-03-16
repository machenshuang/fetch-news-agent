#!/usr/bin/env python3
"""
财经资讯日报抓取系统
CLI entry point
"""
import argparse
import asyncio
import logging
import sys
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(
        description="财经资讯日报抓取系统",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py                          # Full run for today
  python main.py --date 2026-03-13        # Backfill a specific date
  python main.py --output-dir reports/custom   # Custom output directory
        """,
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Date to fetch in YYYY-MM-DD format (default: today)",
    )
    parser.add_argument(
        "--config",
        type=str,
        default="config/config.yaml",
        help="Path to config file (default: config/config.yaml)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Output directory for reports (default: reports/)",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable debug logging",
    )
    parser.add_argument(
        "--start-time",
        type=str,
        default=None,
        help="Window start time HH:MM (Asia/Shanghai), e.g. 09:00",
    )
    parser.add_argument(
        "--end-time",
        type=str,
        default=None,
        help="Window end time HH:MM (Asia/Shanghai), e.g. 10:30",
    )
    return parser.parse_args()


async def main():
    args = parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    if args.date:
        try:
            date = datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            print(f"Error: Invalid date format '{args.date}'. Use YYYY-MM-DD.", file=sys.stderr)
            sys.exit(1)
    else:
        date = datetime.now(tz=timezone.utc)

    CST = ZoneInfo("Asia/Shanghai")
    time_start = time_end = None
    if args.start_time or args.end_time:
        base = date.astimezone(CST).date()
        if args.start_time:
            h, m = map(int, args.start_time.split(":"))
            time_start = datetime(base.year, base.month, base.day, h, m, tzinfo=CST)
        if args.end_time:
            h, m = map(int, args.end_time.split(":"))
            time_end = datetime(base.year, base.month, base.day, h, m, tzinfo=CST)

    print(f"\n{'='*60}")
    print(f"  财经资讯日报抓取系统")
    print(f"  Date: {date.strftime('%Y-%m-%d')}")
    if time_start or time_end:
        print(f"  Window: {args.start_time or '?'} - {args.end_time or '?'} CST")
    print(f"{'='*60}\n")

    from pipeline import run_pipeline

    result = await run_pipeline(
        date=date,
        config_path=args.config,
        output_dir=args.output_dir,
        time_start=time_start,
        time_end=time_end,
    )

    print(f"\n{'='*60}")
    print(f"  Pipeline Complete")
    print(f"{'='*60}")
    print(f"  Total articles fetched : {result.total_articles}")
    print(f"\n  Articles by source:")
    for source, count in result.source_article_counts.items():
        print(f"    {source}: {count} articles")
    if result.errors:
        print(f"\n  Warnings/Errors ({len(result.errors)}):")
        for err in result.errors:
            print(f"    - {err}")
    print(f"\n  Reports written:")
    for path in result.report_paths:
        print(f"    {path}")
    print()


if __name__ == "__main__":
    asyncio.run(main())
