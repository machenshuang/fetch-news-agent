#!/usr/bin/env python3
"""自动定时调度器：在每个配置时间窗口的开始时触发 pipeline"""
import argparse
import asyncio
import logging
import sys
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
from datetime import datetime
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from pipeline import run_pipeline, load_config


def parse_args():
    p = argparse.ArgumentParser(description="fetch-news scheduler")
    p.add_argument("--config", default="config/config.yaml")
    p.add_argument("--output-dir", default=None)
    p.add_argument("--verbose", "-v", action="store_true")
    return p.parse_args()


async def run_window(window, tz, config_path, output_dir):
    from datetime import timedelta
    name = window["name"]
    now = datetime.now(tz=tz)
    base = now.date()
    sh, sm = map(int, window["start"].split(":"))
    eh, em = map(int, window["end"].split(":"))
    time_end = datetime(base.year, base.month, base.day, eh, em, tzinfo=tz)
    # 跨日窗口：start 时钟值 > end，说明 start 在前一天
    if (sh, sm) > (eh, em):
        yesterday = base - timedelta(days=1)
        time_start = datetime(yesterday.year, yesterday.month, yesterday.day, sh, sm, tzinfo=tz)
    else:
        time_start = datetime(base.year, base.month, base.day, sh, sm, tzinfo=tz)
    logging.info(f"Window [{name}] triggered: {window['start']}-{window['end']} CST")
    try:
        result = await run_pipeline(
            date=time_start,
            config_path=config_path,
            output_dir=output_dir,
            time_start=time_start,
            time_end=time_end,
            window_name=name,
        )
        logging.info(f"Window [{name}] done: {result.total_articles} articles")
    except Exception:
        logging.exception(f"Window [{name}] pipeline failed")


async def main():
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    config = load_config(args.config)
    sched_cfg = config.get("scheduler", {})
    if not sched_cfg.get("enabled", False):
        logging.warning("Scheduler disabled (scheduler.enabled: false). Exiting.")
        return
    tz_name = sched_cfg.get("timezone", "Asia/Shanghai")
    tz = ZoneInfo(tz_name)
    windows = sched_cfg.get("windows", [])
    if not windows:
        logging.warning("No scheduler.windows configured. Exiting.")
        return
    scheduler = AsyncIOScheduler(timezone=tz_name)
    for w in windows:
        sh, sm = map(int, w["start"].split(":"))
        eh, em = map(int, w["end"].split(":"))
        # 所有窗口统一在 end 时刻触发（数据积累完毕后处理）
        scheduler.add_job(
            run_window,
            CronTrigger(hour=eh, minute=em, timezone=tz_name),
            args=[w, tz, args.config, args.output_dir],
            id=f"window_{w['name']}",
            misfire_grace_time=300,
            max_instances=1,
        )
        logging.info(f"Scheduled [{w['name']}] at {w['end']} {tz_name}")
    scheduler.start()
    logging.info(f"Scheduler running with {len(windows)} window(s). Ctrl+C to stop.")
    try:
        await asyncio.Event().wait()
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
