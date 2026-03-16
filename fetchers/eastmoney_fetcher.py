import asyncio
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import List, Optional
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

from fetchers.base import Article, BaseFetcher

logger = logging.getLogger(__name__)

CST = ZoneInfo("Asia/Shanghai")
KUAIXUN_URL = "https://np-weblist.eastmoney.com/comm/web/getFastNewsList"


class EastMoneyFetcher(BaseFetcher):
    def __init__(self, config: dict):
        super().__init__(config)
        cfg = config.get("eastmoney", {})
        self.cutoff_hours = cfg.get("cutoff_hours", 24)
        self.content_timeout = cfg.get("content_timeout", 10)
        self.concurrency = cfg.get("concurrency", 5)

    async def fetch(
        self,
        time_start: Optional[datetime] = None,
        time_end: Optional[datetime] = None,
    ) -> List[Article]:
        loop = asyncio.get_event_loop()
        items = await loop.run_in_executor(None, self._fetch_list_sync, time_start, time_end)
        if not items:
            return []

        semaphore = asyncio.Semaphore(self.concurrency)

        async def fetch_one(item):
            async with semaphore:
                content = await loop.run_in_executor(None, self._fetch_content_sync, item["url"])
                return Article(
                    title=item["title"],
                    summary=item["summary"],
                    url=item["url"],
                    source="东方财富快讯",
                    published=item["published"],
                    content=content or item["summary"],
                )

        articles = await asyncio.gather(*[fetch_one(item) for item in items], return_exceptions=True)
        result = []
        for a in articles:
            if isinstance(a, Exception):
                logger.warning(f"EastMoney article fetch error: {a}")
            else:
                result.append(a)
        logger.info(f"EastMoney: fetched {len(result)} 快讯")
        return result

    def _fetch_list_sync(
        self,
        time_start: Optional[datetime] = None,
        time_end: Optional[datetime] = None,
    ) -> list:
        if time_start is not None:
            cutoff = time_start.astimezone(timezone.utc)
        else:
            cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=self.cutoff_hours)
        upper_bound = time_end.astimezone(timezone.utc) if time_end is not None else None
        result = []
        sort_end = ""

        try:
            while True:
                params = {
                    "client": "web",
                    "biz": "web_724",
                    "fastColumn": "102",
                    "sortEnd": sort_end,
                    "pageSize": "50",
                    "req_trace": str(int(time.time() * 1000)),
                }
                resp = requests.get(
                    KUAIXUN_URL,
                    params=params,
                    timeout=15,
                    headers={"User-Agent": "Mozilla/5.0"},
                )
                resp.raise_for_status()
                data = resp.json().get("data", {})
                items = data.get("fastNewsList", [])
                if not items:
                    break

                reached_cutoff = False
                for item in items:
                    showtime = item.get("showTime", "")
                    try:
                        dt = datetime.strptime(showtime, "%Y-%m-%d %H:%M:%S").replace(tzinfo=CST)
                        dt_utc = dt.astimezone(timezone.utc)
                    except Exception:
                        continue
                    if upper_bound is not None and dt_utc > upper_bound:
                        continue
                    if dt_utc < cutoff:
                        reached_cutoff = True
                        break
                    code = item.get("code", "")
                    title = item.get("title", "").strip()
                    summary = item.get("summary", "").strip()
                    if not title or not code:
                        continue
                    result.append({
                        "title": title,
                        "summary": summary,
                        "url": f"https://finance.eastmoney.com/a/{code}.html",
                        "published": dt_utc,
                    })

                if reached_cutoff:
                    break
                sort_end = data.get("sortEnd", "")
                if not sort_end:
                    break

        except Exception as e:
            logger.error(f"EastMoney 快讯 list fetch error: {e}")

        return result

    def _fetch_content_sync(self, url: str) -> str:
        try:
            resp = requests.get(url, timeout=self.content_timeout, headers={"User-Agent": "Mozilla/5.0"})
            resp.encoding = "utf-8"
            soup = BeautifulSoup(resp.text, "lxml")
            el = soup.find("div", class_="txtinfos")
            if el:
                for tag in el.find_all("p", class_="em_media"):
                    tag.decompose()
                return el.get_text(separator="\n", strip=True)
            return ""
        except Exception as e:
            logger.debug(f"EastMoney content fetch error for {url}: {e}")
            return ""
