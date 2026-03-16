from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional
from abc import ABC, abstractmethod


@dataclass
class Article:
    title: str
    summary: str
    url: str
    source: str
    published: datetime
    content: str = ""

    def __repr__(self):
        return f"Article(source={self.source!r}, title={self.title[:50]!r})"


class BaseFetcher(ABC):
    def __init__(self, config: dict):
        self.config = config

    @abstractmethod
    async def fetch(
        self,
        time_start: Optional[datetime] = None,
        time_end: Optional[datetime] = None,
    ) -> List[Article]:
        """Fetch articles and return as list of Article objects."""
        pass
