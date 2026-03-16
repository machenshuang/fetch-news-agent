import logging
import os
import shutil
import subprocess
from datetime import datetime
from typing import List

from fetchers.base import Article

logger = logging.getLogger(__name__)

BATCH_PROMPT_TEMPLATE = """\
你是一位信息提取助手。请从以下财经快讯中提取关键市场事件，不需要做任何分析或判断。

提取规则：
1. 每条事件必须包含：具体主体（公司/机构/政策文件名）+ 核心事实（数据/动作/结果）
2. 每条不超过30字
3. 忽略以下内容：无实质信息的涨跌报价、重复信息、无主体的泛泛评论

输出格式（严格遵守）：
・[主体] 事实描述
・[主体] 事实描述
...

---
{articles_block}"""

SYNTHESIS_PROMPT_TEMPLATE = """\
你是一位专业的宏观与权益策略分析师。以下是 {date_str} 从市场资讯中分批提炼的关键事件，已按时间段分组。

{combined_batches}

---

你的任务：在上述事实基础上，生成一份清晰、实用的市场日报，面向投资者。

注意事项：
- 每节都必须引用具体事件（含主体+数据），不接受空泛表述
- 投资影响须有事件依据，覆盖利好/利空/中性三个方向，每个方向至少1条
- 风险项须说明"为何尚未充分定价"，每条风险独立成段，不使用编号和嵌套列表
- "重要事件"按重要性降序，最多8条

请严格按以下结构输出（注意每个加粗标题前后必须有空行）：

### 一、投资影响

**利好**

- [板块/资产]：逻辑依据（引用具体事件主体+数据）

**利空**

- [板块/资产]：逻辑依据（引用具体事件主体+数据）

**中性**

- [板块/资产]：逻辑依据（引用具体事件主体+数据）

### 二、风险与不确定性

**[风险因素名称]**

来源事件：... 潜在影响：... 为何尚未充分定价：...

**[风险因素名称]**

来源事件：... 潜在影响：... 为何尚未充分定价：...

### 三、重要事件与关注点

- **【事件主体】** 核心事实（数据/动作） → 为何值得关注/可能的后续影响

*以上分析仅供参考，不构成投资建议。*"""


class GeminiAnalyzer:
    def __init__(self, config: dict):
        cfg = config.get("analyzer", {})
        self.model = cfg.get("model", "gemini-3.1-pro-preview")
        self.fallback_model = cfg.get("fallback_model", "gemini-3-flash-preview")
        self.output_dir = cfg.get("output_dir", "reports")
        self.batch_size = cfg.get("batch_size", 50)

    def analyze(self, articles: List[Article], date: datetime,
                window_name=None, time_start=None, time_end=None) -> str:
        tmp_dir = os.path.join(self.output_dir, "tmp")
        os.makedirs(tmp_dir, exist_ok=True)
        try:
            batch_summaries = self._batch_extract(articles, tmp_dir)
            return self._synthesize(batch_summaries, date, time_start, time_end)
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    def write_report(self, analysis: str, date: datetime,
                     output_dir: str,
                     window_name: str | None = None,
                     time_start=None, time_end=None) -> str:
        os.makedirs(output_dir, exist_ok=True)
        prefix = f"{date.strftime('%Y-%m-%d')}-{window_name}" if window_name else date.strftime('%Y-%m-%d')
        filename = f"{prefix}-analysis.md"
        path = os.path.join(output_dir, filename)

        from zoneinfo import ZoneInfo
        CST = ZoneInfo("Asia/Shanghai")
        title = f"# 财经分析报告 — {date.strftime('%Y-%m-%d')}"
        if time_start or time_end:
            s = time_start.astimezone(CST).strftime("%H:%M") if time_start else "?"
            e = time_end.astimezone(CST).strftime("%H:%M") if time_end else "?"
            title += f" {s}-{e} CST"

        with open(path, "w", encoding="utf-8") as f:
            f.write(title + "\n\n" + analysis)
        return path

    def _batch_extract(self, articles: List[Article], tmp_dir: str) -> List[str]:
        batches = [articles[i:i + self.batch_size] for i in range(0, len(articles), self.batch_size)]
        summaries = []
        for idx, batch in enumerate(batches):
            tmp_path = os.path.join(tmp_dir, f"batch_{idx + 1}.txt")
            if os.path.exists(tmp_path):
                logger.info(f"Batch {idx + 1}/{len(batches)}: resuming from cache")
                with open(tmp_path, encoding="utf-8") as f:
                    summaries.append(f.read())
                continue
            logger.info(f"Batch {idx + 1}/{len(batches)}: extracting {len(batch)} articles")
            result = self._call_with_fallback(self._build_batch_prompt(batch))
            with open(tmp_path, "w", encoding="utf-8") as f:
                f.write(result)
            summaries.append(result)
        return summaries

    def _build_batch_prompt(self, batch: List[Article]) -> str:
        lines = [
            f"{i}. {a.title}｜{a.summary or (a.content[:80] if a.content else '')}"
            for i, a in enumerate(batch, 1)
        ]
        return BATCH_PROMPT_TEMPLATE.format(articles_block="\n".join(lines))

    def _synthesize(self, summaries: List[str], date: datetime,
                    time_start=None, time_end=None) -> str:
        from zoneinfo import ZoneInfo
        CST = ZoneInfo("Asia/Shanghai")
        time_range = ""
        if time_start or time_end:
            s = time_start.astimezone(CST).strftime("%H:%M") if time_start else "?"
            e = time_end.astimezone(CST).strftime("%H:%M") if time_end else "?"
            time_range = f" {s}-{e} CST"
        date_str = date.strftime("%Y-%m-%d") + time_range
        combined = "\n\n".join(f"【第{i + 1}批】\n{s}" for i, s in enumerate(summaries))
        return self._call_with_fallback(SYNTHESIS_PROMPT_TEMPLATE.format(
            date_str=date_str,
            combined_batches=combined,
        ))

    def _call_with_fallback(self, prompt: str) -> str:
        try:
            return self._call_gemini(prompt, self.model)
        except (RuntimeError, subprocess.TimeoutExpired) as e:
            err_str = str(e)
            if isinstance(e, subprocess.TimeoutExpired) or any(
                kw in err_str for kw in ("quota", "429", "Resource exhausted", "not found", "404")
            ):
                logger.warning(f"Error with {self.model} ({err_str[:80]}), falling back to {self.fallback_model}")
                return self._call_gemini(prompt, self.fallback_model)
            raise

    def _call_gemini(self, prompt: str, model: str) -> str:
        gemini_exe = shutil.which("gemini")
        if gemini_exe is None:
            raise RuntimeError("gemini command not found in PATH. Please install it first.")
        proc = subprocess.run(
            [gemini_exe, "-m", model],
            input=prompt,
            capture_output=True,
            text=True,
            timeout=300,
        )
        if proc.returncode != 0:
            raise RuntimeError(proc.stderr)
        return proc.stdout
