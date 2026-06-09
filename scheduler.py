from __future__ import annotations

import logging
from datetime import datetime, timezone
from collections.abc import Awaitable, Callable
from typing import Any

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from agents.altdata_agent import AltDataAgent
from agents.macro_agent import MacroAgent
from agents.price_agent import PriceAgent
from analysis.correlation import CorrelationEngine
from config import Settings
from storage.data_lake import DataLakeWriter
from storage.data_lake_reader import DataLakeReader
from storage.r2_client import R2Store
from storage.supabase_client import SupabaseStore


class PipelineScheduler:
    def __init__(self, settings: Settings, store: SupabaseStore) -> None:
        self.settings = settings
        self.store = store
        self.logger = logging.getLogger(self.__class__.__name__)
        self.scheduler = AsyncIOScheduler(timezone=settings.timezone)
        self.data_lake: DataLakeWriter | None = None
        self.data_lake_reader: DataLakeReader | None = None
        if settings.r2_enabled:
            r2_store = R2Store(settings)
            self.data_lake = DataLakeWriter(r2_store, store)
            self.data_lake_reader = DataLakeReader(r2_store, store)
        self.price_agent = PriceAgent(store=store, data_lake=self.data_lake)
        self.altdata_agent = AltDataAgent(
            store=store, data_lake=self.data_lake
        )
        self.macro_agent = MacroAgent(
            store=store, data_lake=self.data_lake
        )
        self.correlation_engine = CorrelationEngine(
            settings=settings,
            store=store,
            data_lake=self.data_lake_reader,
        )
        self.last_results: dict[str, Any] = {}
        self.alerts_enabled = True
        self.daily_summary_callback: Callable[[], Awaitable[None]] | None = None
        self.price_heartbeat_callback: (
            Callable[[dict[str, Any]], Awaitable[None]] | None
        ) = None
        self.alt_data_heartbeat_callback: (
            Callable[[dict[str, Any]], Awaitable[None]] | None
        ) = None

    def set_daily_summary_callback(
        self, callback: Callable[[], Awaitable[None]]
    ) -> None:
        self.daily_summary_callback = callback

    def set_price_heartbeat_callback(
        self,
        callback: Callable[[dict[str, Any]], Awaitable[None]],
    ) -> None:
        self.price_heartbeat_callback = callback

    def set_alt_data_heartbeat_callback(
        self,
        callback: Callable[[dict[str, Any]], Awaitable[None]],
    ) -> None:
        self.alt_data_heartbeat_callback = callback

    def start(self) -> None:
        self.scheduler.add_job(
            self.fetch_price_data_scheduled,
            CronTrigger(minute=0, timezone="UTC"),
            id="price_fetch_hourly",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
        self.scheduler.add_job(
            self.fetch_alt_data_scheduled,
            CronTrigger(hour="*/6", minute=10, timezone="UTC"),
            id="alt_data_fetch_6h",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
        self.scheduler.add_job(
            self.fetch_macro_data,
            CronTrigger(hour=6, minute=0, timezone="UTC"),
            id="macro_data_daily",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
        self.scheduler.add_job(
            self.run_correlation,
            CronTrigger(hour=7, minute=0, timezone="UTC"),
            id="correlation_daily",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
        self.scheduler.add_job(
            self.send_daily_summary,
            CronTrigger(hour=7, minute=30, timezone="UTC"),
            id="daily_summary",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
        self.scheduler.start()
        self.logger.info("Scheduler started")

    def stop(self) -> None:
        self.scheduler.shutdown(wait=False)
        self.logger.info("Scheduler stopped")

    async def fetch_price_data(self) -> dict[str, Any]:
        started_at = datetime.now(timezone.utc)
        self.logger.info("Fetching price data")
        result = await self.price_agent.fetch_all()
        result["started_at"] = started_at.isoformat()
        result["finished_at"] = datetime.now(timezone.utc).isoformat()
        self.last_results["price_fetch"] = result
        self.logger.info("Price fetch completed: %s", result)
        return result

    async def fetch_price_data_scheduled(self) -> dict[str, Any]:
        result = await self.fetch_price_data()
        if self.price_heartbeat_callback is not None:
            try:
                await self.price_heartbeat_callback(result)
            except Exception:
                self.logger.exception("Price heartbeat delivery failed")
        return result

    async def fetch_alt_data(self) -> dict[str, Any]:
        started_at = datetime.now(timezone.utc)
        self.logger.info("Fetching alternative data")
        result = await self.altdata_agent.fetch_all()
        result["started_at"] = started_at.isoformat()
        result["finished_at"] = datetime.now(timezone.utc).isoformat()
        self.last_results["alt_data_fetch"] = result
        self.logger.info("Alternative data fetch completed: %s", result)
        return result

    async def fetch_alt_data_scheduled(self) -> dict[str, Any]:
        result = await self.fetch_alt_data()
        if self.alt_data_heartbeat_callback is not None:
            try:
                await self.alt_data_heartbeat_callback(result)
            except Exception:
                self.logger.exception("Alt-data heartbeat delivery failed")
        return result

    async def fetch_macro_data(self) -> dict[str, Any]:
        started_at = datetime.now(timezone.utc)
        self.logger.info("Fetching macro data")
        result = await self.macro_agent.fetch_all()
        result["started_at"] = started_at.isoformat()
        result["finished_at"] = datetime.now(timezone.utc).isoformat()
        self.last_results["macro_data_fetch"] = result
        self.logger.info("Macro data fetch completed: %s", result)
        return result

    async def run_correlation(self) -> dict[str, Any]:
        started_at = datetime.now(timezone.utc)
        self.logger.info("Running lag correlation analysis")
        result = await self.correlation_engine.run()
        result["started_at"] = started_at.isoformat()
        result["finished_at"] = datetime.now(timezone.utc).isoformat()
        self.last_results["correlation"] = result
        return result

    async def fetch_all_now(self) -> dict[str, Any]:
        return {
            "price": await self.fetch_price_data(),
            "alt_data": await self.fetch_alt_data(),
            "macro_data": await self.fetch_macro_data(),
        }

    async def send_daily_summary(self) -> None:
        self.last_results["daily_summary"] = {
            "status": "started",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        if self.daily_summary_callback is None:
            self.last_results["daily_summary"]["status"] = "no_callback_registered"
            return

        await self.daily_summary_callback()
        self.last_results["daily_summary"]["status"] = "sent"

    def status_snapshot(self) -> dict[str, Any]:
        jobs = []
        for job in self.scheduler.get_jobs():
            jobs.append(
                {
                    "id": job.id,
                    "next_run_time": job.next_run_time.isoformat()
                    if job.next_run_time
                    else None,
                }
            )

        return {
            "running": self.scheduler.running,
            "alerts_enabled": self.alerts_enabled,
            "jobs": jobs,
            "last_results": self.last_results,
            "supabase_enabled": self.settings.supabase_enabled,
            "telegram_enabled": self.settings.telegram_enabled,
            "r2_enabled": self.settings.r2_enabled,
        }
