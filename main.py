from __future__ import annotations

import asyncio
import signal

from bot.telegram_bot import TelegramController
from config import get_settings
from scheduler import PipelineScheduler
from storage.supabase_client import SupabaseStore
from utils.logging_config import configure_logging
from utils.single_instance import SingleInstanceLock


async def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level, settings.log_file)
    import logging

    logger = logging.getLogger("main")
    instance_lock = SingleInstanceLock("logs/quant-pipeline.lock")
    instance_lock.acquire()

    try:
        store = SupabaseStore(settings)
        scheduler = PipelineScheduler(settings=settings, store=store)
        telegram = TelegramController(settings=settings, scheduler=scheduler)

        scheduler.start()
        await telegram.start()

        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop_event.set)
            except NotImplementedError:
                # Windows event loops do not always support Unix signal handlers.
                pass

        logger.info("Quant pipeline is running")
        await stop_event.wait()

        logger.info("Stopping quant pipeline")
        await telegram.stop()
        scheduler.stop()
    finally:
        instance_lock.release()


if __name__ == "__main__":
    asyncio.run(main())
