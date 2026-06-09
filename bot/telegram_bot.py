from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

from config import Settings
from scheduler import PipelineScheduler


class TelegramController:
    def __init__(self, settings: Settings, scheduler: PipelineScheduler) -> None:
        self.settings = settings
        self.scheduler = scheduler
        self.logger = logging.getLogger(self.__class__.__name__)
        self.application: Application | None = None

        if settings.telegram_enabled:
            self.application = Application.builder().token(
                settings.telegram_bot_token
            ).build()
            self._register_handlers()
            self.application.add_error_handler(self.error_handler)
            self.scheduler.set_daily_summary_callback(self.send_daily_summary)
            self.scheduler.set_price_heartbeat_callback(
                self.send_price_heartbeat
            )
            self.scheduler.set_alt_data_heartbeat_callback(
                self.send_alt_data_heartbeat
            )
        else:
            self.logger.warning("Telegram env is empty; bot is disabled")

    async def start(self) -> None:
        if self.application is None:
            return
        await self.application.initialize()
        await self.application.start()
        await self.application.updater.start_polling()
        self.logger.info("Telegram bot started")

    async def stop(self) -> None:
        if self.application is None:
            return
        await self.application.updater.stop()
        await self.application.stop()
        await self.application.shutdown()
        self.logger.info("Telegram bot stopped")

    def _register_handlers(self) -> None:
        assert self.application is not None
        self.application.add_handler(CommandHandler("status", self.status))
        self.application.add_handler(CommandHandler("fetch", self.fetch))
        self.application.add_handler(
            CommandHandler("correlation", self.correlation)
        )
        self.application.add_handler(CommandHandler("latest", self.latest))
        self.application.add_handler(CommandHandler("samples", self.samples))
        self.application.add_handler(CommandHandler("alert", self.alert))
        self.application.add_handler(CommandHandler("help", self.help))

    async def status(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        await self._reply(update, self._format_status(self.scheduler.status_snapshot()))

    async def fetch(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        await self._reply(update, "Fetch dimulai. Aku kabari begitu selesai.")
        try:
            result = await self.scheduler.fetch_all_now()
            await self._reply(update, self._format_fetch_result(result))
        except Exception as exc:
            self.logger.exception("Manual fetch failed")
            await self._reply(update, f"Fetch gagal: {exc}")

    async def latest(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        rows = await self.scheduler.store.latest_btc_ohlcv(limit=5)
        if not rows:
            await self._reply(update, "Belum ada data terbaru di Supabase.")
            return

        lines = ["Latest BTC OHLCV:"]
        for row in rows:
            lines.append(
                f"{row['timestamp']} {row['interval']} close={row['close']} volume={row['volume']}"
            )
        await self._reply(update, "\n".join(lines))

    async def correlation(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        try:
            await self._reply(update, "Correlation analysis dimulai.")
            result = await self.scheduler.run_correlation()
            if not isinstance(result, dict):
                raise TypeError("Correlation engine returned an invalid result")
            await self._reply(update, self._format_correlation_result(result))
        except Exception as exc:
            self.logger.exception("Manual correlation failed")
            await self._reply_error(
                update,
                context,
                f"Correlation gagal: {type(exc).__name__}: {exc}",
            )

    async def samples(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        try:
            counts = await self.scheduler.store.sample_counts()
            await self._reply(update, self._format_sample_counts(counts))
        except Exception as exc:
            self.logger.exception("Loading sample counts failed")
            await self._reply_error(
                update,
                context,
                f"Gagal menghitung samples: {type(exc).__name__}: {exc}",
            )

    async def error_handler(
        self,
        update: object,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        self.logger.error(
            "Unhandled Telegram error",
            exc_info=(
                type(context.error),
                context.error,
                context.error.__traceback__,
            )
            if context.error
            else None,
        )
        message = (
            f"Terjadi error: {type(context.error).__name__}: {context.error}"
            if context.error
            else "Terjadi error yang tidak diketahui."
        )
        telegram_update = update if isinstance(update, Update) else None
        await self._reply_error(telegram_update, context, message)

    async def alert(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        args = context.args or []
        if not args or args[0].lower() not in {"on", "off"}:
            state = "on" if self.scheduler.alerts_enabled else "off"
            await self._reply(update, f"Alert sekarang: {state}. Pakai /alert on atau /alert off.")
            return

        self.scheduler.alerts_enabled = args[0].lower() == "on"
        state = "on" if self.scheduler.alerts_enabled else "off"
        await self._reply(update, f"Alert di-set: {state}")

    async def help(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        await self._reply(
            update,
            "\n".join(
                [
                    "Commands:",
                    "/status - cek scheduler dan env",
                    "/fetch - fetch harga, alt, dan macro data",
                    "/correlation - jalankan lag correlation",
                    "/latest - 5 data BTC terbaru",
                    "/samples - jumlah data per source",
                    "/alert on|off - toggle alert",
                    "/help - list command",
                ]
            ),
        )

    async def send_daily_summary(self) -> None:
        if self.application is None:
            return
        await self.application.bot.send_message(
            chat_id=self.settings.telegram_chat_id,
            text=self._format_daily_summary(),
        )

    async def send_price_heartbeat(
        self, price_result: dict[str, Any]
    ) -> None:
        if self.application is None:
            return
        message = self._format_price_heartbeat(price_result)
        if message is None:
            return
        await self.application.bot.send_message(
            chat_id=self.settings.telegram_chat_id,
            text=message,
        )

    async def send_alt_data_heartbeat(
        self, alt_result: dict[str, Any]
    ) -> None:
        if self.application is None:
            return
        await self.application.bot.send_message(
            chat_id=self.settings.telegram_chat_id,
            text=self._format_alt_data_heartbeat(alt_result),
        )

    @staticmethod
    async def _reply(update: Update, text: str) -> None:
        if update.effective_message:
            await update.effective_message.reply_text(text)

    async def _reply_error(
        self,
        update: Update | None,
        context: ContextTypes.DEFAULT_TYPE,
        text: str,
    ) -> None:
        try:
            if update and update.effective_message:
                await update.effective_message.reply_text(text)
                return

            chat_id = (
                update.effective_chat.id
                if update and update.effective_chat
                else self.settings.telegram_chat_id
            )
            await context.bot.send_message(chat_id=chat_id, text=text)
        except Exception:
            self.logger.exception("Failed to deliver Telegram error message")

    @staticmethod
    def _format_status(snapshot: dict[str, Any]) -> str:
        lines = [
            "Quant Pipeline Status",
            f"Scheduler: {'running' if snapshot['running'] else 'stopped'}",
            f"Supabase: {'on' if snapshot['supabase_enabled'] else 'off'}",
            f"Telegram: {'on' if snapshot['telegram_enabled'] else 'off'}",
            f"R2: {'on' if snapshot.get('r2_enabled') else 'off'}",
            f"Alerts: {'on' if snapshot['alerts_enabled'] else 'off'}",
            "",
            "Jobs:",
        ]
        for job in snapshot["jobs"]:
            lines.append(f"- {job['id']}: next {job['next_run_time']}")
        return "\n".join(lines)

    @staticmethod
    def _format_fetch_result(result: dict[str, Any]) -> str:
        price = result.get("price", {})
        lines = ["Fetch selesai:"]
        for interval, payload in price.items():
            if not isinstance(payload, dict):
                continue
            status = payload.get("status", "ok")
            if status == "error":
                lines.append(f"- {interval}: error - {payload.get('error', 'unknown')}")
            else:
                lines.append(
                    f"- {interval}: fetched {payload.get('fetched', 0)}, upserted {payload.get('upserted', 0)}"
                )

        alt_data = result.get("alt_data", {})
        if alt_data:
            lines.append("Alt data:")
            for source in ("fear_greed", "dxy", "polymarket"):
                payload = alt_data.get(source, {})
                if payload.get("status") == "ok":
                    row = payload.get("row", {})
                    lines.append(
                        f"- {source}: {row.get('value')} ({row.get('label') or 'ok'})"
                    )
                elif payload:
                    lines.append(
                        f"- {source}: error - {payload.get('error', 'unknown')}"
                    )
            lines.append(f"- upserted: {alt_data.get('upserted', 0)}")

        macro_data = result.get("macro_data", {})
        if macro_data:
            lines.append("Macro data:")
            treasury = macro_data.get("treasury_10y", {})
            lines.append(
                TelegramController._format_macro_line(
                    "treasury_10y", treasury
                )
            )

            trends = macro_data.get("google_trends", {})
            metrics = trends.get("metrics", {})
            for keyword in ("bitcoin", "crypto"):
                lines.append(
                    TelegramController._format_macro_line(
                        f"google_trends {keyword}",
                        metrics.get(
                            keyword,
                            {
                                "status": trends.get("status", "error"),
                                "error": trends.get("error", "unknown"),
                            },
                        ),
                    )
                )

            lines.append(
                TelegramController._format_macro_line(
                    "bdi", macro_data.get("bdi", {})
                )
            )
            lines.append(f"- upserted: {macro_data.get('upserted', 0)}")
        return "\n".join(lines)

    @staticmethod
    def _format_macro_line(name: str, payload: dict[str, Any]) -> str:
        status = payload.get("status", "error")
        row = payload.get("row", {})
        if status == "ok":
            return f"- {name}: {row.get('value')} (ok)"
        if status == "skip":
            return f"- {name}: unavailable (skip)"
        return f"- {name}: {payload.get('error', 'unknown')} (error)"

    @staticmethod
    def _format_price_heartbeat(
        price_result: dict[str, Any],
    ) -> str | None:
        fetched = 0
        upserted = 0
        for payload in price_result.values():
            if not isinstance(payload, dict):
                continue
            if payload.get("status", "ok") != "ok":
                continue
            fetched += int(payload.get("fetched", 0))
            upserted += int(payload.get("upserted", 0))

        if upserted <= 0:
            return None

        finished_at = price_result.get("finished_at")
        try:
            timestamp = datetime.fromisoformat(str(finished_at))
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
            timestamp = timestamp.astimezone(timezone.utc)
            time_text = timestamp.strftime("%H:%M")
        except (TypeError, ValueError):
            time_text = datetime.now(timezone.utc).strftime("%H:%M")

        return f"✓ {time_text} — price fetched ({fetched} candles)"

    @staticmethod
    def _format_alt_data_heartbeat(
        alt_result: dict[str, Any],
    ) -> str:
        finished_at = alt_result.get("finished_at")
        try:
            timestamp = datetime.fromisoformat(str(finished_at))
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
            timestamp = timestamp.astimezone(timezone.utc)
            utc_text = timestamp.strftime("%H:%M")
            wib_text = timestamp.astimezone(
                ZoneInfo("Asia/Jakarta")
            ).strftime("%H:%M")
        except (TypeError, ValueError):
            timestamp = datetime.now(timezone.utc)
            utc_text = timestamp.strftime("%H:%M")
            wib_text = timestamp.astimezone(
                ZoneInfo("Asia/Jakarta")
            ).strftime("%H:%M")

        statuses = []
        for source in ("fear_greed", "dxy", "polymarket"):
            status = alt_result.get(source, {}).get("status", "error")
            statuses.append(f"{source}={status}")

        return (
            f"✓ {utc_text} UTC / {wib_text} WIB — alt data fetched "
            f"({', '.join(statuses)})"
        )

    @staticmethod
    def _format_correlation_result(result: dict[str, Any]) -> str:
        if result.get("status") == "insufficient_data":
            return str(
                result.get("message")
                or "Insufficient data: butuh minimal 30 samples per source."
            )

        lines = [
            str(result.get("message") or "Correlation selesai:"),
            f"- evaluated: {result.get('evaluated', 0)}",
            f"- insufficient: {result.get('insufficient', 0)}",
            f"- significant: {result.get('significant', 0)}",
            f"- upserted: {result.get('upserted', 0)}",
        ]
        for item in result.get("results", [])[:10]:
            lines.append(
                f"- {item['dataset_a']} lag {item['lag_hours']}h: "
                f"r={item['pearson_r']:.3f}, p={item['p_value']:.4f}, "
                f"n={item['sample_size']}"
            )
        return "\n".join(lines)

    @staticmethod
    def _format_sample_counts(
        counts: dict[str, dict[str, int]],
    ) -> str:
        alt = counts.get("alt_data", {})
        macro = counts.get("macro_data", {})
        btc = counts.get("btc_ohlcv", {})

        def sample_text(value: int, *, pending: bool = False) -> str:
            suffix = "sample" if value == 1 else "samples"
            text = f"{value:,} {suffix}"
            return f"{text} ⏳" if pending and value < 30 else text

        bdi = int(macro.get("bdi", 0))
        lines = [
            "📊 Data Samples per Source:",
            "",
            "Alt Data:",
            f"→ fear_greed  : {sample_text(int(alt.get('fear_greed', 0)))}",
            f"→ dxy         : {sample_text(int(alt.get('dxy', 0)))}",
            (
                "→ polymarket  : "
                f"{sample_text(int(alt.get('polymarket', 0)), pending=True)}"
            ),
            "",
            "Macro Data:",
            (
                "→ treasury_10y         : "
                f"{sample_text(int(macro.get('treasury_10y', 0)))}"
            ),
            (
                "→ google_trends_btc    : "
                f"{sample_text(int(macro.get('google_trends_btc', 0)))}"
            ),
            (
                "→ google_trends_crypto : "
                f"{sample_text(int(macro.get('google_trends_crypto', 0)))}"
            ),
            (
                f"→ bdi                  : {sample_text(bdi)}"
                if bdi
                else "→ bdi                  : 0 (skip)"
            ),
            "",
            "BTC OHLCV:",
            f"→ 1m  : {int(btc.get('1m', 0)):,} candles",
            f"→ 5m  : {int(btc.get('5m', 0)):,} candles",
            f"→ 15m : {int(btc.get('15m', 0)):,} candles",
        ]
        return "\n".join(lines)

    def _format_daily_summary(self) -> str:
        snapshot = self.scheduler.status_snapshot()
        price_fetch = snapshot["last_results"].get("price_fetch", {})
        lines = [
            "Daily Report - Quant Pipeline",
            f"Scheduler: {'running' if snapshot['running'] else 'stopped'}",
            f"Alerts: {'on' if snapshot['alerts_enabled'] else 'off'}",
            "",
            "Price fetch terakhir:",
        ]

        if not price_fetch:
            lines.append("Belum ada fetch tercatat.")
            return "\n".join(lines)

        for interval, payload in price_fetch.items():
            if not isinstance(payload, dict):
                continue
            status = payload.get("status", "ok")
            if status == "error":
                lines.append(f"- {interval}: error - {payload.get('error', 'unknown')}")
            else:
                lines.append(
                    f"- {interval}: fetched {payload.get('fetched', 0)}, upserted {payload.get('upserted', 0)}"
                )
        return "\n".join(lines)
