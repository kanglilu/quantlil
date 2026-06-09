from __future__ import annotations

import argparse
import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

import pandas as pd

from config import get_settings
from storage.r2_client import R2Store
from storage.supabase_client import SupabaseStore
from utils.logging_config import configure_logging


BATCH_SIZE = 500


@dataclass(frozen=True)
class MigrationSpec:
    name: str
    table: str
    prefixes: tuple[str, ...]
    upsert_method: str
    expected_source: str
    expected_metric: str | None = None


MIGRATIONS = (
    MigrationSpec(
        name="fear_greed",
        table="alt_data",
        prefixes=(
            "raw/alternative/fear_greed/",
            "raw/market/alt_data/fear_greed/",
        ),
        upsert_method="upsert_alt_data",
        expected_source="fear_greed",
        expected_metric="index",
    ),
    MigrationSpec(
        name="dxy",
        table="alt_data",
        prefixes=(
            "raw/alternative/dxy/",
            "raw/market/alt_data/dxy/",
        ),
        upsert_method="upsert_alt_data",
        expected_source="dxy",
        expected_metric="close",
    ),
    MigrationSpec(
        name="treasury_10y",
        table="macro_data",
        prefixes=(
            "raw/macro/fred/",
            "raw/market/macro_data/treasury_10y/",
        ),
        upsert_method="upsert_macro_data",
        expected_source="fred",
        expected_metric="treasury_10y",
    ),
    MigrationSpec(
        name="google_trends",
        table="macro_data",
        prefixes=(
            "raw/macro/google_trends/",
            "raw/market/macro_data/google_trends/",
        ),
        upsert_method="upsert_macro_data",
        expected_source="google_trends",
    ),
)


class R2ToSupabaseMigrator:
    def __init__(
        self,
        r2: R2Store,
        store: SupabaseStore,
        *,
        batch_size: int = BATCH_SIZE,
        dry_run: bool = False,
    ) -> None:
        self.r2 = r2
        self.store = store
        self.batch_size = max(1, batch_size)
        self.dry_run = dry_run
        self.logger = logging.getLogger(self.__class__.__name__)

    async def run(self) -> list[dict[str, Any]]:
        results = []
        for spec in MIGRATIONS:
            results.append(await self._migrate_isolated(spec))
        return results

    async def _migrate_isolated(
        self, spec: MigrationSpec
    ) -> dict[str, Any]:
        print(f"\n[{spec.name}] scanning R2")
        try:
            return await self.migrate_spec(spec)
        except Exception as exc:
            self.logger.exception("Migration failed for %s", spec.name)
            print(f"[{spec.name}] FAILED: {type(exc).__name__}: {exc}")
            return {
                "source": spec.name,
                "table": spec.table,
                "status": "error",
                "files": 0,
                "rows": 0,
                "upserted": 0,
                "error": f"{type(exc).__name__}: {exc}",
            }

    async def migrate_spec(
        self, spec: MigrationSpec
    ) -> dict[str, Any]:
        object_keys: set[str] = set()
        for prefix in spec.prefixes:
            object_keys.update(await self.r2.list_parquet_objects(prefix))

        sorted_keys = sorted(object_keys)
        print(f"[{spec.name}] parquet files found: {len(sorted_keys)}")
        rows_by_key: dict[tuple[str, str, str], dict[str, Any]] = {}
        failed_files = 0

        for index, object_key in enumerate(sorted_keys, start=1):
            try:
                frame = await self.r2.download_parquet(object_key)
                rows = self._normalize_frame(frame, spec)
                for row in rows:
                    unique_key = (
                        row["source"],
                        row["metric_name"],
                        row["timestamp"],
                    )
                    rows_by_key[unique_key] = row
                print(
                    f"[{spec.name}] file {index}/{len(sorted_keys)}: "
                    f"{object_key} ({len(rows):,} rows)"
                )
            except Exception as exc:
                failed_files += 1
                self.logger.exception("Failed reading %s", object_key)
                print(
                    f"[{spec.name}] skip {object_key}: "
                    f"{type(exc).__name__}: {exc}"
                )

        rows = list(rows_by_key.values())
        upserted = 0
        if not self.dry_run:
            upsert: Callable[
                [list[dict[str, Any]]], Awaitable[int]
            ] = getattr(self.store, spec.upsert_method)
            for offset in range(0, len(rows), self.batch_size):
                batch = rows[offset : offset + self.batch_size]
                upserted += await upsert(batch)
                print(
                    f"[{spec.name}] upsert progress: "
                    f"{min(offset + len(batch), len(rows)):,}/{len(rows):,}"
                )

        status = "ok" if failed_files == 0 else "partial"
        result = {
            "source": spec.name,
            "table": spec.table,
            "status": status,
            "files": len(sorted_keys),
            "failed_files": failed_files,
            "rows": len(rows),
            "upserted": upserted,
        }
        print(
            f"[{spec.name}] {status}: rows={len(rows):,}, "
            f"upserted={upserted:,}"
        )
        return result

    @staticmethod
    def _normalize_frame(
        frame: pd.DataFrame | None,
        spec: MigrationSpec,
    ) -> list[dict[str, Any]]:
        if frame is None or frame.empty:
            return []

        normalized = frame.copy()
        normalized["source"] = normalized.get(
            "source", spec.expected_source
        )
        if spec.expected_metric is not None:
            normalized["metric_name"] = normalized.get(
                "metric_name", spec.expected_metric
            )

        required = {"source", "metric_name", "timestamp", "value"}
        missing = required.difference(normalized.columns)
        if missing:
            raise ValueError(
                f"Missing required Parquet columns: {sorted(missing)}"
            )

        normalized["timestamp"] = pd.to_datetime(
            normalized["timestamp"], utc=True, errors="coerce"
        )
        normalized["value"] = pd.to_numeric(
            normalized["value"], errors="coerce"
        )
        normalized = normalized.dropna(
            subset=["source", "metric_name", "timestamp", "value"]
        )

        columns = ["source", "metric_name", "timestamp", "value"]
        if spec.table == "alt_data":
            if "label" not in normalized.columns:
                normalized["label"] = None
            columns.append("label")

        normalized = normalized[columns].copy()
        normalized["timestamp"] = normalized["timestamp"].map(
            lambda value: value.isoformat()
        )
        normalized["value"] = normalized["value"].map(float)
        normalized = normalized.where(pd.notna(normalized), None)
        return normalized.to_dict("records")


def print_summary(results: list[dict[str, Any]], dry_run: bool) -> None:
    mode = "DRY RUN" if dry_run else "UPSERT"
    print(f"\n=== R2 TO SUPABASE SUMMARY ({mode}) ===")
    for result in results:
        print(
            f"{result['source']}: status={result['status']}, "
            f"files={result['files']}, rows={result['rows']:,}, "
            f"upserted={result['upserted']:,}"
        )
    print(
        f"TOTAL: rows={sum(item['rows'] for item in results):,}, "
        f"upserted={sum(item['upserted'] for item in results):,}"
    )


async def async_main(dry_run: bool, batch_size: int) -> int:
    settings = get_settings()
    if not settings.r2_enabled:
        raise RuntimeError("R2 credentials are incomplete in .env")
    if not settings.supabase_enabled:
        raise RuntimeError("Supabase credentials are incomplete in .env")

    migrator = R2ToSupabaseMigrator(
        R2Store(settings),
        SupabaseStore(settings),
        batch_size=batch_size,
        dry_run=dry_run,
    )
    results = await migrator.run()
    print_summary(results, dry_run)
    return 1 if any(item["status"] == "error" for item in results) else 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "One-time migration of selected alt/macro Parquet files "
            "from R2 into Supabase."
        )
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Read and validate R2 files without writing to Supabase.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=BATCH_SIZE,
        help=f"Supabase upsert batch size (default: {BATCH_SIZE}).",
    )
    args = parser.parse_args()
    configure_logging("INFO", "logs/r2-to-supabase.log")
    raise SystemExit(asyncio.run(async_main(args.dry_run, args.batch_size)))


if __name__ == "__main__":
    main()
