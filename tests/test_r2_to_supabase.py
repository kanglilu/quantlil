from __future__ import annotations

import asyncio

import pandas as pd

from r2_to_supabase import MIGRATIONS, R2ToSupabaseMigrator


class FakeR2:
    def __init__(self) -> None:
        self.frames = {
            "raw/alternative/fear_greed/year=2026/data.parquet": (
                pd.DataFrame(
                    [
                        {
                            "source": "fear_greed",
                            "metric_name": "index",
                            "timestamp": "2026-06-09T00:00:00Z",
                            "value": "10",
                            "label": "Extreme Fear",
                            "fetched_at": "2026-06-09T01:00:00Z",
                        }
                    ]
                )
            )
        }

    async def list_parquet_objects(self, prefix):
        return [key for key in self.frames if key.startswith(prefix)]

    async def download_parquet(self, object_key):
        return self.frames[object_key]


class FakeStore:
    def __init__(self) -> None:
        self.alt_rows = []

    async def upsert_alt_data(self, rows):
        self.alt_rows.extend(rows)
        return len(rows)


def test_migrator_normalizes_and_upserts_alt_rows() -> None:
    r2 = FakeR2()
    store = FakeStore()
    migrator = R2ToSupabaseMigrator(  # type: ignore[arg-type]
        r2,
        store,  # type: ignore[arg-type]
        batch_size=1,
    )

    result = asyncio.run(migrator.migrate_spec(MIGRATIONS[0]))

    assert result["status"] == "ok"
    assert result["rows"] == 1
    assert result["upserted"] == 1
    assert store.alt_rows == [
        {
            "source": "fear_greed",
            "metric_name": "index",
            "timestamp": "2026-06-09T00:00:00+00:00",
            "value": 10.0,
            "label": "Extreme Fear",
        }
    ]


def test_migrator_dry_run_does_not_write() -> None:
    store = FakeStore()
    migrator = R2ToSupabaseMigrator(  # type: ignore[arg-type]
        FakeR2(),
        store,  # type: ignore[arg-type]
        dry_run=True,
    )

    result = asyncio.run(migrator.migrate_spec(MIGRATIONS[0]))

    assert result["rows"] == 1
    assert result["upserted"] == 0
    assert store.alt_rows == []
