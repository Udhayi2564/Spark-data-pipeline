from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional


STATE_DIRECTORY: Path = Path("storage") / "migration_state"
STATE_FILE: Path = STATE_DIRECTORY / "migrations.json"


class MigrationStateError(Exception):
    pass


def _ensure_state_directory() -> None:
    STATE_DIRECTORY.mkdir(
        parents=True,
        exist_ok=True,
    )


def _load_states() -> Dict[str, Dict[str, Any]]:
    _ensure_state_directory()

    if not STATE_FILE.exists():
        return {}

    try:
        content: str = STATE_FILE.read_text(
            encoding="utf-8",
        )

        if not content.strip():
            return {}

        states: Dict[str, Dict[str, Any]] = json.loads(content)

        return states

    except json.JSONDecodeError as error:
        raise MigrationStateError(
            f"Invalid migration state file: {error}"
        ) from error


def _save_states(
    states: Dict[str, Dict[str, Any]],
) -> None:
    _ensure_state_directory()

    content: str = json.dumps(
        states,
        indent=4,
        default=str,
    )

    STATE_FILE.write_text(
        content,
        encoding="utf-8",
    )


def build_migration_key(
    source_table: str,
    target_table: str,
) -> str:
    source_key: str = source_table.strip().lower()
    target_key: str = target_table.strip().lower()

    return f"{source_key}__{target_key}"


def get_migration_state(
    source_table: str,
    target_table: str,
) -> Optional[Dict[str, Any]]:
    states: Dict[str, Dict[str, Any]] = _load_states()

    migration_key: str = build_migration_key(
        source_table=source_table,
        target_table=target_table,
    )

    return states.get(migration_key)


def is_initial_migration(
    source_table: str,
    target_table: str,
) -> bool:
    state: Optional[Dict[str, Any]] = get_migration_state(
        source_table=source_table,
        target_table=target_table,
    )

    if state is None:
        return True

    initialized: bool = bool(
        state.get("initialized", False)
    )

    return not initialized


def save_migration_state(
    source_table: str,
    target_table: str,
    rows_processed: int,
) -> None:
    states: Dict[str, Dict[str, Any]] = _load_states()

    migration_key: str = build_migration_key(
        source_table=source_table,
        target_table=target_table,
    )

    states[migration_key] = {
        "source_table": source_table,
        "target_table": target_table,
        "initialized": True,
        "last_rows_processed": rows_processed,
    }

    _save_states(states)