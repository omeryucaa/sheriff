from __future__ import annotations

from pathlib import Path
import shutil
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.settings import get_settings
from app.storage.database_service import DatabaseService


RUNTIME_LOGS = [
    "data/ingest_trace.log",
    "ingest_trace.log",
    "dry_run_trace.log",
    "data/backend.log",
    "data/frontend.log",
]

RUNTIME_DBS = [
    "data/redkid.db",
    "redkid.db",
]

RUNTIME_CACHE_DIRS = [
    "app/__pycache__",
    "app/adapters/__pycache__",
    "app/api/__pycache__",
    "app/config/__pycache__",
    "app/models/__pycache__",
    "app/pipeline/__pycache__",
    "app/prompts/__pycache__",
    "app/services/__pycache__",
    "app/storage/__pycache__",
    "app/utils/__pycache__",
    "tests/__pycache__",
]


def _remove_file(path: Path) -> bool:
    if not path.exists():
        return False
    path.unlink()
    return True


def _remove_tree(path: Path) -> bool:
    if not path.exists():
        return False
    shutil.rmtree(path)
    return True


def main() -> None:
    project_root = PROJECT_ROOT
    settings = get_settings()
    db_path = (project_root / settings.sqlite_db_path).resolve()
    db_service = DatabaseService(str(db_path))
    db_service.init_schema()
    deleted_rows = db_service.reset_runtime_state()

    removed_files = []
    for relative_path in [*RUNTIME_LOGS, *RUNTIME_DBS]:
        target = (project_root / relative_path).resolve()
        if _remove_file(target):
            removed_files.append(str(target.relative_to(project_root)))

    removed_dirs = []
    for relative_path in RUNTIME_CACHE_DIRS:
        target = (project_root / relative_path).resolve()
        if _remove_tree(target):
            removed_dirs.append(str(target.relative_to(project_root)))

    print("Runtime tables cleared:")
    for table_name, count in deleted_rows.items():
        print(f"- {table_name}: {count}")

    print("\nRemoved files:")
    for item in removed_files:
        print(f"- {item}")

    print("\nRemoved directories:")
    for item in removed_dirs:
        print(f"- {item}")


if __name__ == "__main__":
    main()
