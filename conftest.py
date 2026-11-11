"""pytest 引导：隔离模式（python -I）下补回仓库根目录，并确保样例存在。"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))


def _ensure_samples() -> None:
    sample_dirs = [
        ROOT / "samples" / "healthy_table" / "_delta_log",
        ROOT / "samples" / "incident_table" / "_delta_log",
        ROOT / "samples" / "shared_history_table" / "_delta_log",
    ]
    if any(not d.glob("*.json") for d in sample_dirs):
        sys.path.insert(0, str(ROOT / "scripts"))
        from generate_samples import main as generate  # noqa: E402
        generate()


_ensure_samples()
