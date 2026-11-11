"""pytest 引导：隔离模式（python -I）下补回仓库根目录，并确保样例存在。"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))


def _ensure_samples() -> None:
    healthy_log = ROOT / "samples" / "healthy_table" / "_delta_log"
    incident_log = ROOT / "samples" / "incident_table" / "_delta_log"
    if not healthy_log.glob("*.json") or not incident_log.glob("*.json"):
        sys.path.insert(0, str(ROOT / "scripts"))
        from generate_samples import main as generate  # noqa: E402
        generate()


_ensure_samples()
