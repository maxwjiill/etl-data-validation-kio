from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path


def _run(cmd: list[str], *, cwd: Path) -> None:
    completed = subprocess.run(cmd, cwd=cwd, text=True, capture_output=True)
    if completed.returncode != 0:
        details = (completed.stderr or completed.stdout or "").strip()
        raise RuntimeError(f"Command failed: {' '.join(cmd)}\n{details}")


def _docker_health(container_name: str) -> str:
    completed = subprocess.run(
        ["docker", "inspect", "-f", "{{.State.Health.Status}}", container_name],
        text=True,
        capture_output=True,
    )
    if completed.returncode != 0:
        return "unknown"
    return completed.stdout.strip()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reset and start temporary experiment postgres in Docker.")
    parser.add_argument("--container", default="etl_kio_postgres")
    parser.add_argument("--timeout-seconds", type=int, default=180)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    compose_file = repo_root / "docker-compose.experiments.yml"
    if not compose_file.exists():
        raise FileNotFoundError(f"Missing compose file: {compose_file}")

    print("Resetting temporary PostgreSQL container and volume...")
    _run(["docker", "compose", "-f", str(compose_file), "down", "-v"], cwd=repo_root)
    _run(["docker", "compose", "-f", str(compose_file), "up", "-d", "postgres"], cwd=repo_root)

    print("Waiting for healthy status...")
    deadline = time.time() + args.timeout_seconds
    while time.time() < deadline:
        status = _docker_health(args.container)
        if status == "healthy":
            print("Temporary DB is healthy at localhost:55432")
            return
        time.sleep(2)
    raise TimeoutError("Temporary DB did not become healthy in time.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(1)
