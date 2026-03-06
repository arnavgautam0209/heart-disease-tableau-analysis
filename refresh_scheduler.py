"""
Automated Data Refresh Scheduler
===================================
Automates the full pipeline: CSV → SQLite → Extracts → Tableau exports.
Supports both manual runs and cron-based scheduling.

Usage:
  python3 refresh_scheduler.py --once              # Run once immediately
  python3 refresh_scheduler.py --interval 3600     # Run every hour
  python3 refresh_scheduler.py --install-cron       # Install crontab for daily refresh
  python3 refresh_scheduler.py --uninstall-cron     # Remove crontab entry
"""

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_PATH = os.path.join(BASE_DIR, "refresh_log.json")


def log_refresh(status, duration_sec, details=""):
    """Append a refresh entry to the JSON log."""
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "duration_seconds": round(duration_sec, 2),
        "details": details,
    }

    # Load or create log
    if os.path.exists(LOG_PATH):
        with open(LOG_PATH, "r") as f:
            try:
                log = json.load(f)
            except json.JSONDecodeError:
                log = []
    else:
        log = []

    log.append(entry)

    # Keep last 100 entries
    log = log[-100:]

    with open(LOG_PATH, "w") as f:
        json.dump(log, f, indent=2)

    return entry


def run_pipeline():
    """Execute the full refresh pipeline."""
    print(f"\n  [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Starting data refresh...\n")
    start = time.time()

    steps = [
        ("Database Pipeline", [sys.executable, os.path.join(BASE_DIR, "db_setup.py")]),
        ("Extract Generation", [sys.executable, os.path.join(BASE_DIR, "generate_extract.py")]),
    ]

    all_ok = True
    details_parts = []

    for step_name, cmd in steps:
        print(f"  ── {step_name} ──")
        try:
            result = subprocess.run(
                cmd,
                cwd=BASE_DIR,
                capture_output=True,
                text=True,
                timeout=300,  # 5 min timeout per step
            )
            if result.returncode == 0:
                # Print stdout (indented)
                for line in result.stdout.strip().split("\n"):
                    print(f"    {line}")
                details_parts.append(f"{step_name}: OK")
            else:
                print(f"    ❌ {step_name} failed (exit code {result.returncode})")
                if result.stderr:
                    for line in result.stderr.strip().split("\n")[:5]:
                        print(f"    {line}")
                details_parts.append(f"{step_name}: FAILED ({result.returncode})")
                all_ok = False
        except subprocess.TimeoutExpired:
            print(f"    ❌ {step_name} timed out (300s)")
            details_parts.append(f"{step_name}: TIMEOUT")
            all_ok = False
        except Exception as e:
            print(f"    ❌ {step_name} error: {e}")
            details_parts.append(f"{step_name}: ERROR ({e})")
            all_ok = False
        print()

    duration = time.time() - start
    status = "success" if all_ok else "partial_failure"
    entry = log_refresh(status, duration, "; ".join(details_parts))

    icon = "✅" if all_ok else "⚠️"
    print(f"  {icon}  Refresh complete in {duration:.1f}s — Status: {status}")
    print(f"  📋  Log: {LOG_PATH}\n")

    return all_ok


def run_interval(seconds):
    """Run the pipeline on a fixed interval."""
    print("=" * 60)
    print("  Heart Disease — Scheduled Data Refresh")
    print("=" * 60)
    print(f"  Interval: every {seconds}s ({seconds/3600:.1f}h)")
    print(f"  Press Ctrl+C to stop.\n")

    while True:
        try:
            run_pipeline()
            print(f"  💤  Next refresh in {seconds}s...\n")
            time.sleep(seconds)
        except KeyboardInterrupt:
            print("\n  Scheduler stopped.")
            break


def install_cron():
    """Install a crontab entry for daily refresh at 2 AM."""
    python = sys.executable
    script = os.path.join(BASE_DIR, "refresh_scheduler.py")
    cron_line = f"0 2 * * * cd {BASE_DIR} && {python} {script} --once >> {BASE_DIR}/cron_refresh.log 2>&1"

    # Check if already installed
    try:
        existing = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
        current_cron = existing.stdout if existing.returncode == 0 else ""
    except Exception:
        current_cron = ""

    if "refresh_scheduler.py" in current_cron:
        print("  ⚠️  Cron job already installed. Use --uninstall-cron to remove it first.")
        return

    # Add the new cron entry
    new_cron = current_cron.rstrip("\n") + "\n" + cron_line + "\n"
    process = subprocess.run(
        ["crontab", "-"],
        input=new_cron,
        capture_output=True,
        text=True,
    )

    if process.returncode == 0:
        print("  ✅  Cron job installed successfully!")
        print(f"  Schedule: Daily at 2:00 AM")
        print(f"  Command:  {cron_line}")
        print(f"  Log file: {BASE_DIR}/cron_refresh.log")
    else:
        print(f"  ❌  Failed to install cron: {process.stderr}")


def uninstall_cron():
    """Remove the refresh crontab entry."""
    try:
        existing = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
        if existing.returncode != 0:
            print("  No crontab found.")
            return

        lines = existing.stdout.split("\n")
        new_lines = [l for l in lines if "refresh_scheduler.py" not in l]
        new_cron = "\n".join(new_lines)

        if len(new_lines) == len(lines):
            print("  ⚠️  No refresh cron job found to remove.")
            return

        subprocess.run(["crontab", "-"], input=new_cron, capture_output=True, text=True)
        print("  ✅  Cron job removed successfully.")
    except Exception as e:
        print(f"  ❌  Failed to remove cron: {e}")


def main():
    parser = argparse.ArgumentParser(description="Heart Disease — Data Refresh Scheduler")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--once", action="store_true", help="Run refresh once and exit")
    group.add_argument("--interval", type=int, metavar="SECS", help="Run refresh every N seconds")
    group.add_argument("--install-cron", action="store_true", help="Install daily cron job (2 AM)")
    group.add_argument("--uninstall-cron", action="store_true", help="Remove cron job")
    args = parser.parse_args()

    if args.once:
        success = run_pipeline()
        sys.exit(0 if success else 1)
    elif args.interval:
        run_interval(args.interval)
    elif args.install_cron:
        install_cron()
    elif args.uninstall_cron:
        uninstall_cron()


if __name__ == "__main__":
    main()
