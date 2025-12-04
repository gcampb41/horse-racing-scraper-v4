#!/usr/bin/env python3
"""
Retry failed races listed in a log file. The log will be truncated before retry,
so only new failures remain after the run. A backup of the log is kept.
"""
import argparse
import shutil
import subprocess
import sys
from pathlib import Path

DEFAULT_JOBS = 6


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Retry failed races from a log file.')
    parser.add_argument('--log', required=True, help='Path to log file with failed race URLs')
    parser.add_argument('-r', '--region', help='Region code (defaults to region inferred from log path)')
    parser.add_argument('-j', '--jobs', type=int, default=DEFAULT_JOBS, help='Concurrent workers (1-10)')
    return parser.parse_args()


def extract_dates(log_path: Path) -> list[str]:
    dates: set[str] = set()
    for line in log_path.read_text(encoding='utf-8').splitlines():
        parts = line.split('/')
        if len(parts) < 2:
            continue
        # Date segment sits just before the race id
        date_part = parts[-2]
        if len(date_part.split('-')) == 3:
            dates.add(date_part)
    return sorted(dates)


def main():
    args = parse_args()
    log_path = Path(args.log).expanduser().resolve()
    if not log_path.exists():
        print(f'Log file not found: {log_path}')
        sys.exit(1)

    # Infer region from log path if not provided
    region = args.region
    try:
        if region is None:
            # Expect path like data/dates/<region>/...
            region = log_path.parts[log_path.parts.index('dates') + 1]
    except ValueError:
        pass

    if not region:
        print('Region could not be inferred; please provide --region')
        sys.exit(1)

    dates = extract_dates(log_path)
    if not dates:
        print('No dates found in log; nothing to retry.')
        sys.exit(0)

    jobs = max(1, min(10, args.jobs))

    backup_path = log_path.with_suffix(log_path.suffix + '.bak')
    shutil.copyfile(log_path, backup_path)
    print(f'Backed up log to {backup_path}')

    # Truncate log so only new failures are recorded
    log_path.write_text('', encoding='utf-8')

    script_dir = Path(__file__).parent
    rpscrape = script_dir / 'rpscrape.py'

    for d in dates:
        date_arg = d.replace('-', '/')
        print(f'Retrying {d}...')
        cmd = [sys.executable, str(rpscrape), '-d', date_arg, '-r', region, '-j', str(jobs)]
        result = subprocess.run(cmd)
        if result.returncode != 0:
            print(f'  ! rpscrape failed for {d} (exit {result.returncode})')

    print('Retry complete. Remaining failures (if any) are in the log.')


if __name__ == '__main__':
    main()
