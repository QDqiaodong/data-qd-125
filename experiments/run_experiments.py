"""Reproducible experiments for the SQLite incremental pipeline.

Each scenario runs in a separate ``worker.py`` child process (started with
``python -I``) so peak RSS is measured per stage without polluting the runner.
The runner only orchestrates and writes the JSON report; generated batches and
databases live under the chosen output directory.

Scenarios::

    functional   late bridge, same-second touches, empty-batch watermark
                 advance, byte-identical replay, crash/rollback recovery, and
                 a small-data equivalence cross-check against the full model;
    scale        N events (default 1,000,000) with one heavily skewed user,
                 ingested in several batches and exported, measuring peak RSS.
"""
import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', type=Path, default=ROOT / 'run' / 'experiments')
    parser.add_argument('--scale-events', type=int, default=1_000_000)
    parser.add_argument('--skew-share', type=float, default=0.7)
    parser.add_argument('--rss-budget-mib', type=int, default=128)
    args = parser.parse_args()
    if args.out.exists():
        shutil.rmtree(args.out)
    args.out.mkdir(parents=True)
    worker = Path(__file__).with_name('worker.py')
    env = dict(os.environ, PYTHONPATH=str(ROOT / 'src'))

    def spawn(mode, workdir):
        proc = subprocess.run(
            [sys.executable, '-I', str(worker), mode, str(workdir),
             str(args.scale_events), str(args.skew_share)],
            capture_output=True, text=True, env=env)
        if proc.returncode != 0:
            raise RuntimeError(f'worker {mode} failed\nstdout={proc.stdout}\nstderr={proc.stderr}')
        return json.loads(proc.stdout)

    functional = spawn('functional', args.out / 'functional')
    scale_dir = args.out / 'scale'
    scale = spawn('scale', scale_dir)
    export_stats = spawn('scale-export', scale_dir)
    verify = spawn('verify', scale_dir)
    scale.update(export_stats)
    scale['full_export_verification'] = verify
    scale['requested_events'] = args.scale_events
    scale['skew_share'] = args.skew_share
    report = {
        'python': sys.version.split()[0],
        'scale_events': args.scale_events,
        'skew_share': args.skew_share,
        'rss_budget_mib': args.rss_budget_mib,
        'scenarios': {'functional': functional, 'scale_memory': scale},
        'within_budget': (scale['apply_peak_rss_mib'] <= args.rss_budget_mib and
                          scale['export_peak_rss_mib'] <= args.rss_budget_mib),
    }
    report_path = args.out / 'experiment-report.json'
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + '\n')
    print(json.dumps(report, indent=2, sort_keys=True))
    if not report['within_budget']:
        print('RSS BUDGET EXCEEDED', file=sys.stderr)
        raise SystemExit(1)


if __name__ == '__main__':
    main()
