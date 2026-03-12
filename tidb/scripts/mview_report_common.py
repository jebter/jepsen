#!/usr/bin/env python3
import json
from pathlib import Path

WORKLOADS = ['mv-stateful', 'mv-lifecycle', 'mv-autosched', 'mv-autosched-time']


def load_json(path: Path):
    if not path.exists():
        return None
    return json.loads(path.read_text())


def pick_first(*values):
    for value in values:
        if value is not None:
            return value
    return None


def get_any(mapping, *keys):
    if mapping is None:
        return None
    for key in keys:
        if key in mapping and mapping[key] is not None:
            return mapping[key]
    return None


def summarize_store(store_dir: Path):
    manifest_path = store_dir / 'build' / 'manifest.json'
    manifest = load_json(manifest_path) or {}
    workloads = {}
    for workload in WORKLOADS:
        summary = load_json(store_dir / workload / 'summary.json')
        if summary is not None:
            workloads[workload] = {
                'summary': summary,
                'analysis': load_json(store_dir / workload / 'analysis.json'),
                'snapshots': load_json(store_dir / workload / 'snapshots.json'),
            }
    report = {
        'store_dir': str(store_dir),
        'manifest_present': manifest_path.exists(),
        'recognized_store': bool(workloads),
        'manifest': {
            'branch': manifest.get('branch'),
            'commit_sha': manifest.get('commit_sha'),
            'workload': manifest.get('workload'),
            'nemesis': manifest.get('nemesis'),
            'tarball_url': manifest.get('tarball_url'),
            'binary_urls': manifest.get('binary_urls'),
            'feature_flags': manifest.get('feature_flags'),
            'build_time': manifest.get('build_time'),
            'repro_command': manifest.get('repro_command'),
        },
        'workloads': {},
    }
    for name, payload in workloads.items():
        summary = payload['summary'] or {}
        analysis = payload['analysis'] or {}
        report['workloads'][name] = {
            'valid': get_any(summary, 'valid', 'valid?'),
            'experimental': get_any(summary, 'experimental', 'experimental?'),
            'summary_path': get_any(summary, 'summary-json-path', 'summary_json_path', 'summary-path'),
            'snapshot_path': get_any(summary, 'snapshot-json-path', 'snapshot_json_path', 'snapshot-path'),
            'analysis_path': get_any(summary, 'analysis-json-path', 'analysis_json_path', 'analysis-path'),
            'anomaly_count': pick_first(get_any(summary, 'anomaly-count', 'anomaly_count'), get_any(analysis, 'anomaly-count', 'anomaly_count')),
            'warning_count': pick_first(get_any(summary, 'warning-count', 'warning_count'), get_any(analysis, 'warning-count', 'warning_count')),
            'first_anomaly': get_any(summary, 'first-anomaly', 'first_anomaly'),
            'first_warning': get_any(summary, 'first-warning', 'first_warning'),
            'first_failure': get_any(summary, 'first-failure', 'first_failure'),
            'first_unresolved_write': get_any(summary, 'first-unresolved-write', 'first_unresolved_write'),
            'last_snapshot': get_any(summary, 'last-snapshot', 'last_snapshot'),
        }
        if analysis:
            report['workloads'][name]['quiet_window_ms'] = get_any(analysis, 'quiet-window-ms', 'quiet_window_ms')
            report['workloads'][name]['schedule_metadata'] = get_any(analysis, 'schedule-metadata', 'schedule_metadata')
            report['workloads'][name]['clock_summary'] = get_any(analysis, 'clock-summary', 'clock_summary')
    return report


def require_recognized_store(report):
    if report.get('recognized_store'):
        return report
    raise ValueError(
        f"{report.get('store_dir')} is not a recognized TiDB MV Jepsen store directory: "
        "missing workload summaries"
    )



def format_store_text(report):
    lines = []
    manifest = report['manifest']
    lines.append(f"store: {report['store_dir']}")
    lines.append(
        f"build: workload={manifest.get('workload')} nemesis={manifest.get('nemesis')} "
        f"branch={manifest.get('branch')} commit={manifest.get('commit_sha')}"
    )
    if manifest.get('repro_command'):
        lines.append(f"repro: {manifest['repro_command']}")
    for name, payload in report['workloads'].items():
        lines.append(
            f"[{name}] valid={payload.get('valid')} anomalies={payload.get('anomaly_count')} "
            f"warnings={payload.get('warning_count')}"
        )
        if payload.get('first_anomaly'):
            anomaly = payload['first_anomaly']
            lines.append(f"  first anomaly: {anomaly.get('kind')} - {anomaly.get('message')}")
        if payload.get('first_warning'):
            warning = payload['first_warning']
            lines.append(f"  first warning: {warning.get('kind')} - {warning.get('message')}")
        if payload.get('first_failure'):
            lines.append(
                f"  first failure present: {json.dumps(payload['first_failure'], ensure_ascii=False)[:240]}"
            )
        if payload.get('summary_path'):
            lines.append(f"  summary json: {payload['summary_path']}")
    return '\n'.join(lines)


def aggregate_suite(suite_name, store_dirs):
    runs = []
    invalid_runs = []
    workload_counts = {}
    nemesis_counts = {}

    for store_dir in store_dirs:
        report = require_recognized_store(summarize_store(Path(store_dir)))
        manifest = report.get('manifest', {})
        workloads = report.get('workloads', {})
        inferred_workload = next(iter(workloads)) if len(workloads) == 1 else None
        workload = manifest.get('workload') or inferred_workload
        nemesis = manifest.get('nemesis')

        if not workload or not nemesis:
            raise ValueError(
                f"{store_dir} is not a complete TiDB MV suite input: "
                "missing manifest workload/nemesis metadata"
            )

        run_valid = True
        run_anomalies = 0
        run_warnings = 0
        first_anomaly = None
        first_warning = None

        for _, payload in workloads.items():
            if payload.get('valid') is False:
                run_valid = False
            run_anomalies += payload.get('anomaly_count') or 0
            run_warnings += payload.get('warning_count') or 0
            if first_anomaly is None and payload.get('first_anomaly'):
                first_anomaly = payload.get('first_anomaly')
            if first_warning is None and payload.get('first_warning'):
                first_warning = payload.get('first_warning')

        run = {
            'store_dir': str(store_dir),
            'workload': workload,
            'nemesis': nemesis,
            'branch': manifest.get('branch'),
            'commit_sha': manifest.get('commit_sha'),
            'valid': run_valid,
            'anomaly_count': run_anomalies,
            'warning_count': run_warnings,
            'first_anomaly': first_anomaly,
            'first_warning': first_warning,
            'report': report,
        }
        runs.append(run)
        workload_counts[run['workload']] = workload_counts.get(run['workload'], 0) + 1
        nemesis_counts[run['nemesis']] = nemesis_counts.get(run['nemesis'], 0) + 1
        if not run_valid:
            invalid_runs.append(run)

    return {
        'suite_name': suite_name,
        'run_count': len(runs),
        'invalid_run_count': len(invalid_runs),
        'valid_run_count': len(runs) - len(invalid_runs),
        'workload_counts': workload_counts,
        'nemesis_counts': nemesis_counts,
        'runs': runs,
    }


def format_suite_text(summary):
    lines = [
        f"suite: {summary['suite_name']}",
        f"runs: total={summary['run_count']} valid={summary['valid_run_count']} invalid={summary['invalid_run_count']}",
    ]
    if summary['workload_counts']:
        workload_parts = [f"{k}={v}" for k, v in sorted(summary['workload_counts'].items())]
        lines.append(f"workloads: {' '.join(workload_parts)}")
    if summary['nemesis_counts']:
        nemesis_parts = [f"{k}={v}" for k, v in sorted(summary['nemesis_counts'].items())]
        lines.append(f"nemeses: {' '.join(nemesis_parts)}")
    for run in summary['runs']:
        lines.append(
            f"- {run['workload']} / {run['nemesis']} valid={run['valid']} "
            f"anomalies={run['anomaly_count']} warnings={run['warning_count']}"
        )
        lines.append(f"  store: {run['store_dir']}")
        if run['first_anomaly']:
            lines.append(
                f"  first anomaly: {run['first_anomaly'].get('kind')} - {run['first_anomaly'].get('message')}"
            )
        if run['first_warning']:
            lines.append(
                f"  first warning: {run['first_warning'].get('kind')} - {run['first_warning'].get('message')}"
            )
    return '\n'.join(lines)
