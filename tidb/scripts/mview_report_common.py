#!/usr/bin/env python3
import json
import re
from collections import OrderedDict
from pathlib import Path

WORKLOADS = ['mv-stateful', 'mv-lifecycle', 'mv-autosched', 'mv-autosched-time']

TIDB_RUNTIME_PATTERNS = {
    'release_version': re.compile(r'\["Release Version"=([^\]]+)\]'),
    'edition': re.compile(r'\[Edition=([^\]]+)\]'),
    'git_commit_hash': re.compile(r'\["Git Commit Hash"=([^\]]+)\]'),
    'git_branch': re.compile(r'\["Git Branch"=([^\]]+)\]'),
    'utc_build_time': re.compile(r'\["UTC Build Time"="([^"]+)"\]'),
    'enterprise_extension_commit_hash': re.compile(r'\["Enterprise Extension Commit Hash"=([^\]]+)\]'),
}

INSTALL_TARBALL_PATTERN = re.compile(r':tarball-url ([^,}\s]+)')
INSTALL_BINARY_OVERRIDE_PATTERN = re.compile(r':stage :binary-override-start, :url ([^,}\s]+)')


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


def parse_tidb_runtime_line(line: str):
    if 'Welcome to TiDB.' not in line:
        return None

    fingerprint = {}
    for field, pattern in TIDB_RUNTIME_PATTERNS.items():
        match = pattern.search(line)
        if match:
            fingerprint[field] = match.group(1)

    if not fingerprint.get('release_version'):
        return None
    return fingerprint


def collect_tidb_runtime_fingerprints(store_dir: Path):
    fingerprints = OrderedDict()

    for db_log in sorted(store_dir.glob('node-*/db.log')):
        try:
            with db_log.open(errors='replace') as handle:
                for line in handle:
                    fingerprint = parse_tidb_runtime_line(line)
                    if fingerprint is None:
                        continue

                    key = json.dumps(fingerprint, sort_keys=True)
                    entry = fingerprints.setdefault(
                        key,
                        {
                            **fingerprint,
                            'nodes': [],
                            'occurrences': 0,
                        },
                    )
                    entry['nodes'].append(db_log.parent.name)
                    entry['occurrences'] += 1
                    break
        except OSError:
            continue

    values = list(fingerprints.values())
    for entry in values:
        entry['nodes'].sort()

    return {
        'tidb_fingerprints': values,
        'uniform_tidb_fingerprint': values[0] if len(values) == 1 else None,
    }


def parse_install_inputs_from_jepsen_log(store_dir: Path):
    jepsen_log = store_dir / 'jepsen.log'
    if not jepsen_log.exists():
        return {'tarball_url': None, 'binary_urls': []}

    tarball_url = None
    binary_urls = []

    try:
        with jepsen_log.open(errors='replace') as handle:
            for line in handle:
                if tarball_url is None and 'TiDB install {' in line and ':tarball-url ' in line:
                    match = INSTALL_TARBALL_PATTERN.search(line)
                    if match and match.group(1) != 'nil':
                        tarball_url = match.group(1)

                if ':stage :binary-override-start' in line:
                    match = INSTALL_BINARY_OVERRIDE_PATTERN.search(line)
                    if match and match.group(1) not in binary_urls:
                        binary_urls.append(match.group(1))
    except OSError:
        return {'tarball_url': None, 'binary_urls': []}

    return {'tarball_url': tarball_url, 'binary_urls': binary_urls}


def summarize_store(store_dir: Path):
    manifest_path = store_dir / 'build' / 'manifest.json'
    manifest = load_json(manifest_path) or {}
    install_inputs = parse_install_inputs_from_jepsen_log(store_dir)
    runtime = collect_tidb_runtime_fingerprints(store_dir)
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
        'reportable_store': bool(
            workloads
            or manifest_path.exists()
            or (store_dir / 'jepsen.log').exists()
            or runtime['tidb_fingerprints']
        ),
        'recognized_store': bool(workloads),
        'manifest': {
            'branch': manifest.get('branch'),
            'commit_sha': manifest.get('commit_sha'),
            'workload': manifest.get('workload'),
            'nemesis': manifest.get('nemesis'),
            'version': manifest.get('version'),
            'tarball_url': manifest.get('tarball_url'),
            'binary_urls': manifest.get('binary_urls'),
            'feature_flags': manifest.get('feature_flags'),
            'build_time': manifest.get('build_time'),
            'notes': manifest.get('notes'),
            'repro_command': manifest.get('repro_command'),
        },
        'build_inputs': {
            'requested_version': manifest.get('version'),
            'tarball_url': pick_first(manifest.get('tarball_url'), install_inputs.get('tarball_url')),
            'tarball_url_source': (
                'manifest'
                if manifest.get('tarball_url') is not None
                else 'jepsen.log'
                if install_inputs.get('tarball_url') is not None
                else None
            ),
            'binary_urls': pick_first(manifest.get('binary_urls'), install_inputs.get('binary_urls')),
            'binary_urls_source': (
                'manifest'
                if manifest.get('binary_urls') is not None
                else 'jepsen.log'
                if install_inputs.get('binary_urls')
                else None
            ),
        },
        'runtime': runtime,
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
    build_inputs = report.get('build_inputs', {})
    runtime = report.get('runtime', {})
    lines.append(f"store: {report['store_dir']}")
    lines.append(
        f"build: workload={manifest.get('workload')} nemesis={manifest.get('nemesis')} "
        f"requested_version={build_inputs.get('requested_version')} "
        f"branch={manifest.get('branch')} commit={manifest.get('commit_sha')}"
    )
    if build_inputs.get('tarball_url'):
        lines.append(
            f"install tarball: {build_inputs['tarball_url']} "
            f"(source={build_inputs.get('tarball_url_source')})"
        )
    if build_inputs.get('binary_urls'):
        lines.append(
            f"install binary overrides: {', '.join(build_inputs['binary_urls'])} "
            f"(source={build_inputs.get('binary_urls_source')})"
        )
    if runtime.get('uniform_tidb_fingerprint'):
        fingerprint = runtime['uniform_tidb_fingerprint']
        lines.append(
            "runtime tidb: "
            f"release={fingerprint.get('release_version')} "
            f"edition={fingerprint.get('edition')} "
            f"commit={fingerprint.get('git_commit_hash')} "
            f"branch={fingerprint.get('git_branch')} "
            f"build_time={fingerprint.get('utc_build_time')} "
            f"nodes={','.join(fingerprint.get('nodes', []))}"
        )
    elif runtime.get('tidb_fingerprints'):
        lines.append("runtime tidb fingerprints:")
        for fingerprint in runtime['tidb_fingerprints']:
            lines.append(
                "  "
                f"release={fingerprint.get('release_version')} "
                f"edition={fingerprint.get('edition')} "
                f"commit={fingerprint.get('git_commit_hash')} "
                f"build_time={fingerprint.get('utc_build_time')} "
                f"nodes={','.join(fingerprint.get('nodes', []))}"
            )
    if manifest.get('notes'):
        lines.append(f"notes: {manifest['notes']}")
    if manifest.get('repro_command'):
        lines.append(f"repro: {manifest['repro_command']}")
    if not report['workloads']:
        lines.append("[no workload summaries present]")
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
