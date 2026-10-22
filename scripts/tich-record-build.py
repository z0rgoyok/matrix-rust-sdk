#!/usr/bin/env python3
"""Record the exact source and files of an Apple SDK build."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys

root = Path(__file__).resolve().parents[1]
revision = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip()
dirty = subprocess.check_output(['git', 'status', '--porcelain'], cwd=root, text=True).strip()
if revision != sys.argv[1] or dirty:
    raise SystemExit('SDK source changed during the build; do not publish this artifact')
generated = root / 'bindings/apple/generated'
files = {}
for folder in ['swift', 'MatrixSDKFFI.xcframework']:
    for path in sorted((generated / folder).rglob('*')):
        if path.is_file():
            files[str(path.relative_to(generated))] = hashlib.sha256(path.read_bytes()).hexdigest()
if not files or not any(name.endswith('.a') for name in files):
    raise SystemExit('SDK output is incomplete')
receipt = {'source_sha': revision,
           'source_tree': subprocess.check_output(['git', 'rev-parse', 'HEAD^{tree}'], cwd=root, text=True).strip(),
           'rust': '1.98.0',
           'xcode': subprocess.check_output(['xcodebuild', '-version'], text=True).strip(),
           'targets': ['aarch64-apple-ios', 'aarch64-apple-ios-sim'],
           'profile': 'small-release', 'files': files}
(generated / 'tich-build.json').write_text(json.dumps(receipt, indent=2) + '\n')
print(json.dumps({'source_sha': revision, 'verified_files': len(files)}))
