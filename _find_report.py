#!/usr/bin/env python3
import subprocess, os
os.chdir('/Users/havivrosh/work/metabase_kpi_sync')

def run(cmd):
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    return r.stdout.strip()

print("=== LOCAL BRANCHES ===")
print(run("git branch"))

print("\n=== REMOTE BRANCHES ===")
print(run("git branch -r"))

print("\n=== STASH LIST ===")
print(run("git stash list") or "(none)")

print("\n=== RECENT COMMITS (all branches, last 40) ===")
print(run("git log --all --oneline -40"))

print("\n=== REFLOG (last 20) ===")
print(run("git reflog -20"))

print("\n=== FILES deleted in git history (matching report/state/ascii/weekly) ===")
out = run("git log --all --diff-filter=D --name-only --oneline -50")
for line in out.split('\n'):
    if any(w in line.lower() for w in ['report', 'state', 'ascii', 'weekly', 'snapshot', 'previous', '.py', '.json']):
        print(f"  {line}")

print("\n=== Workspace files matching state/report/snapshot/weekly ===")
for root, dirs, files in os.walk('.'):
    if '.git' in root:
        continue
    for f in files:
        if any(w in f.lower() for w in ['state', 'report', 'snapshot', 'previous', 'weekly', 'ascii']):
            print(f"  {os.path.join(root, f)}")
