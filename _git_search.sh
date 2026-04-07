#!/bin/bash
cd /Users/havivrosh/work/metabase_kpi_sync

echo "=== LOCAL BRANCHES ==="
git branch

echo ""
echo "=== REMOTE BRANCHES ==="
git branch -r

echo ""
echo "=== STASH LIST ==="
git stash list

echo ""
echo "=== RECENT COMMITS (all branches) ==="
git log --all --oneline -40

echo ""
echo "=== DELETED FILES IN HISTORY ==="
git log --all --diff-filter=D --summary --oneline -20 2>/dev/null | grep -E "delete|\.py|\.json|state|report|ascii|weekly" | head -40

echo ""
echo "=== REFLOG (recent branch changes) ==="
git reflog -20

echo ""
echo "=== FILES matching state/report/snapshot/previous ==="
find . -name "*state*" -o -name "*report*" -o -name "*snapshot*" -o -name "*previous*" -o -name "*weekly*" -o -name "*ascii*" 2>/dev/null | grep -v node_modules | grep -v .git
