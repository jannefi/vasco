
# VASCO — Post‑setup Next Steps Checklist

Congrats — all 11 step issues, PR template, workflow gates, smoke tests, and CODEOWNERS are in place. This checklist helps you drive the work to completion with predictable reviews and CI.

## 1) Operate the step issues (today)

- [ ] **Assign owners** (you or others) to each step issue (#4–#14).
- [ ] **Add acceptance criteria** to each issue using the Step Checklists in `PLAN.md`.
- [ ] **Set due date** via milestone (optional).
- [ ] **Link PRs** to issues with `Closes #<id>` in the PR body.

### Commands (GitHub CLI)
```bash
# Set milestone due date (example date)
MID=$(gh api repos/:owner/:repo/milestones --jq '.[] | select(.title=="Multi-step split") | .number')
# Update milestone due_on
gh api repos/:owner/:repo/milestones/$MID -X PATCH -f due_on='2026-01-31T23:59:59Z'

# Assign issues (replace @user)
for id in 4 5 6 7 8 9 10 11 12 13 14; do
  gh issue edit $id --add-assignee jannefi
done
```

## 2) CI hardening options (opt‑in)

- [ ] **Require CODEOWNERS review** on `main` via branch protection.
- [ ] **Block direct pushes** to `main`; allow only PR merges.
- [ ] **Require the new checks**: PR issue‑link gate + step smoke.

> UI path: *Settings → Branches → Branch protection rules → Add rule* (Branch name: `main`).

### API (requires admin rights)
```bash
# Require status checks and PR reviews on main
OWNER_REPO=$(gh repo view --json nameWithOwner --jq .nameWithOwner)
# Minimal rule example (tweak as needed)
DATA='{
  "required_status_checks": {
    "strict": true,
    "contexts": [
      "PR must link a step issue",
      "VASCO step smoke tests"
    ]
  },
  "enforce_admins": true,
  "required_pull_request_reviews": {
    "required_approving_review_count": 1,
    "require_code_owner_reviews": true
  },
  "restrictions": null
}'
# Create or update rule
# Note: endpoint varies per GitHub plan; if not available, use the UI.
# gh api repos/:owner/:repo/branches/main/protection -X PUT -H "Accept: application/vnd.github+json" -f data="$DATA"
```

## 3) Definition of Done (per step)

Use the following DoD for each step PR before merge:

- **Artifacts present** for the step (see `PLAN.md` → Step Checklists).
- **Manifest updated** with inputs/outputs/params.
- **Logs** written to the correct `logs/` subfolder.
- **Smoke test** for that step passes (or is skipped with a documented reason).
- **Linked issue** closed on merge (`Closes #<id>`).

## 4) Documentation hygiene

- [ ] Keep `PLAN.md` updated as steps evolve.
- [ ] Add short `README.md` snippets under `scripts/cli/` describing usage.
- [ ] Record any default changes in `config/pipeline.yaml`.

## 5) Release tagging (when steps 0–9 are stable)

```bash
# Tag and create a release (example)
git checkout main
# ensure CI is green
gh release create v0.06.10 --title "VASCO v0.06.10 (CDS-only, step split)" \
  --notes "Stable two-pass PSF flow; smoke tests across steps; dashboards updated"
```

## 6) Backlog ideas (near‑term)

- Bright‑star mask generator: parameterized heuristics + catalog toggle.
- USNO‑B caching & rate‑limits; retry/backoff.
- STILTS wrapper: robust arcsec/degree fallback with explicit unit checks.
- Packaging: `.sha256` per zip and a top‑level `artifacts/index.json`.

---
**References**: CI workflow present in baseline zip (`.github/workflows/ci.yml`); Step Checklists appended to `PLAN.md`; milestone **Multi‑step split**.
