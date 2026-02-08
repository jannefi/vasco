## Interpretation Policy (VASCO / POSS‑I replication)

**Version:** 2026‑02‑08  
**Scope:** All analyses and summaries derived from POSS‑I (red) DSS1 scans and related cross‑matches in the VASCO Post‑1.6 pipeline.

### 1) Evidence hierarchy (what counts as “real”)
- **Object‑level claims**: A detection is **not** interpreted as a confirmed optical transient unless **microscopic inspection of the original POSS‑I glass negative** (and, where feasible, intermediate copy positives/negatives) verifies a star‑like silver‑grain profile consistent with a short‑duration optical event. Copy‑plate or scan-level evidence alone is **insufficient**.
- **Population‑level claims** (e.g., Earth’s‑shadow deficits, temporal correlations, alignment statistics) must be supported by **plate‑aware** and **background‑aware** statistics (see §2 and §3). Extraordinary claims require independent replication.

### 2) Plate/time correctness (foundational constraints)
- Every detection we publish is **plate‑qualified**: `plate_id = REGION` is canonical, and plate headers (UTC start/stop, exposure, field center) are published and used in all schedule‑normalized analyses.
- All time‑based statistics are **conditioned on actual observing days** (“plate‑days”), not wall‑clock spans. South‑only observation days are excluded when the analyzed dataset contains no southern sampling.
- Spatial analyses are **per‑plate**, using measured **2‑D density maps** and **radial profiles**; these maps define the expected background (inhomogeneous null) for any “signal” test.

### 3) Artefact control (copy/scan chain first, then catalogs)
- **Cross‑scan gate** is mandatory: detections present in one scan (DSS or SuperCOSMOS) but absent in the other are treated as **artefacts** and removed.
- **External gates** (post cross‑scan): Gaia/PS1 (5″), NEOWISE strict mask (documented radius & quality), SkyBoT (asteroids), VSX/PTF (variables), plus **secondary HPM sweep** for Gaia entries without PM.
- A lightweight **morphology QA** (PSF symmetry/ellipticity outlier checks) is applied before any “signal” tests.

### 4) Claims discipline (language and thresholds)
- **Object‑level wording**: Use *candidate*, *plate‑level detection*, or *unresolved origin* unless microscopic confirmation on **original negatives** exists.
- **Population‑level wording**: Report **Observed vs Expected** under an **inhomogeneous background** per plate (never uniform‑Poisson by default). Publish uncertainty and **power**; avoid over‑interpretation of small‑N effects.
- **Temporal claims**: Always normalized to **plate‑days** and (where practicable) stratified by season/lunar conditions. Report schedule overlap explicitly.

### 5) Reproducibility (what we publish with results)
- **Counts Appendix**: stage‑wise counts (raw→morphology→optical→IR→cross‑scan→external→R_like), with per‑plate median/p95 and unique‑object estimates (incl. de‑dup radius).
- **QC artefacts**: per‑plate 2‑D background parquet + quick‑look plots; plate‑day registry parquet; manifest SHA‑256 for every deliverable.
- **Reason codes**: every exclusion is tagged (`drop_reason[]`), and connector jobs keep a minimal ledger (status/retries/duration/rows).

### 6) Limits & non‑goals
- We do **not** infer technosignatures or exotic mechanisms from copy‑scan datasets alone.
- We do **not** rely on secondary media (e.g., social media posts) as evidence for or against detections.
- We don't make extraordinary claims. We **publish code, data products, and methods** so results are checkable.

### 7) Update policy
- This policy is revised when (a) microscopic studies of **original** plates are available, (b) material changes to cross‑scan/comparison sources occur, or (c) the statistical framework for background modeling is updated.

> **One‑sentence summary:** *Absent microscopic confirmation on the original negatives, copy‑plate detections are treated as candidates only; all population‑level inferences are made with plate‑aware, inhomogeneous‑background and schedule‑normalized methods, with uncertainty and power reported.*
