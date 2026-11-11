# ITL-895: function output values are not checked against the declared type

Status log (for a successor picking this up):

- 2026-11-11 — Step 1 (root cause) done. Findings and one evidence script per claim are in
  `superpowers/specs/itl-895-float-to-int/` (`FINDINGS.md`, script outputs in `outputs/`).
  The findings were sent to the coordinator with `orca orchestration ask`. **Waiting for the
  coordinator's choice of fix option ((a) pod check, (b) strict converter, (c) both on one
  rule) and the conformance decisions D1–D7 in FINDINGS section 11.** No library code has
  been changed yet.

The implementation plan (writing-plans) is written here after the coordinator replies.
