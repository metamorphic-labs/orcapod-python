# Remove legacy hashing exports

- Discover Orcapod packages dynamically and check each declared export for a
  non-None value. Run the regression with the implementation from origin/main
  and verify that it reports the nine exports.
- Remove the legacy import, fallback assignments, and export entries from
  hashing. Delete the broken generator and its two unused lookup tables.
- Record the fix in the design log and changelog. Regenerate the type
  baseline across the whole project and check that it only removes entries
  fixed by this change.
- Run the targeted pytest tests in one process, repository lint and type
  checks, and the golden hash check. Open a draft pull request against main.
