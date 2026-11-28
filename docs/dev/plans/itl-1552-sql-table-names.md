# ITL-1552 SQL store table names

1. Reproduce the table-name collision through the SQLite store and add a
   PostgreSQL integration test for long Unicode paths.
2. Extract the readable hint and canonical path digest from PR 589 into a shared
   naming helper with caller-supplied length limits. Verify the helper against
   one of PR 589's name examples.
3. Apply the approved identifier budget and path listing design to the SQL
   stores. Test that another connection can list and read the tables and that
   the stale-table check finds older layouts and result revisions under the
   node name. Test that tables written under the old names are ignored and
   preserved.
4. Document the required store rebuild and record the digest in the hash site
   inventory. Run the targeted suites, lint, type checks, and golden check.
