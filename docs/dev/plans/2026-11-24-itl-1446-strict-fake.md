# ITL-1446 PR 1: pyspiral refusals

- Verify the restrictions in sections 0, 3.2 and 3.4 of PR 542 against pyspiral 0.16.33 without a service.
- Cover the fake's refusals directly. Make the connector report unsupported Arrow types and table names before writes, and check its errors through the connector and database APIs.
- Update test input that relied on an unsupported write. Regenerate the database type matrix and run the targeted suites, lint, types and golden checks.

The installed client's `spiral.api.types.TableName` declares 128 characters and the ASCII pattern
`^[a-zA-Z_][a-zA-Z0-9_-]*$`. An offline Pydantic `TypeAdapter` check confirmed the endpoints
and rejects a final newline. Offline `spiral.core.table.spec.Schema.from_arrow` rejects fixed-size
binary, duration, union and interval. Fixed-size binary and duration also fail inside lists,
structs, maps and dictionary or run-end encoded values.

The installed native library carries the write diagnostics for a value name starting with
`$`, a write array with only key columns, and duplicate keys in an input batch. It also
says a key schema cannot contain a Null type field.

The scope of duplicate detection across Arrow batches is unverified. The native null column-group
diagnostic does not establish refusal of an Arrow `null` value column, which `Schema.from_arrow`
accepts. The connector explicitly refuses such columns as required by PR 1. The schema converter
preserves struct field order, so the fake does not infer nested ordering from the client. ITL-1367
recorded nested sorting on the service.
