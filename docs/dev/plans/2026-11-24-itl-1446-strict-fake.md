# ITL-1446 PR 1 SpiralDB client and service behavior

Offline `spiral.core.table.spec.Schema.from_arrow` in pyspiral 0.16.33 rejects fixed-size
binary, duration, union, and interval. Fixed-size binary and duration also fail inside
containers. The installed native library carries the writer diagnostics for value names
starting with `$`, key-only writes, and duplicate keys in an input batch. It separately
diagnoses a key schema that contains a Null type field.

The ITL-1446 service verification confirmed refusal of fixed-size binary, duration,
interval, empty structs, and null struct values both at the top level and in a struct field.
The service sorts the immediate fields of a top-level struct column by name. An absent
key requested with `Scan.to_table(key_table=...)` leaves an all-null output row in its
slot. A zero-row write panics in the client, so the fake raises an ordinary exception.
The service truncates a column name at its first NUL. The fake and connector refuse
such a name before writing.

The installed client's `spiral.api.types.TableName` annotation imposes a 128-character
ASCII identifier constraint, but the actual create path accepted table names at 128,
129, and 255 characters and rejected 256. The connector and fake enforce the measured
limit on the table part. The service also keeps untyped Arrow `null` value columns, as
the offline schema conversion does. The database matrix measures their round trip
through the connector.

Duplicate detection across input batches, restrictions on other characters in the table
part of a name, and sorting struct fields inside other containers remain unverified.
