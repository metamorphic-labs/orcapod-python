## Result cache and pipeline record selection

### RTT1 — Timestamp ties depended on database row order
**Status:** resolved
**Severity:** high
**Issue:** ITL-1551

Result cache lookup and function-node record selection could choose a different
result when the database returned several equal-time records in a different order.

**Fix:** Compare the stored timestamps first and the result UUIDv7 bytes second.
The rule also selects the placeholder when multiple records point to vanished
ephemeral results for the same instance.
