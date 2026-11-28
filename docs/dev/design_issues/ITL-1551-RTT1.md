## Result cache and pipeline record selection

### RTT1 — Timestamp ties depended on database row order
**Status:** resolved
**Severity:** high
**Issue:** ITL-1551

Result cache lookup and function-node record selection could choose a different
result when the database returned several equal-time records in a different order.

**Fix:** Compare stored timestamps first. In the result cache, use the greater
result UUIDv7 on a tie. For equally timed pipeline records, an available result
takes priority over a missing ephemeral result. When the tied results are both
available or both gone, use the greater UUIDv7. If all ephemeral results for an
instance are gone, this ordering selects the record that supplies its placeholder.
