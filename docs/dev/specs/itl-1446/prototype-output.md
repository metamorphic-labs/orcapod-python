# Prototype output (generated: `PYTHONPATH=. uv run python docs/dev/specs/itl-1446/prototype.py`)

```text
codec: 200000 distinct names -> 200000 stored names, all decoded
job: first run calls {'add': 6, 'mean': 2, 'notify': 2}, rerun calls {}; rows, schemas, system tags and content hashes equal the in-memory store's
order (6 rows): pipeline table equal False, as_source() equal True
order (60 rows): pipeline table equal False, as_source() equal True
cached source, in memory: k [0, 1, 2, 3, 4, 5], Batch(2) gives y [[0.0, 1.0], [2.0, 3.0], [4.0, 5.0]]
cached source, Delta: k [0, 1, 2, 3, 4, 5], Batch(2) gives y [[0.0, 1.0], [2.0, 3.0], [4.0, 5.0]]
cached source, SpiralDB: k [4, 5, 1, 2, 3, 0], Batch(2) gives y [[4.0, 5.0], [1.0, 2.0], [3.0, 0.0]]
record ids, in memory, 4 rows, key None: ['row_0', 'row_1', 'row_2', 'row_3']
record ids, in memory, 4 rows, key ['k']: ['{"k":0}', '{"k":1}', '{"k":2}', '{"k":3}']
record ids, in memory, 8 rows, key None: ['row_0', 'row_1', 'row_2', 'row_3']
record ids, in memory, 8 rows, key ['k']: ['{"k":0}', '{"k":1}', '{"k":2}', '{"k":3}']
record ids, Delta, 4 rows, key None: ['row_0', 'row_1', 'row_2', 'row_3']
record ids, Delta, 4 rows, key ['k']: ['{"k":0}', '{"k":1}', '{"k":2}', '{"k":3}']
record ids, Delta, 8 rows, key None: ['row_4', 'row_5', 'row_6', 'row_7']
record ids, Delta, 8 rows, key ['k']: ['{"k":0}', '{"k":1}', '{"k":2}', '{"k":3}']
record ids, SpiralDB, 4 rows, key None: ['row_3', 'row_0', 'row_2', 'row_1']
record ids, SpiralDB, 4 rows, key ['k']: ['{"k":0}', '{"k":1}', '{"k":2}', '{"k":3}']
record ids, SpiralDB, 8 rows, key None: ['row_5', 'row_1', 'row_4', 'row_2']
record ids, SpiralDB, 8 rows, key ['k']: ['{"k":0}', '{"k":1}', '{"k":2}', '{"k":3}']

| Rows | Run | list tables | look up table | get metadata | set metadata | create table | write transaction | whole-table scan |
|---|---|---|---|---|---|---|---|---|
| 2 | first | 35 | 15 | 5 | 5 | 5 | 5 | 3 |
| 2 | unchanged rerun | 10 | 14 | 7 | 0 | 0 | 0 | 7 |
| 2 | 10 rows added | 48 | 89 | 48 | 0 | 5 | 5 | 34 |
| 10 | first | 67 | 15 | 5 | 5 | 5 | 5 | 3 |
| 10 | unchanged rerun | 10 | 14 | 7 | 0 | 0 | 0 | 7 |
| 10 | 10 rows added | 48 | 89 | 48 | 0 | 5 | 5 | 34 |
| 50 | first | 227 | 15 | 5 | 5 | 5 | 5 | 3 |
| 50 | unchanged rerun | 10 | 14 | 7 | 0 | 0 | 0 | 7 |
| 50 | 10 rows added | 48 | 89 | 48 | 0 | 5 | 5 | 34 |

| Path A | Path B | Today's one name | Proposed names differ |
|---|---|---|---|
| `job/_result/my-fn/<64 hex>/v0/python.function.v1/rev1/rdb_v1` | `job/_result/my_fn/<64 hex>/v0/python.function.v1/rev1/rdb_v1` | `job___result__my_fn__<64 hex>__v0__python_function_v1__rev1__rdb_v1` | True |
| `job/_result/café/<64 hex>/v0/python.function.v1/rev1/rdb_v1` | `job/_result/cafè/<64 hex>/v0/python.function.v1/rev1/rdb_v1` | `job___result__caf___<64 hex>__v0__python_function_v1__rev1__rdb_v1` | True |
| `run-1/add/pdb_v1` | `run_1/add/pdb_v1` | `run_1__add__pdb_v1` | True |
| `a/b__c` | `a__b/c` | `a__b__c` | True |
| `a_/b` | `a/_b` | `a___b` | True |
| `1x` | `t_1x` | `t_1x` | True |

| Stored table | Today's length | Proposed length |
|---|---|---|
| `proto___result__add__<64 hex>__v0__python_function_v1__rev1__rdb_v1` | 123 | at most 124 |
| `proto___result__mean__<64 hex>__v0__python_function_v1__rev1__rdb_v1` | 124 | at most 124 |
| `proto__add__<64 hex>__v0__python_function_v1__schema_<64 hex>__pdb_v1` | 181 | at most 124 |
| `proto__mean__<64 hex>__v0__python_function_v1__schema_<64 hex>__pdb_v1` | 182 | at most 124 |
| `proto__side_effects__notify__schema_<64 hex>__tdb_v1` | 108 | at most 124 |

wrong result: my-fn doubles [0.0, 2.0]; my_fn triples [0.0, 1.0] and is given [0.0, 2.0]
```
