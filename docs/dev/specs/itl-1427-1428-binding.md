# Binding a job's sources: stale node hashes (ITL-1427) and slot names (ITL-1428)

Status: **proposal, for the developer to decide. Nothing is implemented.**

Everything under "Measured" was run on `main` at `4d55ad75` through the public API
(except where a private attribute is named), with two tags, two data columns and the chain
source -> function -> operator -> function -> side effect. The proposals marked
"simulated" were patched into the library inside the script's own process; nothing under
`src/` changed. The scripts and their output are kept by the coordinator; the short
reproductions are in the appendix.

## Decisions asked for

| # | Question | Recommendation |
|---|---|---|
| D1 | How does a node's content hash follow a bind? | A memoized hash is valid for one *binding epoch* (section 1.4) |
| D2 | Two slots bound to one source (ITL-1442, found here): the nodes below are equal and one is silently not run | Refuse at `bind` and at the start of a run, naming the slots (section 1.5) |
| D3 | Node DAGs keyed by a content hash that a bind changes (ITL-1204) | Key them by object, in the same change as D1 |
| D4 | What a loaded job's nodes answer after the user binds a source | The bound pipeline's hash, as for any job; a load alone moves nothing (section 1.6). Can be a second PR |
| D5 | A source made by `node.as_source()` before a bind | Refuse to read once the node's hash is no longer the one it was made from (section 1.7) |
| D6 | A slot's default name | Assigned label, else source name; explicit `name=` first (section 2.3) |
| D7 | One source given under two labels in one pipeline | Refuse (section 2.4) |
| D8 | A `CachedSource` over a labelled source | Its own label, else the wrapped source's label, else the name |

**No golden value moves in either change, and no stored path in the default table scope.**
The exact effect on hashes is in sections 1.8 and 2.6.

## 1. ITL-1427: a bound job carries a stale hash

### 1.1 Measured

| Case | Slot's hash | Nodes below |
|---|---|---|
| Run over A, `bind` B (before and after the next run, sync and async) | B's | **A's**, all four nodes |
| `slot.bound_source = B` (no `bind`) | B's | **A's** |
| `bind(sources=..., database=other)` in one call | B's | B's, but the side-effect node keeps **A's** |
| `from_pipeline(p, database=db)`, a node's hash read while unbound, then `bind` | right | the **unbound placeholder's**, whichever source is bound |
| the same, no hash read before `bind` | right | right, but the side-effect node keeps the **placeholder's** |
| Bind back to A | A's | A's |

Consequences measured:

- **Stored rows of two bindings cannot be told apart.** After A then B, all four rows of
  each function's pipeline table carry A's `__node_content_hash`; the stored view of the first function
  shows `s1, s2, s3, s4`.
- **Two jobs of one blueprint share one hash**: over one database, the job bound to B shows
  A's rows too.
- **A replayed operator returns wrong rows.** An operator that logged A, was rebound to B,
  logged B and then replayed (cache mode set through a private attribute, because attaching
  a database hides the defect) returned the rows of A and B.
- **Downstream provenance.** The default id of `node.as_source()` is built from the node's
  hash, so sources of two bindings have one id.
- **A saved job contradicts itself.** Saved after a bind without a run, it records the new
  source under `bindings.sources` and the old pipeline's hashes under
  `run.node_job_content_hashes`.
- **The run URI** (`<job>@<hash of the sink nodes' hashes>`, in status records) does not
  change with the binding.

Not affected: pipeline hashes, stored paths, result reuse (by input row hash), the
side-effect log (keyed by pipeline hash and record id), and the rows a job shows after a run.

### 1.2 Cause

`ContentIdentifiableBase.content_hash()` memoizes per object. `SourceJobNode._bind` clears
the slot's own memo only. The nodes below hold a reference to the slot, not to its hash,
and nothing clears theirs. `attach_databases` of function and operator nodes happens to
clear them, which is why the answer depends on the order of calls and why side-effect
nodes, whose attach does not, are stale in more cases. A node built at compile has a memo
from the start: the compile puts nodes in a hash-keyed DAG.

`PipelineJob.bind` already loops over every node to forget its output schema. Clearing the
hash in that loop is not enough: it misses the `bound_source` setter, streams and nodes
outside the job built over one of its nodes, and it is a second list to keep complete.

### 1.3 Rule

> A node's content hash is always the hash of the pipeline it stands in, as bound now:
> equal to the hash of the same pipeline built directly over the bound sources. When it
> was read before, how the source was bound and whether a database was attached never
> change it. A bind never changes a pipeline hash.

### 1.4 Mechanism (D1)

One module-level counter, the *binding epoch*, increased after every change of what a hash
covers: `SourceJobNode._bind` (bind, setter, unbind), a slot's `_resolve`, and the
`upstreams` setters. A memoized content hash of an object **with upstreams** records the
epoch read before it was computed, and is used only while the epoch is the same. An object
without upstreams (a root source, a raw stream, a pod) keeps its memo: its identity does not
depend on a binding, and a raw stream's hash costs a pass over its rows. A slot without
upstreams clears its own memo when bound, as today. The path memo of a node whose table scope
is `content_hash` follows the same epoch (1.8).

- Reading a current hash stays O(1) (a function node reads it once per stored row). After
  a bind each node recomputes its hash once, so a diamond costs its nodes, not its paths.
- The epoch is taken before computing, and increased after the binding changes, so a hash
  computed during a bind on another thread is never kept as current. An unrelated bind
  costs a recomputation and never a wrong value.
- Nothing keeps a list of dependents.

Alternatives: clear every node in `bind` (misses the cases of 1.2); check the upstreams'
hashes on every read (a walk per read, per path in a diamond); weak references from a slot
to its dependents (a second graph to maintain).

Simulated: every case of 1.1 gives the hash of the directly built pipeline, for function,
operator and side-effect nodes, sync and async; stored rows carry the hash of their
binding; the replay returns B's rows. All of `tests/test_pipeline` and the async/sync
equivalence tests pass (7332 passed, 6 skipped, 116 expected failures), and the golden
tests pass (2577). The loaded job needs 1.6.

### 1.5 What becomes visible once the hash is right (D2, D3)

**The DAG.** `OrcaDAG` is a dict keyed by nodes, whose `__hash__` and `__eq__` are content
identity. Today only the rebound slot is lost from `job.dag` (ITL-1204); with right hashes
nodes below it are lost too (simulated: both function nodes and the side-effect node; the
operator node was still found by a lookup that happened to land on its old place, as a dict
probe can). The library looks up no node in
`job.dag` after a bind, but `dag.predecessors(node)` would raise for user code. D1 must not
land without ITL-1204. Recommended: job nodes are keyed by object in every DAG and
per-run dict (`job.dag`, the run's graph, `node_run.plan_run`, both orchestrators). This is a
key of those containers; `__eq__` and `__hash__` of the node classes need not change.

**Two slots bound to one source** (ITL-1442). A blueprint with two slots, each feeding the
same function under its own label; one source bound to both:

| | Nodes run | Rows held (r1, r2) | Entries stored for 2 input rows |
|---|---|---|---|
| main, hashes read before the bind | 2 | 2, 2 | 4 (two different stale hashes) |
| main, hashes not read before | **1** | 2, **0** | 2 |
| D1 only (simulated) | **1** | 2, **0** | 2 |
| D1 + object-keyed DAG, sync (simulated) | 2 | 2, 2 | 2 |
| D1 + object-keyed DAG, async (simulated) | 2 | 2, 2 | **4**, and each stored view shows **4** rows |

Two equal nodes are one computation, stored under one key. A recorded pipeline never has
them: identical invocations merge when recorded. Only a bind can make them. Options:

- **(a) Refuse** (recommended for now): `bind`, before anything changes, and the start of a
  run (for the setter) refuse when two slots of a job have bound sources of one content
  hash. The error names the slots and the source and says to build the pipeline over the
  source once. In the `tests/test_pipeline` files that bind (1999 tests), no test makes such
  a bind.
- (b) Run one of the equal nodes and let all of them hold its run. The right semantics if
  binding one table as, for example, both "train" and "test" is wanted; touches the state
  rule, held rows, the run summary and both orchestrators.
- (c) Run both and deduplicate on write. The async run has no order between them, so each
  store would need an atomic check.

(a) can later be relaxed to (b) without changing a hash. A slot's content hash cannot tell
the two slots apart, since a bound slot's identity is its source's.

### 1.6 A loaded job (D4)

Measured: a loaded job's nodes return the hash the file records, whatever is bound. Bound
to B, its nodes still answer with A's hashes and its stored views show A's rows, although
the slot shows B.

The issue asks that the hash follow a bind in a loaded job too. A loaded function node has no
pod, so it cannot recompute from a pod. It does not need one: a node's hash is the hash of
its pod's content hash and its upstreams' content hashes, in the pod's argument symmetry
(checked for function, operator and side-effect nodes: equal to the node's own hash).

Rule: **loading moves nothing; a bind by the user does.**

- A slot of a loaded job answers, below it, with its *saved run hash* until the user binds
  a source (a source rebuilt from the file does not count: a file that was moved, or a
  kind whose version rose, must not make the stored results unreadable).
- When the user binds a source to a slot, the nodes below it answer with the hash computed
  from their saved pod hash and their upstreams. Binding the saved source gives the saved hash.
- A stored view then shows the rows stored for that binding; none if there are none.
- A load that computes a hash different from the saved one for a node with nothing rebound
  refuses the file as inconsistent.

If this is too much for 0.4.0: keep today's behaviour (a loaded job answers for its saved
run) and say so in the docs; D1 and D4 are independent, but D4 changes the saved file, so it
is cheaper before 0.4.0. Not recommended: refusing every bind on a loaded job, which the
docs use to read a source node.

### 1.7 A source made before a bind (D5)

`d = node.as_source()` takes its id and content hash from the node when it is made and reads
the node's stored rows when first read. Measured, made before a bind and first read after
the next run: today it shows A's and B's rows; with D1 it shows **B's rows under the id
made from A's hash**. Recommended: a plain `DerivedSource` keeps the hash it was made from and
raises on the first read (or re-read) when its origin's hash differs, with the remedy (call `as_source()`
again). One that already read its rows keeps them. `DerivedSource` is provisional (ITL-1317),
so its identity could change; this proposal does not change it.

### 1.8 What moves

| | Moves? |
|---|---|
| Golden values | **None** |
| Pipeline hash, schema, stored path (default table scope) | No |
| Any hash of a job recorded over its sources and never rebound | No |
| Content hash of nodes below a rebound slot | Yes: to the hash of the directly built pipeline, the value already used everywhere else |
| `__node_content_hash` of rows stored after a bind; operator cache record hashes after a bind (they cover it) | Yes, as above |
| Default source id and content hash of `node.as_source()` after a bind, and what is downstream of it | Yes, as above (provisional) |
| Run URI after a bind | Yes |
| Path of a node built by hand with `table_scope="content_hash"` | Yes, after a bind: that scope's path names the instance. A job never uses that scope, so ITL-1112's rule holds for jobs; its wording should name the default scope |

### 1.9 Saved files and stores

Saved job files, D1 alone: no format change. `run.node_job_content_hashes` holds the hashes
of the current binding. A job saved after a bind without a run is loaded with views that
show what is stored for the new binding; its `run.status` still says `complete` although
that binding was never run (recommended: `pending` when no node holds a current run).

With D4: `run.slot_content_hashes` (slot name to the hash of the source bound when saved, or
`null` if unbound), and `pod_content_hash` on each function, operator and side-effect descriptor.
Required in a file of the new format; no backward reading, as decided on 2026-11-21. The
format version is 0.4.0 and unreleased: the fields can be added within it, or the version
raised to separate files of earlier development builds.

Rows already stored under a stale hash stay as they are. A file saved after a stale run
names that stale hash and goes on reading the same rows. Measured: a correct job over B on a
store where B's rows carry A's hash calls no function again, delivers no side effect again,
stores B's entries once more under B's hash (the results are reused) and reads exactly B's
rows. The stored view of A keeps showing B's old rows. They can be found (their source-id
system tag names B) but are not repaired by Orcapod; a tool for it belongs to ITL-1048. An
operator cache written under a stale hash should be deleted.

### 1.10 Tests

Two tags, two data columns; sources with and without key columns; the chain of 1.1 plus a
join of two slots, with the slots bound in both orders and the sources mapping in both orders.

1. For every kind of node: hash after a bind equals the directly built job's, for each of
   `bind`, setter, `from_pipeline(sources=)`, `bind` with and without a database, a
   hash read before and not; A, B, A; unbind; a diamond; two slots, one rebound (a node
   below the other keeps its hash).
2. The same after `run`, `run_async`, `node.run()` and a load-only run; stored hashes per
   binding compared as sets of rows on the async path, exactly on the sync path.
3. Stored views, `as_source()` ids, operator `LOG` then `REPLAY`, run URI; pipeline hash and
   the list of table paths unchanged.
4. A stream and a second job built over the first job's node follow its bind.
5. Every node in `job.dag` after every kind of bind (ITL-1204).
6. Two slots, one source: refused, the job unchanged; by `bind` (in one call and two), setter
   and `from_pipeline`; different sources of one schema accepted (ITL-1442).
7. Loaded: no bind, same source, another source, back; CSV (rebuilt) and in-memory (not);
   save, load, save again keeps the file.
8. A source made before a bind: read before, first read after.
9. Cost: one hash computation per node per bind on a deep diamond; none between binds.
10. A hash computed while another thread binds is not kept as current.

## 2. ITL-1428: name a slot by its source's label

### 2.1 Measured

- A slot's name is the source's name; `label=` is ignored (`source_id="first",
  label="subjects"` gives the slot `first`).
- The bind key is the slot's name; the key in `job.nodes` is its display label, which the
  compile can suffix: a source named `ratio` beside a pod labelled `ratio` is bound as
  `ratio` and listed as `ratio_1`. Labels will make this more common.
- **One source in two objects with two labels is one slot** (slots are keyed by content
  hash). Simulating label naming on today's code, the first recorded label wins: `train`,
  or `test` when the order is swapped. Order would count (ITL-1271).
- Two sources with one label, and a label equal to another source's name, are refused by
  the existing check (`SourceSlotNameCollisionError`); its message speaks of source names only.
- **The issue's statement on saved files is too narrow.** It says the name-dependent
  hash is written only in a file saved while a slot is unbound. A job's file always embeds
  the blueprint, so the node keys, edges and descriptors' `content_hash` of a bound and completed
  job are name-dependent too. They are used only inside the file.
- **Node labels can swap.** Nodes that share a label get `_1`, `_2` in the order of their
  blueprint hash, which holds the slot's name. Renaming one of two slots swapped `r_1` and
  `r_2` in 4 of 30 tries. That is a key of `job.nodes` for a `Pipeline` and a job made from
  it; a job recorded over its sources is not affected.
- `CachedSource` does not take its wrapped source's label, but takes its name. `to_config`
  of a source does not save its label.
- `trace`/`why_missing` accept a source node's label, its source's name or a full id, and treat
  the node's label as an id its rows may carry ("a slot is named by its source's name"),
  which a role name is not.

### 2.2 What a slot name is

The name of a role in the pipeline, fixed when the pipeline is compiled. A later bind
changes the source, never the name. Today's default makes it the name of the first data.

### 2.3 Rule (D6)

1. An explicit `name=` (`SourceNode(name, ...)`, `from_stream(..., name=)`).
2. A source's **assigned, non-empty** label (`label=` or the setter). An inferred label (the
   class name) never counts.
3. A source's name, as today.
4. Any other stream: `"<label>:<hash>"`, as today.

One helper computes it, used by `SourceNode.from_stream` and `SourceJobNode.from_stream`
(two copies today). The label is read at compile; setting it later renames nothing. `bind`
accepts the slot name only, no source-name alias: two keys for one slot would be ambiguous,
and the unknown-key error already lists the known names. `bind` does not look at the label
of the source it binds.

`RunSummary.conditions`, `limits` and the diagram, which use the slot's name, follow. Reports
of a source read outside a slot keep its source id.

### 2.4 Uniqueness (D7)

- Two sources, one slot name: refused as today; the message gives, per source, its full id
  and where the name came from (label or source name).
- One source (one content hash) under two assigned labels in one recording: **refused**, at
  compile, naming both labels and the source. All recorded inputs are looked at, not
  only the first. One labelled and one unlabelled use: the label, in either order.
- The alternative for one source under two labels is two slots. It needs the graph keyed
  by role rather than source identity, gives equal nodes while both are bound to that
  source (1.5), and is the same question as D2(b). To be reconsidered with it.
- Suffixes: nodes sharing a label should be ordered by pipeline hash first, then by a
  name-free key. Nodes of one schema tie on the pipeline hash, so this does not remove
  the dependence on the slot name for them; with explicit, distinct pod labels nothing is
  suffixed. Recommended: leave it, and say it in the changelog.

### 2.5 Provenance (changes with the rule)

`_static_source_ids` no longer counts a slot's label as an id its rows carry; the id built
from the stored source name remains. `trace(source=...)` keeps label, source name and full
id; a string that is one slot's label and another's source name is refused as ambiguous, as
the existing rule does. `SourceProxy`, which presents a full saved id, is unchanged.

### 2.6 What moves

| | Moves? |
|---|---|
| Golden values | **None** (golden tests pass with the rule simulated) |
| Anything for a source without an assigned label | No |
| Pipeline hash, stored path, system-tag column, source id, record id | No |
| Content hash of a bound slot and of nodes below it; stored `__node_content_hash`; result reuse | No (measured equal) |
| `slot` name in `job.nodes`, `job.sources`, `unbound_sources`, `bind`, summaries, diagram | Yes, for a labelled source |
| Content hash of an unbound slot and of blueprint nodes (`Pipeline`), and the node keys of saved files | Yes, for a labelled source; used inside one file only |
| `_1`/`_2` suffixes in a `Pipeline` and in jobs made from it | Possibly, where nodes share a label and an input is labelled |
| `SideEffect` deliveries and log keys | No |

### 2.7 Saved files

No format change. The file stores the slot's name; a load uses it as written, so a file saved
before keeps its old slot names and its `bind` keys, and goes on reading its rows. A pipeline
rebuilt from code gets the new names. There is nothing to migrate in the file. Code that binds
by the source name of a **labelled** source changes its key once; the error lists the names.
Simulated: a labelled job saved, loaded and read gives the slot `subjects`, the key `subjects`
under `bindings.sources` and the same stored rows.

Optionally, save a source's label in its config, so a rebuilt source keeps it. The slot's name
does not need it.

### 2.8 Tests

1. Precedence: name, label, source name; empty label; label by setter before and after
   compile; inferred label; `Pipeline` and `PipelineJob`.
2. Every source kind with and without a label; a `CachedSource` with its own label, over a
   labelled source, over an unlabelled one; a restricted copy (`where`) keeps the label.
3. Two and three sources, each recording order: collisions (label/label, label/name, name/name);
   one source under two labels, under a label and none.
4. `bind` by label, with several slots in both mapping orders; by the old name (refused, lists names);
   a bound source with another label.
5. Labelled against unlabelled: every hash, path, stored row and reuse count of a bound job equal.
6. Save, load, bind, save again; a file written with source-name slots.
7. Provenance by label, source name, full id; a label equal to another slot's source name.
8. A slot and a pod with one label: bind key and `job.nodes` key.
9. Docs samples in `concepts/pipeline-job.md`, `concepts/sources.md`, `reference/hashing.md`.

## 3. Order of work

1. ITL-1204, D2's refusal (ITL-1442) and D1 in one PR: none of the three is correct alone.
   D5 with it.
2. D4 (saved files), before 0.4.0 if it is wanted.
3. ITL-1428, independent of the others apart from D7.

## Appendix: short reproductions

```text
import pyarrow as pa
from orcapod import Pipeline, PipelineJob, function_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.sources import ArrowTableSource

SCHEMA = pa.schema([
    pa.field("subject", pa.large_string(), nullable=False),
    pa.field("session", pa.int64(), nullable=False),
    pa.field("weight", pa.float64(), nullable=False),
    pa.field("count", pa.int64(), nullable=False),
])

def source(name, rows, label=None):
    return ArrowTableSource(
        pa.Table.from_pylist(rows, schema=SCHEMA), tag_columns=["subject", "session"],
        record_id_columns=["subject", "session"], tag_only_columns=True,
        source_id=name, label=label)

A = [{"subject": "s1", "session": 1, "weight": 25.5, "count": 3}]
B = [{"subject": "s3", "session": 2, "weight": 30.0, "count": 6}]

@function_pod(output_keys=["ratio", "twice"])
def ratio(weight: float, count: int) -> tuple[float, int]:
    return weight / count, 2 * count

# ITL-1427
job = PipelineJob(name="j", database=InMemoryArrowDatabase())
with job:
    ratio.pod(source("first", A), label="ratio")
job.run()
before = job.nodes["ratio"].content_hash()
job.bind(sources={"first": source("second", B)})
job.nodes["ratio"].content_hash() == before          # True; should be False
job.run()
job.nodes["ratio"].as_source().as_table().num_rows   # 2; should be 1

# ITL-1442
p = Pipeline(name="p")
with p:
    ratio.pod(source("first", A), label="r1")
    ratio.pod(source("second", B), label="r2")
job = PipelineJob.from_pipeline(p, database=InMemoryArrowDatabase())
one = source("third", A)
job.bind(sources={"first": one, "second": one})
job.run()
[job.nodes[n].as_table().num_rows for n in ("r1", "r2")]   # [1, 0]

# ITL-1428
job = PipelineJob(name="j", database=InMemoryArrowDatabase())
with job:
    ratio.pod(source("first", A, label="subjects"), label="ratio")
list(job.sources)   # ['first']; proposed ['subjects']
```
