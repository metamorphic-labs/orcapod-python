# Build a pipeline job from streams made while exploring (ITL-1387)

Status: APPROVED TO PROCEED (developer, 2026-11-22), on the decisions D4 and D7 as revised
and rule 2.1. D1-D3, D5, D6, D8, D9, D13 and D14 are built as recommended, not individually
confirmed (section 11). Measured on `origin/main` fc1df420 (2026-11-22).

Developer's words (2026-11-22): "we can build a feature whereby we can register one or more
streams against a pipeline job and it will create a full pipeline based off of the passed on
streams + all of its upstreams up till and including sources. That can simplify the
exploration -> pipeline development process." Also: "there should be a point between
separating exploration code from proper executable research code so the need to redefine all
steps plainly in a pipeline_job is actually acceptable", and reopening `with pipeline_job:` to
add steps over the nodes so far should still work. Nice to have for the workshop
(2026-11-25), not a blocker.

**Changed in revision 2** (developer, 2026-11-22):

- **D7 reversed.** "Every node in a pipeline simply captures some basic orcapod pipeline
  underlying it so there is no reason that one pipeline cannot absorb/share part of the
  pipeline as another. Generally, if any stream/node is to be added to a pipeline (e.g. by
  register) it ought to add that pod as well as all of its upstream operations up to and
  including source." A node of another job is now walked like any stream (section 5.1),
  with the property proven on it. What the two jobs share and do not share is stated with
  its condition: tables live under each job's OWN database and job name (section 5.1). A
  node of a LOADED job is answered with a script, function importable and not (section 5.2).
- **One rule for both ways in** (section 2.1): a stream is treated as a source only when the
  user says so (`as_source()`, or an explicit source object). A `with` block over a
  pre-built stream takes in its upstreams exactly as `register` does. On the
  `<label>:<hash>` slot the developer asked: "that's only if you are treating that stream as
  source, no?" Then (2026-11-22): "yes move D4 into first version. A stream used in pipeline
  should try to use it as is as part of a pipeline, without auto-invoking as_source. In fact
  treating a stream as source is something that a user must explicitly do to intentionally
  cut pipelines into parts." Section 9 is the plan with this in the first version: what
  replaces the implicit leaf, the refused-block residue, every existing test that pins the
  implicit leaf, old saved jobs and the docs on cutting a pipeline.
- D11 and D12 (pre-existing naming issues) are filed as **ITL-1389**.

Evidence. Every claim below about today's behaviour is backed by a script run against
fc1df420. The scripts are kept with the ITL-1387 working notes, outside the repository:

| Script | What it shows |
|---|---|
| `reopen_job.py` | reopening `with job:` adds a step over `job.doubled`; it runs |
| `reopen_renumbers.py` | a second unlabelled step of one kind renames the first (`MapData` to `MapData_1`) and every node object is rebuilt |
| `prebuilt_stream.py` | what a job records when a step is built over a stream made before the job (F07); a refused join stays recorded (an empty second `with` block is refused again) |
| `same_step_two_labels.py` | one step recorded under two labels: the last recorded wins |
| `prototype_register.py` | a prototype of the walk, OUTSIDE the library, and the property checked on it |
| `edge_cases.py` | raw-stream roots, side-effect pods, another job's nodes under the prototype walk |
| `walk.py` (rev 2) | the prototype walk used by the rev-2 scripts: it classifies a step by its producer, so job nodes are walked |
| `absorb_other_job.py` (rev 2) | a job absorbs another job's nodes: equal to the hand-written job; when the two see each other's stored results |
| `loaded_job_walk.py` (rev 2) | nodes of a loaded job, function importable and not: what has a producer, what the walk does |
| `leaf_probe_plugin.py` (rev 2) | a pytest plugin, run once over `tests/` (one process, before the implementation started), listing every test that compiles a stream with a producer as a leaf or calls `from_stream` on one |

## 1. What exists today

### 1.1 Recording

A `PipelineJob` is a tracker. While a `with job:` block is open, every pod call
(`FunctionPod.process`, `StaticOutputPod.process` for operators, `SideEffectPod.process`,
`CachedFunctionPod.process`, `FallThroughPod.process`) calls the tracker manager, which calls
`AbstractPipelineBase._record_invocation` with a `FunctionInvocation`, `OperatorInvocation`
or `SideEffectInvocation` holding the pod, the exact input streams and the `label` argument.
The invocation is keyed by its content hash (`_invocation_lut`); each input stream whose hash
is not a recorded invocation is kept as a leaf (`_source_streams`). `_record_invocation`
first checks the ITL-1218 rule (`require_one_restriction` over every held stream and the new
inputs). On exit, `compile()` rebuilds the whole DAG from scratch: each leaf becomes a
`SourceJobNode.from_stream(leaf)` (binding it through the `SourceNodeBase.validate` check),
each invocation a `FunctionJobNode` / `OperatorJobNode` / `SideEffectJobNode`, and labels are
disambiguated: a label held by several nodes becomes `<label>_1`, `<label>_2`, ... in
content-hash order.

The automatic join of a multi-input function pod call and the automatic select of ITL-807
are ordinary operator calls (`Join`, `SelectDataColumns`), so they are recorded like any
other step.

### 1.2 Reopening works

`reopen_job.py`: a first `with job:` records `double.pod(src, label="doubled")`; after
`job.run()`, a second `with job:` records `plus.pod(job.doubled, label="added")`. The job then
has `['added', 'doubled', 's']` and runs. It works because a job node's content hash equals
its invocation's key, so `job.doubled` is recognised as a held step and not taken as a leaf.

Two properties of reopening that this design inherits (`reopen_renumbers.py`):

- `compile()` rebuilds every node object. A node object taken from the job before reopening
  is not the node the job holds afterward.
- Disambiguation suffixes are given at every compile: a job with one unlabelled `MapData`
  that gains a second one has `MapData_1` and `MapData_2`, and the first is no longer called
  `MapData`. The names never depend on recording order (content-hash order), but they are not
  stable as the job grows. Labels enter no hash and no stored path; they are the keys of
  `job.nodes`, of attribute access and of a saved job.

### 1.3 A stream made before the job (finding F07), reproduced

`prebuilt_stream.py` builds, with no job active, `explored = double.pod(a, label="added")`
over an `ArrowTableSource` `a` (source id `a`), then records one step over it:
`with job: plus.pod(explored, label="plussed")`. Today the job records:

- **Nodes**: `['added:e20e5cdd...a4bd', 'plussed']`. The pre-built stream is one LEAF: a
  `SourceJobNode` named `<stream label>:<full content-hash hex>`
  (`SourceNode.from_stream`, case 3), bound to the `FunctionPodStream` itself
  (`job.sources == {'added:e20e...': FunctionPodStream}`). The source `a` is not a node; the
  step `double` is not a node.
- **Hashes**: the content hash of `plussed` equals that of the same pipeline written by hand
  (the slot's content hash delegates to the bound stream). The pipeline hash differs (the
  slot's pipeline hash is its schema only).
- **Stored**: after `job.run()`, only `plus` has tables: its result table (same path as by
  hand) and its pipeline table under a different `schema:<pipeline hash>` component than by
  hand. `double` is computed in memory on every run and never stored.
- **Saved**: `job.save()` writes the slot with `source_config.source_type == "node"`; the
  bound `FunctionPodStream` is not saved.
- **Loaded**: `PipelineJob.load` warns that the source `added:e20e...` "(unknown)" cannot be
  rebuilt; the loaded job lists `['added:e20e...', 'plussed']`, and `run(load_only=True)`
  gives zero rows for `plussed`.
- **Over a join**: the same experiment with `explored = add.pod(a, b, label="added")` (an
  automatic join of two sources) is REFUSED at the end of the `with` block:
  `SourceSpecMismatchError` ("a joined or merged stream cannot be bound ... ITL-1127"). The
  job then has no nodes, but the refused invocation stays recorded, so the next compile of
  that job fails again.

The by-hand pipeline (`with job: plus.pod(double.pod(a, label="added"), label="plussed")`)
has `['a', 'added', 'plussed']`, stores both steps, saves and loads.

So today the only way to turn exploration into a job is to write every step again inside the
`with` block. A step over a pre-built stream gives a job that looks right (its last hash is
right) but stores less, hashes differently upstream, cannot be saved usefully, and is refused
over a join.

### 1.4 Other measured facts

- `same_step_two_labels.py`: one step (same content hash) recorded twice in one `with` block
  as `label="x_label"` then `label="y_label"` gives one node named `y_label`; in the other
  order, `x_label`. The name follows recording order (the later invocation overwrites the
  earlier in `_invocation_lut`). This is a pre-existing departure from "order never counts",
  filed as ITL-1389.
- `edge_cases.py`: a step over a raw `ArrowTableStream` is refused by the bind check
  (`SourceSpecMismatchError`), in a `with` block and under the prototype walk alike.
- `edge_cases.py`: a node of ANOTHER job used in a `with` block today becomes a leaf named
  `doubled:<hash>` bound to that job node, the F07 shape again; when that node lies over a
  join, the `with` block is refused (`absorb_other_job.py`: `SourceSpecMismatchError`).

### 1.5 The walk is enough (prototype)

Every stream that is not a root carries what the tracker records: `stream.producer` (the
pod), `stream.upstreams` (the exact inputs the pod was called with) and the label it was
given (`has_assigned_label`, `label`). `prototype_register.py` walks each registered stream's
producers and upstreams back to the roots, depth first, and records each step with the job's
own `_record_invocation`, stopping at a root (`RootSource`, or a stream with no producer) and
at a step the job already holds, then calls `compile()`. On a pipeline with two sources, an
automatic join, an automatic select (ITL-807), labelled and unlabelled steps, a `map_data`
chain and a final join of two branches (10 nodes), registering the two end streams, in both
orders, against writing the pipeline in a `with` block gives, sync and async:

- the same node names (`Join_1`, `Join_2`, `MapData`, `SelectDataColumns_1`,
  `SelectDataColumns_2`, `a`, `added`, `b`, `double`, `renamed`);
- per node, the same node type, content hash and pipeline hash;
- the same stored table paths and the same rows in every node after `run()` / `run_async()`;
- the same saved `pipeline` and `bindings` sections (`job.save()`; only the `run` section
  differs, by run id and times);
- a loaded registered job with the by-hand node names, whose stored views read the rows;
- no user function called and nothing stored by registering.

Registering a stream the job already holds changes nothing; registering a stream built
(outside any `with` block) over a held node adds only the new steps. The design below is that
walk, made safe.

## 2. The call

**Recommendation**: one method, `job.register(*streams, **named_streams) -> None`, on
`AbstractPipelineBase` (so `Pipeline` and `PipelineJob` both have it).

```python
# exploration, no job anywhere
cleaned = clean.pod(raw_a, raw_b, label="cleaned")
scores = cleaned.join(meta).map_data({"v": "value"})
summary = summarize.pod(scores)

# the job: one call, both end streams; everything upstream comes along
job = op.PipelineJob(name="study", database=db)
job.register(summary, scores=scores)
job.run()
```

- **Several streams** in one call. Positional streams keep the labels their steps were given;
  a keyword names that stream's node (section 3).
- **Outside or inside a `with` block.** Outside, `register` compiles the job at once (as the
  end of a `with` block does; `auto_compile=False` respected). Inside, it records and the
  block's exit compiles. The same steps are recorded either way.
- **Meets reopening**: `register` and `with job:` add to the same recorded state, in any mix
  and any number of times. A stream built outside any block over a job node
  (`plus.pod(job.doubled)`) registers only its new steps.
- **All or nothing**: a refused `register` leaves the job exactly as it was (recorded state,
  compiled nodes, bindings), by planning and checking everything before recording anything
  (section 9). A `with` block gets the same property (section 2.2).
- Returns `None`, like `bind`; the nodes are read from `job.nodes` / `job.<name>`.

Alternatives weighed:

- `PipelineJob.from_streams(*streams, name=, database=, **named)`: a constructor gives one
  shot and does not meet reopening. It can be a two-line wrapper over `register` later; not
  in the first version (D3).
- `job.add(...)`: "add" reads as adding one node; `register` is the developer's word.
- **No new call, only the `with` block walking pre-built streams.** Decided as part of the
  first version (section 2.1), but not instead of `register`: `register` is the way in when
  the end streams already exist and no new step is to be written, and it takes keyword names.
- A `Pipeline`-only call (blueprint) then `from_pipeline`: possible with the same method on
  the base class; no separate API needed.

### 2.1 One rule for both ways in (decided, revision 2)

Developer (2026-11-22): "A stream used in pipeline should try to use it as is as part of a
pipeline, without auto-invoking as_source. In fact treating a stream as source is something
that a user must explicitly do to intentionally cut pipelines into parts."

> **A stream given to a pipeline or job, by `register` or as an input of a step recorded in
> a `with` block, comes in with its pod and every upstream step down to its sources. A
> stream becomes a source only by the user's explicit act: a source object (any
> `RootSource`), or a node's `as_source()`.**

So the `<label>:<hash>` slot of 1.3 disappears: nothing turns a stream made by a pod into a
source slot implicitly. Both ways in call one function, the walk of section 9, from
`_record_invocation` (for each input of a recorded step that has a producer and is not held)
and from `register`. A stream built inside the block is already held, so the walk stops at
once and nothing changes for code that builds everything inside the block.

What counts as a source (a leaf): a `RootSource` (every source class, `CachedSource`,
`DerivedSource` including a node's `as_source()`, `PollingSource`) and a `SourceJobNode` or
`SourceNode` (a slot of any pipeline or job). Everything else with a producer is walked. A
function node with no producer (a node of a loaded job) is refused when it is recorded, by
name, with its remedies (section 5.2). As built: a raw `ArrowTableStream` (no producer, not
a source) stays a leaf and is refused by the bind check when the job compiles, with today's
message and exception (`SourceSpecMismatchError`); moving that refusal to recording time
would change a tested exception for no gain, since a refused block now restores its state
(2.2).

### 2.2 A refused `with` block leaves the job as it was

Today a refused block leaves its invocations recorded, so the next compile of that job fails
again (1.3, `prebuilt_stream.py`: an empty second `with` block is refused). With the walk at
recording time, a stream that cannot be bound is refused when the step over it is recorded,
before anything of that step is recorded. A refusal can still come later (in `compile()`: two
sources with one name and different schemas, a restriction conflict through a slot rebound
meanwhile). Recommendation: `__enter__` keeps a copy of the recording state
(`_invocation_lut`, `_source_streams`, `_hash_graph`; plain dicts and an `nx.DiGraph`), and
when the block's body or its `compile()` raises, `__exit__` restores it, so a refused block
leaves the job exactly as before the block (its compiled nodes untouched, since compile
replaces them only on success). `register` uses the same restore.

## 3. Node names

Names must be stable and never depend on the order of registration or of the streams in the
call (CLAUDE.md, ITL-1271).

**Recommendation**:

1. A step's node takes the label its step was given (`has_assigned_label`, read at
   registration), exactly what the `with` block records from the `label=` argument. The label
   is read from the stream; the stream is not changed.
2. An unlabelled step is not refused: it gets its computed label (the function name, the
   operator class name) and today's content-hash disambiguation (`Join_1`, `Join_2`). This is
   required by the property: the job must equal the by-hand job, which names them so. Proven
   order-free by the prototype (both orders give the same names).
3. A keyword name, `job.register(scores=stream)`, labels that stream's node `scores`; it
   overrides the step's own label for this job only (the stream keeps its label). Refused
   (`ValueError`, before anything is recorded):
   - a name some OTHER node of the job already has (assigned or computed), or a node of
     another step gets in the same call: an explicit name is a promise that `job.scores` is
     that stream, so it never takes a suffix;
   - one stream under two names, or a name for a step the job already holds under another
     assigned label;
   - a name for a root source: its default slot name is the source's `source_name`;
     set the source name with `source_id=`;
   - a name starting with `_` (attribute access refuses it).
4. Two steps with ONE assigned label (two different steps both `label="norm"`): accepted with
   today's suffixes (`norm_1`, `norm_2`, content-hash order), because the by-hand job does
   that. A warning is not added (it would differ from the `with` block).
5. One step (one content hash) reached under two DIFFERENT assigned labels (1.4: built twice
   as `label="x"` and `label="y"`): refused, naming both labels. A `with` block keeps the
   lexicographically smallest explicit label (ITL-1436).

The suffix renumbering of 1.2 (a node's suffix can change when the job grows) is inherited,
not fixed here (ITL-1389). An explicit keyword name is the remedy a user has today: named nodes
never take a suffix.

## 4. Shared upstreams and steps the job holds

**Identity: the invocation's content hash**, the key `_record_invocation` already uses
(the pod's identity plus its inputs' content hashes, so the sources' identities). Two
registered streams over one upstream step give ONE node, because the walk reaches the same
key twice and records it once. Two steps of one pod over different sources are two nodes
(different content hashes), as in the `with` block. The pipeline hash is not the identity: two
branches over two sources of one schema share a pipeline hash and must stay two nodes.

A stream (or an upstream of it) the job already holds is reached by its key and recorded no
more: registering it again is a no-op (prototype). A job node of THIS job is held by
definition, so a stream built over `job.doubled` registers only what is new.

## 5. Sources

**Every root reached becomes a leaf, and `compile()` makes it a `SourceJobNode` through the
existing bind check**, with nothing new: `SourceJobNode.from_stream` binds the root object
itself, so `SourceNodeBase.validate` (exactly one source-id and one record-id system-tag
column, declared types, Arrow types by `hash_schema`) decides. A raw `ArrowTableStream` root
is refused as today (`edge_cases.py`); so is any other stream with no producer that is not a
bindable source.

What is a root is stated once, in 2.1. The walk never goes beneath a root (a
`DerivedSource`'s origin and a `CachedSource`'s source are not steps of this job), as the
`with` block never records them.

### 5.1 A node of another job is walked (revision 2)

A function, operator or side-effect job node has a producer (its pod) and upstreams (the
other job's nodes), with the same identities as the steps that built it. So the walk takes
it like any stream: its step and every upstream step down to the other job's slots become
steps of THIS job. The other job's `SourceJobNode`s are roots: `compile()` copies each
(`SourceJobNode.from_stream`, case 1), keeping its name and its bound source. The walk
classifies a step by its producer (function pod, side-effect pod, operator), never by the
stream's class, since a job node is not a pod stream (`walk.py`).

Proven (`absorb_other_job.py`): job `other` holds two sources, an automatic join, an
automatic select, labelled and unlabelled steps and two `map_data` steps, and has run. A new
job is built from `final.pod(other.renamed.join(other.nodes["MapData"]), label="final")`, the
step written outside any block, then registered. Against the whole pipeline written by hand in
one `with` block, sync and async: same node names, node types, content and pipeline hashes,
stored table paths and rows. (Today the same step in a `with` block is refused, because
`other.renamed` lies over a join.)

What the two jobs then have in common, and what not:

- **Identities**: every absorbed step has the same content hash and pipeline hash in both
  jobs.
- **Stored tables: only under a condition.** An absorbed step's tables live under THIS job's
  own database and its path prefix (the job name), at the same path relative to that
  database: the path is derived from the step's identity (function, output schema, pipeline
  hash) under the job's prefix. So the two jobs see each other's stored results if and only if
  they use the same database location and the same job name. Measured: same database object
  and same name, the absorbing job's run calls only its own new step (`final`) and reads the
  other's stored results; same database but another job name, or same name but another
  database, every function runs again.
- **Per job**: node names (the other job had `Join`; the absorbing job, with a second join,
  has `Join_1` and `Join_2`), node objects (distinct; the other job's names and nodes are
  unchanged and its node still reads its own run), bindings (the slot copy holds the bound
  source at absorption; a later `other.bind(...)` changes only `other`), held runs and run
  summaries.
- **Stored results instead of steps**: a user who wants what the other job STORED, not its
  steps, cuts the pipeline explicitly with `other.nodes[name].as_source()` (a
  `DerivedSource`, a root). Limitation: `as_source()` of a node downstream of a join cannot
  yet be bound (ITL-1127), so such a cut is refused today.

### 5.2 A node of a loaded job (revision 2; overlaps ITL-1388)

`loaded_job_walk.py` saves a job (`CSVSource` -> function `double` -> `map_data`) on a Delta
database, then loads it in a new process, once with the function's module importable and once
without:

- A loaded **function node has no producer in both cases** (`producer is None`):
  `PipelineJob.load` (through `Pipeline.load`) always builds function nodes with
  `function_pod=None`, regardless of whether the function could be imported. There is nothing to
  walk.
- A loaded **operator node has a producer**: operators are rebuilt from their saved config.
  The walk records it and reaches the function node beneath it.
- The loaded **slot** (`CSVSource`) is rebuilt and bound.
- Today's walk (and today's `with` block) takes the producer-less function node as a leaf and
  the bind check refuses it with a misleading message ("a stream without system-tag columns (a
  raw ArrowTableStream) cannot be bound"), in both cases.

Decided (developer, 2026-11-22): "If the function is loadable the node and upstream chain
should be treated like just any stream whose upstream chain can be taken to construct pod and
nodes into the absorbing pipeline. If any of the step is unloadable, I agree that likely
should be errored out for now." Then revised (developer, 2026-11-22): when a step is not
loadable, "we should actually create the proxy node into the absorbing pipeline without
erroring but do throw a warning and make sure to use databases for the absorbing pipeline".
That target is designed and built under ITL-1407 (with attaching a loadable function when a
job is loaded): a proxy node in the absorbing job, a warning, and the absorbing job's
databases. What follows is the INTERIM behaviour of this PR, a clear and named refusal until
ITL-1407 lands. As built here: a loaded node that has a producer (an operator node) is walked like
any stream; a function node whose function is not attached is refused when it is recorded,
naming the step: its function is not attached (not loadable, or not loaded), and
`node.as_source()` gives its stored results. Once ITL-1407 attaches loadable functions, such
a node has a producer and the walk takes it with no change
(`test_a_loaded_operator_node_is_walked_and_an_unattached_function_refused`).

**Restrictions** (`where`, ITL-1218): a restricted source is a root like any other. Its
restriction travels in the bound source object, so it is carried, shown and saved exactly as
in a `with` block: no hash moves, `RunSummary.conditions` / `limits` /
`restricted_source_ids` report it per slot, and the saved job keeps `where` and `limit` in the
source config. The ITL-1218 rule needs nothing new: every invocation goes through
`_record_invocation`, which calls `require_one_restriction`; `register` checks it once over
the whole plan first (every held stream plus every stream of the plan), so a plan in which
one source is read under two restrictions (or with and without one) is refused before
anything is recorded. The refusal of a caching operator node over a restricted source
(`refuse_restricted_operator_cache`) applies unchanged at `bind`, database distribution and
run. (`where` exists only on `SpiralDBTableSource` today; this is read from the code, not
measured: no SpiralDB server in the evidence runs.)

**A `filter` step** (`PolarsFilter`, `DropNulls`, `SemiJoin`) is an operator: its stream has
a producer, so the walk records it like any operator, and it enters content and pipeline
hashes as in a `with` block. Nothing distinguishes it.

## 6. THE PROPERTY

> A job built by `register`, by a `with` block over pre-built streams, or by absorbing
> another job's nodes, in every registration order, has exactly the node names, node types,
> content hashes, pipeline hashes, stored table paths, stored tables, record ids, results and
> saved file (`pipeline` and `bindings` sections) of the same pipeline written by hand in one
> `with` block, on the sync and the async path. No golden value moves
> (`generate_golden_hashes.py --check`).

It holds by construction when the walk records the same invocations the tracker would:
same pod objects, same input stream objects (the stream's `upstreams` are the objects the pod
was called with), same labels. `compile()`, binding, database distribution, run, save and
load are the existing code, untouched. The prototype measured it (1.5).

How a test proves it (section 8 has the plan):

- A table of **builders**, each a function from fresh sources to one or more end streams,
  called twice: once inside `with hand:`, once with no job active followed by
  `reg.register(...)`. The builders cover every exported operator (completeness checked as
  `test_order_invariance.py` checks it: an operator without a builder fails the test), every
  function pod form (`pod(a)`, `pod(a, b)` with the automatic join, the automatic select of
  ITL-807, `pod.process`, `strict=True`, `CachedFunctionPod`, `FallThroughPod`, a pod with
  `ctx_arg`), a `SideEffectPod`, a `Rejoin` diamond, shared upstreams, labelled, unlabelled and
  duplicate-label steps, keyword names, and the several-item inputs of
  `tests/test_core/operators/several_items.py` (two or more tags and data columns, several
  arrangements).
- Each builder also runs in the two other ways in: built with no job active, then used as the
  inputs of a last step recorded in a `with` block (rule 2.1); and built in another job, run,
  and absorbed through that job's nodes (5.1).
- Each builder runs with its end streams registered in at least two orders and split over two
  `register` calls, and mixed with reopening (half in a `with` block, half registered).
- Compared, by one helper: `sorted(job.nodes)`; per label, node type, `content_hash()`,
  `pipeline_hash()`; `database.list_table_paths(())`; after `run()` and separately after
  `run_async()`, every stored table (rows sorted, record-id system tags included) and every
  node's `as_table()`; the saved file minus its `run` section; `PipelineJob.load` of each, its
  node names and each node's stored view.

## 7. What must NOT happen

- **Registering never computes, polls, reads a row or calls user code.** The walk reads only
  `producer`, `upstreams`, labels and `content_hash()` (identity, no rows); `compile()` reads
  each leaf's schema and typed zero-row Arrow schemas, which never compute (CLAUDE.md, "The
  Arrow schema is never computed"). This is the same work a `with` block does at the same
  points. Tests: functions that count calls (zero after `register`), a `PollingSource` whose
  implementation counts polls (zero), a `DerivedSource` over a job node with nothing stored
  (no error, no read).
- **The user's streams are not mutated.** The walk sets no label, no upstream and no cache on a
  stream; `compile()` builds new job nodes and sets labels only on those. Shared objects are
  shared exactly as in a `with` block (the pods, the bound sources). Tests: each registered
  stream's and each upstream's `label`, `has_assigned_label`, content and pipeline hash and
  `last_modified` are equal before and after; the stream still iterates to the same rows.
  Results the user's streams already computed in memory are not reused by the job: its nodes
  compute (or read the database) on `run()`.
- **Nothing is stored before `run()`**: `database.list_table_paths(())` is empty after
  `register` (prototype).

## 8. Function pods with context, side effects, and source kinds

| Kind | Handling | Why |
|---|---|---|
| Function pod with `ctx_arg` (invocation context) | handled | a `FunctionPodStream` like any other; the node's side-effect rules (stale tables, `SideEffectLogDataContextError`) apply at run, as by hand |
| `CachedFunctionPod`, `FallThroughPod` | handled | their streams subclass `FunctionPodStream`; recorded as function invocations of the same pod, as their `process` records them |
| `SideEffectPod` | handled, recommended (D8) | recorded as a `SideEffectInvocation`; registering delivers nothing, `job.run()` delivers as the by-hand job would (`edge_cases.py`: 2 rows delivered while exploring, 0 by registering, 2 by `job.run()`). Deliveries made while exploring are not in the invocation log (a standalone stream writes none), so `track_completion` cannot skip them. Alternative: refuse side-effect pods in the first version |
| `DerivedSource` (plain, `tags_as_data`) | handled, as a root | bound as a source; its origin is not walked; `ProvisionalHashWarning` as when built |
| `CachedSource` | handled, as a root | as by hand; a `CachedSource` over a restricted source is already refused at construction |
| `PollingSource` | handled, as a root | registering never polls (identity is `impl.identity()`); a run checks it as by hand |
| A node of another job | walked, with every upstream step (D7, revised) | section 5.1 |
| A function node of a loaded job | interim: refused by name while its function is not attached (section 5.2) | `as_source()` for its stored results; target (ITL-1407): a loadable function attached and walked, an unloadable step brought in as a proxy node with a warning, on the absorbing job's databases |
| An operator node of a loaded job | walked (its operator is rebuilt), down to what lies beneath | section 5.2 |
| A raw `ArrowTableStream` root, a joined stream as a root | refused, as today | the bind check; binding such a stream is ITL-1127 |
| A bare root source registered alone | refused (D9) | there is no step to register, and no by-hand equivalent (a `with` block records only steps); the message says to register a stream built from it |
| A stream whose producer is none of the three pod kinds | refused | the walk classifies by producer: a function pod, a side-effect pod or an operator; anything else has no invocation to record |

## 9. Implementation plan (D4 decided into the first version)

### 9.1 The walk

One function does the walk, beside the recording code in `pipeline/base.py`. `register`
calls it with the registered streams; `_record_invocation` calls it with the inputs of the
step being recorded (only inputs that have a producer and are not held; for a step built
inside the block that is none, so the block's cost does not change).

1. **Plan** (no mutation): walk depth first; collect, in topological order,
   `(key, invocation kind, pod, inputs, label)` for each step not held; collect leaves;
   classify each producer (function pod, side-effect pod, operator; anything else refused);
   refuse a stream that has no producer and is not a source (rule 2.1: a raw
   `ArrowTableStream`, a function node of a loaded job), by name with its remedy; apply the
   naming rules of section 3 against the job's current labels and the plan.
2. **Check** (no mutation): `require_one_restriction` over held streams plus every planned
   input; bind-check each new leaf (`SourceNodeBase.validate`, through the construction
   `compile()` uses, without keeping the node); `refuse_restricted_operator_cache` where a
   cache mode is already set.
3. **Record** each planned step, in plan order; keyword names set the invocation's label.
4. **Compile** (`register` outside a block, unless `auto_compile=False`; a block at its exit).
   If anything raises, the recording state is restored (2.2).

### 9.2 What replaces the implicit leaf (`from_stream`, case 3)

`SourceNode.from_stream` and `SourceJobNode.from_stream` have one caller in the library,
`compile()` (step 2). Under rule 2.1 every leaf `compile()` sees is a `RootSource` or a slot:
a stream with a producer has been walked when it was recorded, and a stream with neither has
been refused there. So `compile()` never reaches case 3.

As built, the implicit name is removed only for a stream that has a producer; a raw stream
(no producer) keeps it, since a `SourceNode` of one is a schema-only slot and a
`SourceJobNode` of one is refused by the bind check anyway
(`test_source_node_from_stream_slot_name_uses_full_hash` is unchanged).

Case 3 also has an explicit use: a user (or a test) builds a slot from a stream on purpose,
naming it, `SourceJobNode.from_stream(stream, name="pick")`
(`tests/test_core/sources/test_spiraldb_table_source_where.py` does), and binds streams to
slots with `job.bind(sources={...})`. Both are explicit acts of treating a stream as a
source, so they stay. Recommendation: keep case 3 for an explicit `name`, and remove only the
IMPLICIT name: without `name`, a stream that is neither a `RootSource` nor a slot is refused
(`ValueError`: "a stream made by a pod becomes a source slot only when you name it"). The
`<label>:<content hash>` slot name disappears, and with it the comment in `compile()` step 2a
about colliding `label:hash` names.

### 9.3 Existing tests and documentation that pin the implicit leaf

Found by running the whole test suite once with `leaf_probe_plugin.py` (a pytest plugin that
records every test in which `compile()` meets a leaf that has a producer, and every direct
`from_stream` call on such a stream), plus a search of `docs/` for streams built before a
`with` block. Each listed test changes in P1 (its expectation becomes the walked pipeline, or
the refusal moves to recording time):

The probe run (36,543 passed, 189 skipped because the environment lacked a PostgreSQL or SpiralDB server or an
optional package, 258 expected failures; the 8 failures, all in
`tests/test_core/test_tag_schema_order.py`, belong to the measured base fc1df420 and are
fixed on `main` by #499) found:

- **No test and no documentation sample compiles a pipeline or job over a stream that has a
  producer.** Every documentation page whose samples run (`tests/test_docs`) was in the run.
  So no existing test or sample relies on a pre-built stream becoming a slot inside
  `compile()`; the walk changes none of their results.
- **Direct `from_stream` calls without a name, which pin the implicit `<label>:<hash>` name**
  (they change, per D13: a stream that is neither a `RootSource` nor a slot needs an explicit
  `name`):
  - `tests/test_core/nodes/test_source_node.py::TestFromStreamNonRootSource::test_source_job_node_from_stream_slot_name_uses_full_hash`
    (a `map_data` stream): renamed `test_source_job_node_from_a_pod_stream_needs_a_name`; it
    now checks that the implicit name is refused and an explicit `name` is used as given.
  - `test_source_node_from_stream_slot_name_uses_full_hash` (a raw `ArrowTableStream`, no
    producer) is unchanged (9.2, as built).
- **Direct `from_stream` calls WITH a name** (an explicit cut; unchanged):
  `tests/test_core/sources/test_spiraldb_table_source_where.py::TestBindingTwoSlotsOverOneSource`,
  12 cases over `test_conflicting_restrictions_are_refused_and_nothing_changes`,
  `test_equivalent_restrictions_for_both_slots_are_accepted`,
  `test_one_slot_changed_away_from_the_other_is_refused` and
  `test_one_slot_changed_to_an_equal_restriction_is_accepted`.
- **Tests that hand a pre-built stream to a step in a block, found by reading** (not probe
  hits, because the stream never reaches `compile()` as a leaf):
  `test_spiraldb_table_source_where.py::...::test_a_stream_with_the_hash_of_a_captured_output_is_checked`
  (a pre-built stream over another restriction: refused today at recording by
  `require_one_restriction`; under the walk refused by the same check when the walk records
  its select step, so the expectation holds, with the docstring's reason updated) and
  `...::test_a_stream_with_the_hash_of_an_equal_captured_output_is_accepted` (its hash equals
  a held step, so the walk stops at once: unchanged).

### 9.4 A job saved under the old behaviour

A saved job whose slot came from a pre-built stream holds a slot with
`source_config.source_type == "node"` named `<label>:<hash>` (`prebuilt_stream.py`). That
form is not special: every slot of a saved `Pipeline` blueprint is saved as `"node"`, and the
loader keeps reading it. So such a file loads as it does today: an unbound slot of that name
and schema, its downstream nodes' stored results readable through their stored view
(`node.as_source()`; a loaded node's `as_table()` shows the current run, empty by design),
the slot readable after `job.bind(sources=...)` (checked: a file written on `main` read here). Nothing to migrate: nobody holds such a file (no users before the
workshop; v1 opens until 0.4.0), and P1 does not change the loader.

### 9.5 Documentation on cutting a pipeline

Proposed wording for `docs/concepts/pipeline-job.md` (a new section after the recording
stages), with a sample that builds the steps first and records them by using them in the
block and by `register`:

> **Streams built before the job.** A stream you built while exploring can be used in a
> `with` block, or given to `job.register(...)`: it comes into the job with its pod and every
> step above it, down to its sources, exactly as if you had written those steps inside the
> block. Nothing becomes a source unless you make it one.
>
> **Cutting a pipeline into parts.** To start a new pipeline from what another job stored,
> rather than from its steps, make that cut explicit: `job.nodes["bmi"].as_source()` is a
> source over the node's stored results, and a pipeline built on it begins there. A stream
> that is not a node (an exploration stream) becomes a source only when you build one from
> it, for example an `ArrowTableSource` over its table with a `source_id` you choose.

`docs/concepts/sources.md` ("Reading a job's results") gets one sentence linking to it, and
stage 2 of the job page ("Sources may be created inside or outside the block") gains "and so
may any other step".

### 9.6 Pieces, risk and estimates

| Piece | Content | Estimate |
|---|---|---|
| P1 | `register(*streams, **named)` on `AbstractPipelineBase`; the walk in `_record_invocation` and `register` (rule 2.1); the refusal at recording of producer-less non-sources; the restore of a refused block (2.2); `from_stream` case 3 removed (9.2); the tests of 9.3 changed; the property test (section 6) for `register`, for a `with` block over pre-built streams and for absorbed job nodes, sync and async, saved and loaded; the "must not happen" tests; docs (9.5); changelog fragment (`### Changed`: a stream used in a block is no longer a source slot) | 2.5 to 3 days |
| P2 | `PipelineJob.from_streams` wrapper (D3); a tutorial page on the exploration-to-job step | 0.5 day |

What D4 adds to the first version, in work: the walk's call from `_record_invocation`, the
block restore, the removal of case 3 and the changed tests (about one day over `register`
alone). In risk:

- **Code that used a pre-built stream in a block now gets more nodes.** The steps that were
  hidden inside a slot become nodes: they are stored on `run()` (new tables), named (and may
  renumber an existing node's suffix, ITL-1389), and their hashes upstream now equal the
  hand-written pipeline's. Results do not change; that is the point.
- **A pre-built side-effect stream** used as an input: today a slot bound to a standalone
  stream that delivers on every read; after, a side-effect job node that delivers when the job
  runs, under the job's invocation log. A fix, but a change in when effects happen.
- **Refusals move**: a pre-built stream over a join, refused today at compile with the
  ITL-1127 message, is now accepted (walked); a raw stream or a loaded function node is
  refused at recording, with a message that names it.
- **Restrictions become visible**: a caching operator over a restricted source that was hidden
  inside a slot is now a node, so `refuse_restricted_operator_cache` refuses it at run.
- **No hash, golden value or stored layout changes** for pipelines written inside the block;
  the probe shows which tests exercise the other case.

## 10. Test plan (CLAUDE.md conventions)

- Public interface only: build with pod and operator calls and stream methods, read through
  `job.nodes`, `as_table`, `run`, `run_async`, `save`, `load`, `as_source`, the database's
  `list_table_paths`. No private entry point (the prototype's use of `_record_invocation` is
  evidence only).
- Several items (ITL-1236): `register` takes several streams and several keyword names, so
  every test that is not about a single stream registers two or more, in more than one order,
  over inputs with two or more tag and two or more data columns in more than one arrangement,
  and asserts that names, hashes, paths and rows do not change with the order.
- Order invariance (ITL-1271): as built, the orders are tested in
  `tests/test_pipeline/test_register_streams.py` (every operator case with its streams in
  three registration orders; the function-pod forms in five orders split over two calls;
  node names over every permutation of three registrations), against the hand-written job.

### 10.1 Found while building (as built)

- **Which node computes a shared result is not specified.** Two function nodes of one
  function over inputs with equal data hashes share one result table: the first to run
  computes (`__computed` true), the other reads. Which runs first follows the run order of
  independent nodes, which is not specified: for one hand-written job alone it varies between
  processes with the hash seed (measured: 10 seeds, two outcomes). On the async path on CI's
  parallel runner the shared result table's rows also differed between two runs. The property
  test leaves `__computed` out with that reason, as it leaves out `__pod_ts` and `__data_id`
  (a uuid7), and gives each function-pod form its own function, so no two of its nodes share
  a result table. Filed as ITL-1404.
- **The saved edge list followed recording order** (ITL-1404), and a load gave a node its
  inputs in that order, so a saved and loaded `MergeJoin` could swap its arguments (on `main`
  too). Fixed in review round 1 (11.2): the saved list is canonical, each node's inputs in
  argument order, and the loader keeps that order.
- Refusals: each refusal of sections 3, 5 and 8 with the job unchanged afterward (names,
  `job.nodes`, recorded state through a subsequent `with` block that succeeds).
- Restrictions: one source restricted two ways across two registered streams refused; the
  restriction shown in `RunSummary` and saved (SpiralDB tests run where SpiralDB is available,
  as the existing ITL-1218 tests do).
- Golden hashes: none move; no golden case is added (the property is a comparison, not a new
  hash).

## 11. Decisions

Decided by the developer (2026-11-22): D4 (in P1), D7 (revised), rule 2.1, and that
absorbed steps share storage only under one database and job name (5.1). Built as
recommended, NOT individually confirmed: D1, D2, D3, D5, D6, D8, D9, D13, D14. D11 and D12
are ITL-1389.

1. **D1** (built as recommended, not individually confirmed) The call is `job.register(*streams, **named_streams)`, returning `None`, usable
   outside and inside a `with` block, mixing freely with reopening. (Recommended.)
2. **D2** (built as recommended, not individually confirmed) `register` lives on `AbstractPipelineBase`, so `Pipeline` has it too. (Recommended.)
3. **D3** (built as recommended, not individually confirmed) No `PipelineJob.from_streams` in the first version; a wrapper later if wanted.
   (Recommended.)
4. **D4** DECIDED (developer, 2026-11-22), in P1: one rule for `register` and a `with`
   block (2.1); a stream becomes a source only by the user's explicit act.
5. **D5** (built as recommended, not individually confirmed) Unlabelled steps are accepted with computed labels and today's content-hash suffixes,
   because the property requires the by-hand names. (Recommended.)
6. **D6** (built as recommended, not individually confirmed) Keyword names are exact: refused on collision, never suffixed, never for a root
   source; one step under two different assigned labels is refused. (Recommended.)
7. **D7** REVISED (developer, 2026-11-22): a node of another job is walked with every
   upstream step (5.1); a loaded node with a loadable function is walked like any stream,
   and a step whose function is not attached is refused for now; the decided target is a
   proxy node with a warning on the absorbing job's databases (developer, 2026-11-22;
   ITL-1407) (5.2).
8. **D8** (built as recommended, not individually confirmed) Side-effect pods are registered like any step (deliveries happen on `run()`), rather
   than refused. (Recommended; refusing is the cautious alternative.)
9. **D9** (built as recommended, not individually confirmed) Registering a bare root source alone is refused. (Recommended.)
10. **D10** P1 is the smallest useful version and the workshop target if time allows; the
    workshop does not wait for it.
11. **D11** The suffix renumbering when a job grows (1.2): filed as ITL-1389, not fixed here.
12. **D12** The `with` block keeping the LAST label recorded for one step (1.4): filed as
    ITL-1389; `register` refuses the case itself.
13. **D13** `from_stream` keeps case 3 only with an explicit `name` (an explicit cut); its
    implicit `<label>:<hash>` name is removed (9.2). No `as_source()` is added to plain
    streams in P1 (an exploration stream is cut by building a source from it, 9.5).
    (Recommended.)
14. **D14** A refused `with` block (its body or its compile) restores the recording state of
    its entry (2.2). (Recommended.)

### 11.2 Review round 1 (independent Codex review, 2026-11-22): what changed

Each finding has a test (the PR body has the table). Behaviour as built after the round:

- **Restrictions on every way in.** One step over one source restricted two ways has one
  identity, so the walk meets it once; `register` checks the restriction rule over every
  stream it is given, also when all its steps are held. Refused on `register` (one call or
  two), in a block, by reopening and by taking in another job's nodes.
- **An ordered operator keeps its argument order through save and load.** The blueprint
  saves operators' argument order with their descriptors and sorts the edges independently
  (ITL-1417).
- **A refusal leaves the job exactly as it was** (D14): a refused block or `register`
  restores the recorded steps, the compiled nodes (the same objects) and the bindings, also
  when the refusal comes from compilation (a database that cannot store a result table).
- **Bindings stay the job's own.** A slot of another pipeline is kept as a copy holding its
  binding at the time it is taken in, and each recorded step keeps its inputs' keys as they
  were, so a later bind of the other job changes nothing here.
- **A keyword name is exact in every form**: refused when another step holds or would hold
  the name (in a block, with `auto_compile=False`, across calls in either order); a later
  step labelled with a name given exactly is refused on every way in.
- **Registering held steps is a no-op**: nothing is recorded or compiled, so held runs stay.
- **Labels**: a step held by the job keeps its label (no refusal); a block that builds one
  step twice keeps the lexicographically smallest explicit label (ITL-1436). Within one `register`
  call, an assigned label wins over none, in either order; two different assigned labels
  are refused.
- **A loaded job is read-only**: `register` refuses. A `with` block on a loaded job still
  replaces its nodes, as on `main` (pre-existing; pinned by an expected failure, to be filed).
- **A blueprint keeps a job's slot name**: `SourceNode.from_stream` of another job's slot
  keeps its name, so `PipelineJob.from_pipeline` binds by it.
- **A stream that is neither a source nor made by a pod** given to `register` is refused.
- Pre-existing and not changed: reopening a job after a bind does not find the rebound
  nodes as held (on `main` a `<label>:<hash>` leaf appears; here the steps appear twice);
  pinned by an expected failure, to be filed. `MergeJoin(s, s)` (one stream twice) fails to
  run, by hand as when registered (an execution-path issue).
