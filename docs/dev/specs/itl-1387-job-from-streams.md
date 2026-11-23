# Build a pipeline job from streams made while exploring (ITL-1387)

Status: DESIGN ONLY, for the developer's approval. Measured on `origin/main` fc1df420
(2026-11-22). No library code changes with this document.

Developer's words (2026-11-22): "we can build a feature whereby we can register one or more
streams against a pipeline job and it will create a full pipeline based off of the passed on
streams + all of its upstreams up till and including sources. That can simplify the
exploration -> pipeline development process." Also: "there should be a point between
separating exploration code from proper executable research code so the need to redefine all
steps plainly in a pipeline_job is actually acceptable", and reopening `with pipeline_job:` to
add steps over the nodes so far should still work. Nice to have for the workshop
(2026-11-25), not a blocker.

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
  is not the node the job holds afterwards.
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
  earlier in `_invocation_lut`). This is a pre-existing departure from "order never counts";
  not logged yet (decision D12).
- `edge_cases.py`: a step over a raw `ArrowTableStream` is refused by the bind check
  (`SourceSpecMismatchError`), in a `with` block and under the prototype walk alike.
- `edge_cases.py`: a node of ANOTHER job used in a `with` block today becomes a leaf named
  `doubled:<hash>` bound to that job node, the F07 shape again.

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
  compiled nodes, bindings). Today's `with` block does not have this property (1.3, the
  refused join stays recorded); `register` must, by planning and checking everything before
  recording anything (section 9).
- Returns `None`, like `bind`; the nodes are read from `job.nodes` / `job.<name>`.

Alternatives weighed:

- `PipelineJob.from_streams(*streams, name=, database=, **named)`: a constructor gives one
  shot and does not meet reopening. It can be a two-line wrapper over `register` later; not
  in the first version (D3).
- `job.add(...)`: "add" reads as adding one node; `register` is the developer's word.
- **No new call: make a `with` block take in a pre-built stream's upstreams** (fix F07 in
  place). Attractive, because the trap in 1.3 then disappears for everyone, and it is the same
  walk. But it changes what a `with` block records for an existing pattern, and is a separate
  decision. Recommended as a second piece (D4), after `register` lands with its tests.
- A `Pipeline`-only call (blueprint) then `from_pipeline`: possible with the same method on
  the base class; no separate API needed.

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
   - a name for a root source: a source node's name is its source id, which is part of its
     identity (`compile()` step 2a); rename the source with `source_id=` instead;
   - a name starting with `_` (attribute access refuses it).
4. Two steps with ONE assigned label (two different steps both `label="norm"`): accepted with
   today's suffixes (`norm_1`, `norm_2`, content-hash order), because the by-hand job does
   that. A warning is not added (it would differ from the `with` block).
5. One step (one content hash) reached under two DIFFERENT assigned labels (1.4: built twice
   as `label="x"` and `label="y"`): refused, naming both labels. Today's `with` block keeps the
   last recorded, which is order-dependent; `register` must not inherit that. The `with` block
   itself is D12.

The suffix renumbering of 1.2 (a node's suffix can change when the job grows) is inherited,
not fixed here (D11). An explicit keyword name is the remedy a user has today: named nodes
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

What is a root: a `RootSource` (every source class, `CachedSource`, `DerivedSource`,
`PollingSource`), a `SourceJobNode` of this job, or a stream with no producer. The walk never
goes beneath a root (a `DerivedSource`'s origin and a `CachedSource`'s source are not steps of
this job), as the `with` block never records them.

Under the first version, the walk does NOT go through a node of ANOTHER job, nor a node of a
loaded job: it refuses, naming the node, with the remedies `other.nodes[name].as_source()`
(take its stored results as a source) or building in one job by reopening it. Walking through
would replicate the other job's steps (their producers and upstreams are the same identities),
but whether to read another job's nodes as steps or as stored results is a choice the user
should make, and today's behaviour (a `<label>:<hash>` leaf bound to the node) is the F07
trap. D7.

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

> A job built by `register` has exactly the node names, node types, content hashes, pipeline
> hashes, stored table paths, stored tables, record ids, results and saved file (`pipeline`
> and `bindings` sections) of the same pipeline written by hand in a `with` block.

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
| A node of another job, or of a loaded job | refused (D7) | section 5 |
| A raw `ArrowTableStream` root, a joined stream as a root | refused, as today | the bind check; binding such a stream is ITL-1127 |
| A bare root source registered alone | refused (D9) | there is no step to register, and no by-hand equivalent (a `with` block records only steps); the message says to register a stream built from it |
| A stream whose producer is none of the three pod kinds | refused | the walk classifies by producer: a function pod, a side-effect pod or an operator; anything else has no invocation to record |

## 9. Implementation plan

One function does the walk, beside the recording code in `pipeline/base.py`:

1. **Plan** (no mutation): walk every registered stream depth first; collect, in topological
   order, `(key, invocation kind, pod, inputs, label)` for each step not held; collect leaves;
   classify each producer (function, side effect, operator) or refuse; apply the naming rules
   of section 3 against the job's current labels and the plan; refuse other jobs' nodes.
2. **Check** (no mutation): `require_one_restriction` over held streams plus every planned
   input; bind-check each new leaf (`SourceNodeBase.validate` through the same construction
   `compile()` uses, without keeping the node); `refuse_restricted_operator_cache` where a
   cache mode is already set.
3. **Record**: `_record_invocation` for each planned step, in plan order; keyword names set
   the invocation's label.
4. **Compile** unless inside a `with` block or `auto_compile=False`. If `compile()` raises
   anyway, restore the recorded state taken before step 3 (the three recording structures are
   plain dicts and an `nx.DiGraph`, copied cheaply).

Pieces, with agent-time estimates:

| Piece | Content | Estimate |
|---|---|---|
| P1 (smallest useful version) | `register(*streams, **named)` on `AbstractPipelineBase`; plan, check, record, compile; the refusals of sections 3, 5 and 8; the property test over the operator and function-pod builders, sync and async, saved and loaded; the "must not happen" tests; one docs section in the job concepts page with a several-stream sample; changelog fragment | 1.5 to 2 days |
| P2 | a `with` block takes in a pre-built stream's upstreams through the same walk (F07 goes away), if D4 is yes; the 1.3 refused-join residue fixed by the same all-or-nothing recording | 0.5 to 1 day |
| P3 | `PipelineJob.from_streams` wrapper (D3); a tutorial page on the exploration-to-job step | 0.5 day |

P1 alone answers the developer's request: explore freely, then one `register` call gives the
full pipeline, saved, stored and loadable, identical to the hand-written one.

## 10. Test plan (CLAUDE.md conventions)

- Public interface only: build with pod and operator calls and stream methods, read through
  `job.nodes`, `as_table`, `run`, `run_async`, `save`, `load`, `as_source`, the database's
  `list_table_paths`. No private entry point (the prototype's use of `_record_invocation` is
  evidence only).
- Several items (ITL-1236): `register` takes several streams and several keyword names, so
  every test that is not about a single stream registers two or more, in more than one order,
  over inputs with two or more tag and two or more data columns in more than one arrangement,
  and asserts that names, hashes, paths and rows do not change with the order.
- Order invariance (ITL-1271): `register` gets a case in `tests/test_core/test_order_invariance.py`
  (a job built by registering the same streams in permuted orders and keyword orders).
- Refusals: each refusal of sections 3, 5 and 8 with the job unchanged afterwards (names,
  `job.nodes`, recorded state through a subsequent `with` block that succeeds).
- Restrictions: one source restricted two ways across two registered streams refused; the
  restriction shown in `RunSummary` and saved (SpiralDB tests run where SpiralDB is available,
  as the existing ITL-1218 tests do).
- Golden hashes: none move; no golden case is added (the property is a comparison, not a new
  hash).

## 11. Decisions for the developer

1. **D1** The call is `job.register(*streams, **named_streams)`, returning `None`, usable
   outside and inside a `with` block, mixing freely with reopening. (Recommended.)
2. **D2** `register` lives on `AbstractPipelineBase`, so `Pipeline` has it too. (Recommended.)
3. **D3** No `PipelineJob.from_streams` in the first version; a wrapper later if wanted.
   (Recommended.)
4. **D4** Later piece P2: a `with` block over a pre-built stream takes in its upstreams too,
   replacing the `<label>:<hash>` leaf of F07. (Recommended yes, after P1.)
5. **D5** Unlabelled steps are accepted with computed labels and today's content-hash suffixes,
   because the property requires the by-hand names. (Recommended.)
6. **D6** Keyword names are exact: refused on collision, never suffixed, never for a root
   source; one step under two different assigned labels is refused. (Recommended.)
7. **D7** A node of another job or of a loaded job is refused, with `as_source()` or reopening
   as remedies, rather than walked through. (Recommended for the first version.)
8. **D8** Side-effect pods are registered like any step (deliveries happen on `run()`), rather
   than refused. (Recommended; refusing is the cautious alternative.)
9. **D9** Registering a bare root source alone is refused. (Recommended.)
10. **D10** P1 is the smallest useful version and the workshop target if time allows; the
    workshop does not wait for it.
11. **D11** The suffix renumbering when a job grows (1.2) is inherited and not fixed here: log
    it as a design issue? (Pre-existing; ask before logging, per CLAUDE.md.)
12. **D12** The `with` block keeping the LAST label recorded for one step (1.4) depends on
    recording order: log it as a design issue and a Linear issue? (Pre-existing; `register`
    refuses the case itself.)
