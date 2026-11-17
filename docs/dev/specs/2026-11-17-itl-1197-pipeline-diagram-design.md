# Design Spec: the pipeline DAG as a diagram in notebooks (ITL-1197)

**Date:** 2026-11-17
**Issue:** [ITL-1197](https://linear.app/metamorphic/issue/ITL-1197)
**Status:** Approved by the coordinator for the workshop version (2026-11-17); the
developer approved a vendored library in principle for the follow-up (section 8).

Implementation: `src/orcapod/pipeline/diagram.py`, `Pipeline.diagram()` /
`PipelineJob.diagram()` and the rich display in `src/orcapod/pipeline/base.py`, tests in
`tests/test_pipeline/test_diagram.py`. Screenshots of the tutorial job (before and after a
run, light and dark, a node selected, scripts removed, folded, top to bottom, and `partial`
after a run where `firing_rate` failed for one unit) are in `itl-1197-pipeline-diagram/`,
made by `docs/dev/scripts/itl_1197/diagram_screenshots.py`. The two JupyterLab shots were
taken by hand (section 3): notebooks with a `python3` kernelspec (no kernel dialog), the
news notification turned off in a scratch settings directory, and in the trusted one a
small script added to the output after the diagram clicks the `rates` node, as a reader
would, so its details pane shows. The JupyterLab shots were taken before the collapse checkbox
was added (`collapse-checkbox-light.png` shows it).

**In development** (developer, November 17, 2026): the diagram, its interface and the
notebook display may change in any release without a deprecation period. The docs say so
in a caution box, the docstrings and the changelog (Added and Known limitations), and the
rendered view carries a small "preview" tag next to the title (also in the no-script
fallback). The first diagram made in a process issues `orcapod.errors.InDevelopmentWarning`
(developer, November 17, 2026: once per process, modelled on `ProvisionalHashWarning`; a
general class for any feature in development, bookkeeping in
`orcapod/utils/in_development.py`); importing Orcapod, `repr`, `head()` and the plain display
of an uncompiled pipeline never issue it. `to_dict()` has no version marker; its shape may change.
The developer confirmed the diagram as the default rich display of `Pipeline` and
`PipelineJob` (November 17, 2026).

## 1. What is displayed, read without computing

- Nodes: every node of `pipeline.dag` (source, operator, function pod, function pod with
  side effects, side-effect pod), by `node_type`. Shape and colour by kind, plus the kind
  written in the node (not colour alone).
- Node text: label (its key in `pipeline.nodes`) and a kind line: kind, the operator's or
  function's name when the label does not already say it (`function pod · firing_rate`; not for `SelectDataColumns_2`), for a job the
  run status.
- Edges: the DAG's edges (streams). Under `collapse=True`, dashed when steps were folded.
- Selected node (click/Enter; hover tooltip on the inline picture): tag and data schema from
  `node.output_schema()` (predicted, never computed), its inputs, and for a job: status,
  last-run counts, first error, folded steps.
- Job status, all from memory: source `bound`/`unbound` (`bound_source`); other nodes
  `not run` (`last_modified is None`), `stale` (`is_stale`), `failed` (the node's own
  failed-row count, set by a job run and by the node's own `run()`, read through one
  internal helper until ITL-1209 gives a node its own run summary), `current`
  (`holds_current_run()`, whether the job's last run ran or kept it: `held` was dropped
  as a status after the Copilot review, since `job.last_run` records held rows for function
  nodes only; a function node's facts count them), `loaded only`
  (`holds_current_run(load_only=True)`),
  `partial` (ran, not current, and a node it reads from is `failed` or `partial`: it holds
  the results of the rows that did not fail; Copilot review of #424).
  Row counts from `job.last_run` (function nodes: rows, computed, reused, failed, held,
  not stored). Operators: status only (no count without reading).
- Never: `as_table`, `iter_data`, `head`, `check_for_new_data`, any database call, any
  user function. A `DerivedSource` with `tags_as_data` used to read its origin's rows to
  give `output_schema()`; it now derives them from the origin's zero-row table (fixed in
  this PR, found by the Copilot review). `last_modified`, `is_stale`, `holds_current_run` read attributes only.

## 2. Public API

- `Pipeline.diagram(*, direction="LR", collapse=False, max_nodes=400) -> PipelineDiagram`
  and the same on `PipelineJob`.
- `PipelineDiagram`: `_repr_mimebundle_` (text/html + text/plain), `to_html()`,
  `to_svg()`, `to_text()`, `save(path)` (standalone page), `__repr__`/`print` = text
  listing (used by doc samples), and `to_dict()`: the plain JSON-able model (nodes, edges,
  labels, kinds, schemas as text, status, facts) that any renderer consumes.
- `Pipeline` and `PipelineJob` get `_repr_mimebundle_` delegating to `diagram()` with
  defaults; `__repr__` unchanged. Uncompiled pipeline: text/plain only.
- `show_graph()` (Graphviz) is kept as is, for image files and for users with `dot`.
  Docs point to the diagram first; show_graph stays the "export a PNG/PDF" path.
- collapse: the automatic join and select (ITL-807) are by design indistinguishable from
  hand-written ones, so `collapse=True` folds by kind: Join, Select*/Drop*/Map* operators
  with exactly one consumer are folded into the edge (dashed) and listed in the consumer's
  details. Decided by the operator's class, never its label (a user's `Join(label="joined")`
  is folded, a `DropNulls(label="Join")` is not); an operator's detail is its class name.
- Collapse checkbox (developer, November 17, 2026): the layout stays in Python, so the
  output embeds both layouts: the one `collapse=` asks for as the image, the node table
  and the model block, and the other as a second JSON block (`op-dag-other`: its model and
  its standalone SVG, base64). The script builds a `collapse` checkbox beside the zoom
  buttons and switches pictures; selection, pan, zoom and the details panel keep working,
  and the selected node stays selected when it is in both layouts (else the panel
  closes). The checkbox exists only where scripts run, only when folding changes
  something, and only when the other layout has at most `max_nodes` nodes. The no-script
  fallback shows the `collapse=` layout. Output is deterministic. Size: the other layout
  adds about half again (100 joined branches, 401 nodes, 201 folded: 923 KiB of HTML
  instead of about 613 KiB; built in 0.03 s). Default False (the tutorial teaches the node names, SelectDataColumns_1 etc.).

## 3. Rendering approach

(a) RECOMMENDED for Thursday: Python layered layout (written here, ~250 lines, stdlib
only) -> inline SVG with presentation attributes (draws with no CSS and no JS), one
`<style>` and one inline `<script>` as enhancement (pan by drag, ctrl/cmd-wheel and
+/-/fit zoom, click/Enter details panel, edge highlight). Node data for the panel in a
`<script type="application/json">` block, inserted with textContent only. No dependency,
nothing vendored, no network, no binary. Survives save/reopen and nbconvert HTML export
(it is just HTML+SVG). When scripts do not run (untrusted notebook, GitHub, a sanitizer)
the picture and a `<details>` node table remain.
Front ends: JupyterLab / Notebook 7 run the script for trusted (freshly produced)
outputs; classic Notebook 6 evaluates scripts through jQuery (no
`document.currentScript`: the script falls back to the last un-initialised container);
VS Code and Colab run output scripts in their webview/iframe.

Checked by hand in JupyterLab 4.6.6 (headless server, headless Chrome; screenshots
`itl-1197-pipeline-diagram/jupyterlab-*.png`): for an untrusted notebook its sanitizer
removes inline `<svg>` (keeping only its text, so the hover texts showed as a wall of
text) and `<style>`, and keeps `<img>` with any `src` (it checks schemes on `href` and
`cite` only). So the picture is shipped as `<img src="data:image/svg+xml;base64,...">`,
drawn on its own light background in a fixed text colour (an image does not take the
page's colours), and the script, where it runs, swaps it for the same SVG inline (no
background, `currentColor`) and creates the zoom buttons itself, so nothing inert is left
in a sanitized output. Untrusted: the picture and the Nodes table. Trusted: interactive.
(b) Vendored library: see section 8 (later end state).
(c) anywidget/ipywidgets: rejected: new dependencies, widget state lost on save/export
unless widget state is saved, Colab needs its custom widget manager.

## 4. Layout quality

Layers by longest path (a root sits just before its first consumer), dummy points for
long edges, 12 barycenter sweeps keeping the fewest-crossings order, balanced placement
(average of the up- and down-pushed solutions), cubic Bezier edges through the dummies.
Deterministic: nodes in topological order with ties broken by label, edges sorted, no
set/dict iteration order or hash() in the output; marker id from a SHA-256 of the graph.
150-node random DAG: layout 0.01 s, 173 KiB HTML, readable with zoom. max_nodes (default
400) switches to the text listing beyond it.

## 5. Tests (no browser) and docs

- Model: tutorial-like job: nodes, kinds, labels, edges, schemas; statuses before run,
  after run, failed rows, stale after rebind/clear_cache, held rows on a rerun, loaded only.
- Determinism: two diagrams of the same pipeline are byte-identical; recording order of
  independent branches does not change the output.
- Escaping: label, column name, pipeline name and error message containing
  `</script><script>`, `<img onerror>`, `"><svg onload>`: parse the HTML with
  html.parser and assert only our tag names, no `on*` attribute, no extra script element;
  JSON block contains no `<`.
- No computing: function with a call counter, a database wrapper that fails on any read
  or write, a source whose check/read fails: rendering before and after run leaves all
  untouched.
- Layout: no overlapping boxes within a layer, every edge goes forward, a 300-node graph
  lays out in under a second.
- Docs: `print(job.diagram())` (deterministic text listing) in the pipeline concept page
  section and tutorial Part 5; Part 8 and troubleshooting point to `job.diagram()`
  instead of show_graph. Browser checks stay manual (headless Chrome screenshots).

## 6. Accessibility and themes

Text and edges use `currentColor` (the notebook's own text colour), kinds a solid stroke
with a 14% fill: readable on light and dark themes in JupyterLab, VS Code, Colab without
theme detection. Kind and status are also written as text and drawn as shape/dash (not run,
stale, partial, unbound dashed), never colour alone. `role="group"` + `<title>` on the
inline SVG (it holds the node controls; the `<img>` fallback carries image semantics and
`alt`), nodes
`role="button"`, `tabindex=0`, `aria-label`, Enter/Space selects; panel `aria-live`.

## 7. Thursday version vs later

Thursday: diagram module, `diagram()` on Pipeline and PipelineJob, rich repr, options
direction/collapse/max_nodes, details panel, job status, tests, docs, changelog.
Later: vendored-library renderer (section 8), live updates during a run (the approach
allows it: `display(..., display_id=True)` and `handle.update(job.diagram())` from an
observer), `node.diagram()` for one node's ancestry, operator arguments in details,
search/filter for huge graphs, reuse by the docs site (ITL-1170).

## 8. Later vendored library: recommendation

@dagrejs/dagre 3.1.3: MIT; `dist/dagre.min.js` 49 KB (17 KB gzip) with graphlib
included; a plain IIFE global (no AMD/UMD wrapper, so no RequireJS clash in classic
Notebook); layout only (network-simplex ranking, layered, good for DAGs of this kind) and
we draw the SVG from the same `to_dict()` model. Inlined into each output: offline, works
in JupyterLab, VS Code, Colab and exported HTML; with no scripts the Python SVG remains.
Not chosen: elkjs 0.12.1 (best layered quality, orthogonal routing, ports, compound nodes
for collapsed groups, but 1.6 MB / 480 KB gzip per output and licence EPL-2.0 OR
GPL-3.0); cytoscape 3.35 (MIT, canvas: no static picture, 443 KB min / 139 KB gzip, plus a dagre extension for layered layout); mermaid 12 (MIT,
multi-MB); d3-dag 1.3 (MIT, 143 KB, light maintenance).
