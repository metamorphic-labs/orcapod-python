# ITL-1082: PipelineJob `store` -> `database`

Mechanical rename of the PipelineJob parameter, property and private attribute
(`__init__`, `from_pipeline`, `bind`, `load`, `job.store`). No alias.
Out of scope: `set_ephemeral_store`, error classes, prose.
Saved-file key `bindings.store` is a stored-format choice: left unchanged pending the
developer's answer (marked in job.py). Notes: callers found by grep for `store=` and `.store`.
