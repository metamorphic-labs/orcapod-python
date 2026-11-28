# Tutorials

These tutorials teach Orcapod on one small analysis. The core course builds it from start to
finish, and the short tutorials each add one subject. They are written for scientists who are
comfortable with Python and have not used Orcapod before. You can work through them on your
own, at your own pace, on a laptop.

A page takes about 20 minutes: a few minutes of reading, samples to run, and two or three
exercises. Every exercise has a solution you can unfold. Every sample on these pages is run by
Orcapod's test suite, so what a page shows is what the library prints.

## What you will build

A pipeline that takes a small synthetic neural dataset and computes, for every recording
session, the mean firing rate of its units. The dataset is only the example: the same steps
apply to any analysis that starts from tables or files. Along the way you will store the
results in a database, re-run the pipeline so that only new work is computed, read and write
files, and trace a result back to the rows it came from.

## The dataset

A short script generates the data on your machine. It imitates a small study of neural
recordings in mice. A *probe* is a recording device placed in one brain region, a *unit* is
one neuron detected on a probe, and a *spike* is one event of a unit:

- 4 mice, two wildtype and two knockout
- 2 recording sessions per mouse
- 2 probes per session (`imec0` in V1, `imec1` in CA1)
- 4 units per probe

```
workshop_data/
    subjects.csv       one row per mouse:    subject_id, sex, age_weeks, genotype
    sessions.csv       one row per session:  subject_id, session, date, duration_s
    units.csv          one row per unit:     subject_id, session, probe, unit,
                                             region, depth_um, n_spikes
    sessions/
        mouse_01_s1/
            notes.txt
            spikes_imec0.csv      one row per spike: time_s, unit
            spikes_imec1.csv
        mouse_01_s2/
        ...
```

27 files, about 0.4 MB. [Part 0](00-setup.md) shows how to generate it.

## Core course

Do these parts in order.

| Part | You learn | Time |
|---|---|---|
| [0. Setup](00-setup.md) | Install Orcapod, check it works, generate the dataset | 10 min |
| [1. Your first stream](01-first-stream.md) | Sources, tag and data columns, looking at a stream | 20 min |
| [2. Computing with function pods](02-function-pods.md) | Turning a Python function into a pipeline step | 20 min |
| [3. Reshaping with operators](03-operators.md) | Joining, filtering, renaming | 20 min |
| [4. Many rows into one](04-many-rows-into-one.md) | Grouping rows and computing over the group | 20 min |
| [5. Keeping results](05-pipeline-job.md) | `PipelineJob`: run a pipeline against a database | 20 min |
| [6. Re-runs](06-reruns-and-caching.md) | What is recomputed, and what is not | 20 min |

## Short tutorials

Each is on one subject and can be read in any order after the parts it names.

| Tutorial | You learn | Before you start | Time |
|---|---|---|---|
| [Working with files](files.md) | Functions that read and write files | Parts 0 to 5 | 20 min |
| [Inspecting results](inspecting-results.md) | The steps of a job, where a row came from, the record of each call | Parts 0 to 6 | 15 min |
| [Tracing a value](tracing-a-value.md) | One value back to its source cells; why a row is missing | Parts 0 to 6 | 20 min |
| [Chaining pipelines](chaining-pipelines.md) | A second pipeline over the stored results of the first | Parts 0 to 5 | 10 min |
| [Handling errors](handling-errors.md) | Errors while building a pipeline; failed rows in a run | Parts 0 to 6 | 15 min |
| [Missing values](missing-values.md) | Types that may be null, dropping nulls, nulls in joins and functions | Parts 0 to 3 | 15 min |
| [Coming from pandas or Polars](coming-from-pandas-or-polars.md) | The Orcapod form of select, filter, join, group and new columns; where Orcapod differs | Part 0 | 15 min |

[Where next](where-next.md) is a short tour of features these pages did not cover.

## How to use these pages

Type or paste each sample into a Python session, a notebook, or a script, in the order shown.
Within one page, later samples use names defined by earlier ones. Each page starts from
scratch, so you can stop after any page and pick up at the next one later.

Lines starting with `#` directly under a `print(...)` show what it prints. Where a value
differs from run to run (a hash, a timestamp), the page shows `...` in its place.

If something fails, look at [Troubleshooting](../troubleshooting.md) first.
