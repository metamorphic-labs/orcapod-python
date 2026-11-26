# Part 4: Many rows into one

A summary is computed from many rows: the mean firing rate of a session needs all the units of
that session. In Orcapod this takes two steps. An operator gathers the rows that belong
together into one row whose values are lists. A function pod then computes over those lists.

You will:

- gather rows that share tag values with `group_by`
- write a function pod that takes lists
- compute the mean firing rate of every session
- gather rows by count with `batch`

```python
from workshop_data import make_dataset
from orcapod import function_pod
from orcapod.sources import CSVSource

data_dir = make_dataset()

subjects = CSVSource(
    str(data_dir / "subjects.csv"), tag_columns=["subject_id"], tag_only_columns=True, source_id="subjects"
)
sessions = CSVSource(
    str(data_dir / "sessions.csv"),
    tag_columns=["subject_id", "session"], tag_only_columns=True,
    source_id="sessions",
)
units = CSVSource(
    str(data_dir / "units.csv"),
    tag_columns=["subject_id", "session", "probe", "unit"], tag_only_columns=True,
    source_id="units",
)


@function_pod(output_keys="rate_hz")
def firing_rate(n_spikes: int, duration_s: float) -> float:
    return n_spikes / duration_s


rates = firing_rate.pod(
    units.select_data_columns(["n_spikes"]),
    sessions.select_data_columns(["duration_s"]),
)
```

`rates` is the stream from Part 2: one row per unit, 64 rows, with tags
`subject_id, session, probe, unit` and one data column `rate_hz`.

## Group by tags

`group_by` takes a list of tag columns. All rows with the same values in those columns
become one row:

```python
per_session = rates.group_by(["subject_id", "session"])

print(per_session.output_schema())
# (Schema({'subject_id': str, 'session': str}), Schema({'rate_hz': list[float]}))
print(per_session.as_table().num_rows)
# 8
```

64 unit rows became 8 session rows:

- The columns you grouped by, `subject_id` and `session`, are the tags of the result.
- The other tags, `probe` and `unit`, are dropped, because a session row covers many probes
  and units.
- Every data column became a list: `rate_hz` is now `list[float]`, one element per unit of
  the session.

Here is the first row:

```python
tag, data = next(iter(per_session))
print(tag.as_dict())
# {'subject_id': 'mouse_01', 'session': 's1'}
print([round(r, 2) for r in data.as_dict()["rate_hz"]])
# [2.48, 5.43, 11.03, 9.98, 3.58, 4.98, 2.25, 8.52]
```

These are the eight rates of `mouse_01`'s first session. The members of a group are ordered by
the dropped tags in name order, here `probe`, then `unit`. To order them by other tags first,
name those tags: `group_by([...], member_order=["unit"])`.

The list does not say which probe or unit each rate came from. If a later step needs that, it
has to be a data column before you group; see [GroupBy](../concepts/operators.md#groupby).

## A function pod over lists

Annotate the parameter as `list[float]`:

```python
@function_pod(output_keys="mean_rate_hz")
def mean_rate(rate_hz: list[float]) -> float:
    return sum(rate_hz) / len(rate_hz)


session_means = mean_rate.pod(per_session)
print(session_means.as_pandas_df())
#   subject_id session  mean_rate_hz
# 0   mouse_01      s1      6.033333
# 1   mouse_02      s1      5.845833
# 2   mouse_03      s1      4.906944
# 3   mouse_04      s1      5.136458
# 4   mouse_01      s2      6.793750
# 5   mouse_02      s2      7.506250
# 6   mouse_03      s2      5.059375
# 7   mouse_04      s2      5.129167
```

The rows come out sorted by the group keys in name order: `session`, then `subject_id`.

The whole analysis so far, written as one chain:

```python
session_means = mean_rate.pod(
    firing_rate.pod(
        units.select_data_columns(["n_spikes"]),
        sessions.select_data_columns(["duration_s"]),
    )
    .group_by(["subject_id", "session"])
)
print(session_means.as_table().num_rows)
# 8
```

## Grouping uses tags only

You can group by tag columns that hold a single value per row. Grouping by a data column
raises an error:

```python
from orcapod.errors import InputValidationError

try:
    rates.group_by(["rate_hz"])
except InputValidationError as err:
    print(err)
# GroupBy: ['rate_hz'] are not tag columns of the input stream. Available tag columns: ['subject_id', 'session', 'probe', 'unit']. (Data columns cannot be grouping keys: ['rate_hz'])
```

`genotype` and `sex` are data columns of `subjects`, and they stay data columns after a join.
To group by a data column, give it the tag role first, with `tag_by`:

```python
with_subject = session_means.join(subjects.select_data_columns(["genotype", "sex"]))
by_group = with_subject.tag_by("genotype", "sex").group_by(["genotype", "sex"])

print(by_group.output_schema())
# (Schema({'genotype': str, 'sex': str}), Schema({'genotype': str, 'sex': str, 'mean_rate_hz': list[float]}))
print(by_group.as_table().num_rows)
# 4
```

`tag_by` changes no value. Each column you name is now a tag and is still a data column, so
it shows in both schemas. The same works for a column that a function pod computed. A tag
holds one value per row and no missing value, so `tag_by` refuses a list column and a column
that may hold nulls; see [Adding the tag role](../concepts/data-tags.md#adding-the-tag-role-tag_by).

If you know from the start that you will group or match by a column, make it a tag in the
source, in `tag_columns`. Exercise 3 uses `tag_by`.

## Batch

`batch` gathers rows by count. With no argument, the whole stream becomes a single row:

```python
everyone = subjects.batch()
for tag, data in everyone:
    print(tag.as_dict())
    print(data.as_dict())
# {'subject_id': ['mouse_01', 'mouse_02', 'mouse_03', 'mouse_04']}
# {'sex': ['F', 'M', 'F', 'M'], 'age_weeks': [12, 14, 11, 13], 'genotype': ['wildtype', 'wildtype', 'knockout', 'knockout']}
```

The tags became lists too. With `batch_size=N` you get rows of `N` members each. The last one
may be smaller, unless you pass `drop_partial_batch=True`:

```python
for tag, data in subjects.batch(batch_size=3):
    print(tag.as_dict())
# {'subject_id': ['mouse_01', 'mouse_02', 'mouse_03']}
# {'subject_id': ['mouse_04']}
```

Use `batch` for everything at once or for chunks of a fixed size, such as a grand mean over all
rows or mini-batches for a model.

If you need both `group_by` and `batch`, use them in that order. `batch` then gathers the
grouped rows: here sessions, whose lists of units stay whole. The tags of a batched stream are
lists, and `group_by` cannot use those.

## Key points

- `group_by([...tags])` turns the rows sharing those tag values into one row of lists.
  `batch()` does the same by count.
- A function pod with `list[...]` parameters computes over the lists.
- `group_by` takes tag columns only. To group by a data column, give it the tag role first:
  `stream.tag_by("column")`.
- `group_by` keeps only the tags you group by; the other tags are dropped. `batch` keeps
  every tag, as a list.
- The members of a group are ordered by the dropped tags in name order, or by the tags you
  name in `member_order`. Groups come out sorted by their keys in name order.

## Exercises

**1.** Compute the mean firing rate **per probe** within each session: one row for
`mouse_01, s1, imec0`, one for `mouse_01, s1, imec1`, and so on. How many rows are there?

??? success "Solution"
    Add `probe` to the grouping columns. `mean_rate` works unchanged.

    ```python
    per_probe = mean_rate.pod(
        rates.group_by(["subject_id", "session", "probe"])
    )
    print(per_probe.as_pandas_df().head(4))
    #   subject_id session  probe  mean_rate_hz
    # 0   mouse_01      s1  imec0      7.233333
    # 1   mouse_02      s1  imec0      6.572222
    # 2   mouse_03      s1  imec0      5.102778
    # 3   mouse_04      s1  imec0      5.510417
    print(per_probe.as_table().num_rows)
    # 16
    ```

**2.** Write a pod `summarize` that takes the list of rates of a session and returns three
values: the number of units, the mean rate, and the highest rate. Apply it per session.

??? success "Solution"
    ```python
    @function_pod(output_keys=["n_units", "mean_rate_hz", "max_rate_hz"])
    def summarize(rate_hz: list[float]) -> tuple[int, float, float]:
        return len(rate_hz), sum(rate_hz) / len(rate_hz), max(rate_hz)


    summary = summarize.pod(per_session)
    print(summary.as_pandas_df().head(3))
    #   subject_id session  n_units  mean_rate_hz  max_rate_hz
    # 0   mouse_01      s1        8      6.033333    11.033333
    # 1   mouse_02      s1        8      5.845833    11.400000
    # 2   mouse_03      s1        8      4.906944     7.877778
    ```

**3.** Compute one number for each genotype: the mean of the session means of its animals.
`genotype` is a data column of `subjects`. (Hint: `session_means.join(...)`, then `tag_by`,
then `group_by`.)

??? success "Solution"
    ```python
    @function_pod(output_keys="genotype_mean_hz")
    def mean_of_means(mean_rate_hz: list[float]) -> float:
        return sum(mean_rate_hz) / len(mean_rate_hz)


    genotype_means = mean_of_means.pod(
        session_means.join(subjects.select_data_columns(["genotype"]))
        .tag_by("genotype")
        .group_by(["genotype"])
    )
    for tag, data in genotype_means:
        print(tag.as_dict()["genotype"], round(data.as_dict()["genotype_mean_hz"], 3))
    # knockout 5.058
    # wildtype 6.545
    ```

    The join brings `genotype` in as a data column, `tag_by` makes it a tag, and `group_by`
    gathers the four session means of each genotype into one list.

    With `genotype` as a tag of the source, `tag_by` is not needed: load the subjects table
    with `tag_columns=["subject_id", "genotype"]`, and `group_by(["genotype"])` works right
    after the join. In your own sources, make a column a tag if you expect to group or match
    by it.
