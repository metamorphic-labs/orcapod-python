# Documentation style

How to write the published documentation, the tutorial above all (ITL-1459). CLAUDE.md,
"Documentation samples" and "Documentation site", has the mechanics.

## Sentences

Write short declarative sentences that say what happens or what to do. Cut any sentence that
announces, justifies or repeats another one.

| Avoid | Before | After |
|---|---|---|
| Announcing what comes next | "Now the surprise. Suppose you edit the function:" | "Suppose you edit the function:" |
| Answering a doubt nobody raised | "63 times for 64 units. That is not a mistake: two units have the same inputs." | "63 times for 64 units, because two units have the same inputs." |
| Denying one thing to state another | "identified by the path, not the content" | "identified by the path" |
| "Always" and "never" that change nothing for the reader | "Refer to columns by name, never by position." | "Refer to columns by name." |
| A colon that only delays the sentence | "The habit that makes these quick to fix: print `keys()`." | "Print `keys()`." |
| An aside after a dash | "The schema is unchanged -- pods never modify tags." | "The schema is unchanged: a pod passes tags through." |
| Selling a feature | "Notice that this is why tags matter: you never wrote the lookup." | (cut) |
| Talking about the page | "These samples only serve to produce the clash." | (cut) |
| Emphasis by contrast | "It finishes. It is not finished successfully." | "A job can finish with failed rows." |

A negative sentence is right when the reader would otherwise act wrongly: "A change after the
dot is **not** enough."

## Headings

- Name the subject: "Nodes", "Reuse rule", "Versioning a pod". A heading does not carry the
  conclusion ("An edited function: nothing is recomputed"), ask a question, or say that one
  thing is another ("A source is a stream").
- A changed heading changes its anchor. Find every link to it with
  `grep -rn '<page>.md#' docs README.md`; the site build checks them.

## Facts

- State a fact once, on the page and at the place where the reader needs it. Elsewhere, link to
  it. A message that every page can show (`ProvisionalHashWarning`) is explained in Setup only.
- Do not introduce what the page does not use. Link to the concept page instead.
- Check each sentence against the code, or run it. Show the output the library prints.
- Cutting words must not cut a trap. These stay wherever they apply: an edited function returns
  its old values; comparisons in a filter need parentheses, and `and` drops a condition without
  an error; a node reads empty before `run()`; a finished job can have failed rows; a stored
  path outlives a deleted file.
- Show a number only when the reader can compare it with something.

## Examples

- Orcapod is general. Describe a feature in general terms, and use the workshop data as the
  example.
- An example or exercise gives the domain facts it needs: "Unit numbers restart at 0 on every
  probe." A reader must not have to know the field to answer.
- Code and text use the names of the columns they produce: `return temp_fahrenheit` for
  `output_keys=["temp_fahrenheit"]`.
- Iterate with `for tag, data in stream`.
- The install lines name a release candidate that has every feature the pages use.

## Tutorial pages

- A part opens with what it adds, then a "You will" list.
- "Key points" ends the text, before the exercises. It repeats what the part taught and adds
  nothing: no new term, link or feature, and no preview of the next part.
- No "Next: ..." line. The site has its own next-page link.
