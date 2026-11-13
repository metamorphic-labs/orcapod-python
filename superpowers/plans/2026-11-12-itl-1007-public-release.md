# ITL-1007: make the repository ready for public release from nauticalab

Steps follow the coordinator brief (1 ids, 2 workshop, 3 URLs, 4 workflows, 5 lint workflow,
6 RELEASING.md, 7 guard test first, 8 changelog fragment).

Order: guard test (red) -> scrub ids -> remove workshop -> URLs -> workflows -> RELEASING.md
and agent instructions -> fragment -> full suite -> PR.

Facts checked: nauticalab main is 61d0f18e, an ancestor of our main (52 commits behind);
nauticalab has `tests.yml`, present at 61d0f18e and deleted on our main by 07e5a2ea (#267), so a
fast-forward push deletes it. First commit of the repo: c5d0963e (2025-04-25); 1866 commits on main.

Notes for a successor: placeholders are `<project id>` (prose) and `my-project-123456` (code).
