# Triage Labels

`/triage` assigns every issue **one category role and one state role**. This file maps those canonical roles to the actual label strings used in this repo's issue tracker.

## Category roles

| Canonical role | Label in our tracker | Meaning                    |
| -------------- | -------------------- | -------------------------- |
| `bug`          | `bug`                | Something is broken        |
| `enhancement`  | `enhancement`        | New feature or improvement |

## State roles

| Canonical role    | Label in our tracker | Meaning                                  |
| ----------------- | -------------------- | ---------------------------------------- |
| `needs-triage`    | `needs-triage`       | Maintainer needs to evaluate this issue  |
| `needs-info`      | `needs-info`         | Waiting on reporter for more information |
| `ready-for-agent` | `ready-for-agent`    | Fully specified, ready for an AFK agent  |
| `ready-for-human` | `ready-for-human`    | Requires human implementation            |
| `wontfix`         | `wontfix`            | Will not be actioned                     |

When a skill mentions a role (e.g. "apply the AFK-ready triage label"), use the corresponding label string from this table.

## Wayfinder labels

Not triage roles — used only by `/wayfinder`, which fails if they don't exist:

| Label                 | Meaning                                              |
| --------------------- | ---------------------------------------------------- |
| `wayfinder:map`       | The map issue: index of a journey's tickets           |
| `wayfinder:research`  | Child ticket: resolve an open question                |
| `wayfinder:prototype` | Child ticket: throwaway build answering a design question |
| `wayfinder:grilling`  | Child ticket: stress-test a plan or decision          |
| `wayfinder:task`      | Child ticket: specified work, ready to implement      |

Edit the right-hand column of the role tables to match whatever vocabulary you actually use.
