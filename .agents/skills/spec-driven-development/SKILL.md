---
name: spec-driven-development
description: 'Spec-driven development workflow: draft a Markdown spec before any code, get human approval, implement strictly from the spec, then mark it complete. Use when: planning a new feature, script, or data pipeline step; writing a spec before coding; designing a new component; building from requirements. Triggers: spec, spec-first, sdd, design first.'
argument-hint: 'describe the feature or script to build (e.g. "a script that exports Neo4j data to CSV")'
disable-model-invocation: true
---

# Spec-Driven Development

## Overview

Write the spec first. Get it approved. Implement exactly what the spec says.

**Core principle:** No code without an approved spec. No scope outside the spec.

---

## When to Use

- Planning any new feature, script, or data pipeline step
- Before writing any implementation code
- When a human describes an intent and wants a proper plan before proceeding

---

## Workflow

### Phase 1 — Draft

1. Ask clarifying questions to understand intent if the request is ambiguous.
2. Determine the spec file path: `specs/<area>/<feature-name>.md`
   - Use an existing area folder if one fits (e.g. `specs/data_seeding/`)
   - Create a new area folder if needed
3. Draft the spec using [spec-template.md](./spec-template.md).
4. Fill in all relevant sections. Mark sections as _N/A_ if genuinely not applicable.
5. Present the draft to the human and ask:

   > "Here is the draft spec at `specs/<area>/<feature-name>.md`. Does this capture your intent? Ready to implement, or should we revise?"

### Phase 2 — Review

- **Revision requested**: Edit the spec file, re-present, repeat until approved.
- **Approved**: Proceed to Phase 3.
- Do **not** write any implementation code before approval.

### Phase 3 — Implement

- Follow the spec exactly — phases, constants, schema, edge cases.
- **No scope creep**: if something useful isn't in the spec, add it to the spec first (mini Phase 1/2) before coding it.
- Before writing any code, ask the human: _"Would you like to use test-driven development (write failing tests first, then implement)?"_
  - If **yes**: for each behavior defined in the spec, write a failing test first, then write the minimal code to pass it. Store tests alongside the spec in `specs/<area>/`.
  - If **no**: implement directly from the spec.
- Implement and verify the feature works as specified.

### Phase 4 — Complete

- Add `**Status: Complete**` to the top of the spec file (below the `# Spec:` heading).
- Confirm with the human that the implementation matches the spec.

---

## Quality Gates

| Gate | Criteria |
|------|----------|
| Spec draft complete | All required sections filled; no placeholder text |
| Spec approved | Human explicitly confirms "yes", "looks good", "approved", or similar |
| Implementation complete | All execution phases from spec are implemented |
| No scope creep | Every implemented detail traces back to a spec section |

---

## Anti-Patterns

- **Implement first, spec later** — the spec becomes documentation, not design
- **Vague purpose** — "does stuff" is not a purpose; be specific about inputs, outputs, and side effects
- **Skipping approval** — always get a human sign-off before writing code
- **Expanding scope** — if you think of something new during implementation, stop and update the spec first
