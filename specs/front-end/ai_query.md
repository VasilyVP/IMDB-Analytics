# AI Query - Front-End Spec

## Overview

This specification adds an AI-assisted query flow to the Analytics sidebar.
Users can describe a person, a title, or a recommendation request in natural language, and the front-end will translate the response from `/query/human-to-llm` into the existing search-and-graph workflow.

The feature must reuse the current Analytics-owned filter state and the current graph request path instead of introducing a separate graph-fetch implementation.

## Goal

Provide a natural-language AI Query section that:

- sits above the existing Filters section in the right sidebar
- is expanded by default while Filters is collapsed by default
- sends the user request to `POST /query/human-to-llm`
- shows a spinner while the request is in flight
- automatically applies a concrete title/person result to the existing filter state and triggers graph loading
- renders selectable `Options` when the API returns multiple candidate or recommendation results
- keeps the existing Filters flow available and controlled by `Analytics`

## Scope

Included:

- right-panel layout changes for Status, AI Query, and Filters
- front-end request flow for `/query/human-to-llm`
- AI result-to-filter mapping
- option rendering and click behavior
- loading, empty, and error states
- graph-trigger integration using the same request path as the manual graph action

Excluded:

- backend API or schema changes
- conversational memory or chat history
- streaming token output
- URL persistence of AI query text

## Files

| File | Action |
|------|--------|
| `src/hooks/useHumanToLlmQuery.ts` | Create - TanStack mutation hook for `POST /query/human-to-llm` |
| `src/components/features/AiQueryPanel.tsx` | Create - AI query form, loading state, response handling, and options list |
| `src/components/features/FilterPanel.tsx` | Modify - restructure layout into Status + AI Query + Filters accordion sections |
| `src/routes/Analytics/Analytics.tsx` | Modify - expose one helper that applies updated filters and triggers graph fetch from the same snapshot |

If the project does not already have accordion primitives available, use the existing shadcn accordion pattern for the two collapsible sections.

## Backend Contract Reference

Back-end behavior is defined in `specs/back-end/human_to_llm_query.md`.

Front-end request body:

```ts
type HumanToLlmRequest = {
	query: string;
	limit?: number;
};
```

Front-end response shape:

```ts
type HumanToLlmQueryType =
	| "person_search"
	| "film_search"
	| "person"
	| "film"
	| "recommendation";

type HumanToLlmParsed = {
	role: "actor" | "director" | null;
	name: string | null;
	title: string | null;
	details: string | null;
};

type HumanToLlmResultItem = {
	id: string;
	label: string;
	entityType: "person" | "title";
	birthYear: number | null;
	startYear: number | null;
	score: number | null;
};

type HumanToLlmResponse = {
	type: HumanToLlmQueryType;
	parsed: HumanToLlmParsed;
	results: HumanToLlmResultItem[];
};
```

The front-end must treat the API response as authoritative. It must render option rows according to `entityType` from each result item rather than inferring a different item type from `response.type` alone.

## State Ownership and Graph Triggering

`Analytics` remains the single owner of canonical `FilterState`, submitted graph filters, and graph request token.

This feature must not update filters and then call the existing manual graph callback separately, because that can issue a graph request from stale state.

Required integration rule:

- `Analytics` must expose one helper that receives a filter-state updater, computes the next filter snapshot, commits that snapshot to `filters`, mirrors it to `submittedGraphFilters`, and increments `graphRequestToken`.

Suggested contract:

```ts
type ApplyFiltersAndShowGraph = (updater: (draft: FilterState) => void) => void;
```

This helper becomes the only path used by:

- the manual Show graph action
- AI auto-apply for `person` and `film`
- AI option clicks for `recommendation`, `person_search`, and `film_search`

## Layout and Visual Structure

The right panel should render content in this order:

1. Status block
2. AI Query accordion item
3. Filters accordion item

### Status Block

The current Titles Found / Persons Found summary moves out of the Filters section and remains always visible above AI Query.

Requirements:

- Keep the current counts and loading indicator behavior from `useItemsFound`.
- Keep the counts outside both accordion sections.
- Keep a compact manual graph action in this always-visible area so the existing manual filter workflow still has an explicit trigger.

### Accordion Rules

- AI Query and Filters are rendered as two accordion items.
- AI Query is expanded by default.
- Filters is collapsed by default.
- In the default collapsed state, Filters should visually sit at the bottom of the sidebar.
- Achieve the bottom placement through layout structure, not absolute positioning.

Recommended layout approach:

- `FilterPanel` remains a `flex h-full flex-col` container.
- Place Status and AI Query near the top.
- Place the collapsed Filters accordion item after a flexible spacer so it rests near the bottom when closed.

## Component: `AiQueryPanel`

`AiQueryPanel` is a focused UI component responsible for user input, mutation state, and AI option rendering.

### Props

```ts
type AiQueryPanelProps = {
	filters: FilterState;
	setFilters: (updater: (draft: FilterState) => void) => void;
	applyFiltersAndShowGraph: (updater: (draft: FilterState) => void) => void;
	isGraphLoading: boolean;
};
```

### Input UI

Requirements:

- Use a single natural-language text input control suitable for sentence-like queries.
- Allow submit by button click and Enter.
- Trim input before submission.
- Prevent submission for empty or whitespace-only input.
- Preserve the input text after successful completion so the user can see what was asked.

Suggested label and affordances:

- Section label: `AI Query`
- Input placeholder: `Ask for a title, person, or recommendation...`
- Submit button text: `Ask AI`

## Hook: `useHumanToLlmQuery`

Use TanStack Query mutation semantics because this is a user-triggered request, not passive background state.

Requirements:

- Send `POST /query/human-to-llm`
- Use the existing HTTP helper pattern used elsewhere in the front-end
- Default `limit` to `10` unless the implementation exposes a different constant intentionally
- Account for the current backend rate limit of `1 request / second`
- Return standard mutation state for:
	- `mutate` or `mutateAsync`
	- `isPending`
	- `isError`
	- `error`
	- `data`

## Response Handling Rules

### Shared Rules

On every new submit:

- clear any previously rendered `Options`
- clear any prior AI-specific error message
- keep current filters unchanged until the response is handled

Map a result item into the existing search selection shape as follows:

```ts
type SearchResultItem = {
	id: string;
	name?: string;
	primaryTitle?: string;
};
```

Mapping rules:

- `entityType === "title"` -> `{ id, primaryTitle: label }`
- `entityType === "person"` -> `{ id, name: label }`

Whenever a result is applied to filters:

- `filters.search` becomes `result.label`
- `filters.selectedSearchResult` becomes the mapped `SearchResultItem`
- all non-search filters remain unchanged

### Case 1: `person` or `film`

If `response.type` is `person` or `film` and at least one result exists:

- apply the first returned result immediately through `applyFiltersAndShowGraph`
- do not render an `Options` list
- let the normal graph loading UI take over

Implementation note:

- The current backend implementation may still return multiple rows for `person` and `film` lookups because person lookup has a prefix-match fallback and film lookup uses substring matching with `limit`.
- When this happens, the front-end should treat the first returned row as the backend-ranked match for the auto-apply flow.
- The front-end must not re-rank or guess a different preferred result client-side.

If no results are returned:

- do not change filters
- show a neutral empty-result message below the input

### Case 2: `recommendation`

If `response.type` is `recommendation`:

- render a new section titled `Options` below the AI query input
- render one row per returned item
- show the item label and one year field:
	- `startYear` for title results
	- `birthYear` for person results
- clicking a row applies that result to filters and triggers graph loading

### Case 3: `person_search` or `film_search`

If `response.type` is `person_search` or `film_search`:

- render the same `Options` section used for recommendations
- render items according to the `entityType` carried by each result row
- clicking a row applies that result to filters and triggers graph loading

Implementation note:

- even though the request type is classified as person or film search, the UI should still trust `entityType` per result item when displaying year metadata and mapping the click action

## Options List Rendering

The `Options` block appears only when the most recent successful AI response requires explicit user choice.

Each row must:

- be fully clickable
- show the result label on the left
- show the relevant year metadata inline
- show a `Show graph` action aligned to the right of the label area on hover
- trigger the same apply-and-show behavior whether the user clicks the row or the hover action

Suggested row content:

```text
Nicolas Cage                 1964   [Show graph]
The Matrix                   1999   [Show graph]
```

Interaction rules:

- the hover-only action must not be the only clickable affordance; the whole row remains clickable
- keyboard users must still be able to activate the row without hover
- once a row is chosen, the `Options` block may remain visible until the next submit, but the chosen row must apply immediately

## Filters Section Content

The existing controlled filter controls move under the collapsed `Filters` accordion item.

This includes:

- Search autocomplete
- Quick Queries
- IMDB Rating range
- Release Year range
- Genre
- Type

The existing bottom full-width graph button should no longer be the primary placement for the feature.
Manual graph triggering should be available from the always-visible Status area instead.

## Loading, Empty, and Error States

### Loading

While the AI request is pending:

- show a spinner in the AI Query section
- disable repeated submit action
- keep current filters and graph unchanged until the response resolves

### Empty Results

If the API returns `200` with an empty `results` array:

- show a neutral empty state such as `No options found.`
- do not clear or mutate current filters

### Errors

Show user-safe inline messages in the AI Query section.

Recommended handling:

- `422` -> `Enter a more specific request.`
- `429` -> `Please wait a moment before asking again.`
- `502` -> `The AI response could not be understood. Try again.`
- `503` -> `AI search is temporarily unavailable.`
- any other failure -> `AI query failed. Try again.`

Do not expose raw backend payloads or stack traces.

Rate-limit behavior:

- Because the endpoint is currently limited to `1 request / second`, the UI should avoid rapid repeated submissions while a request is pending.
- If the backend still returns `429`, keep the current filters unchanged and show the user-safe rate-limit message inline.

## Interaction with Existing Search and Counts

After an AI result is applied:

- the controlled Search autocomplete must reflect the chosen label through `filters.search`
- `filters.selectedSearchResult` must be updated so graph, item counts, and later manual actions operate on the same anchor
- the existing `useItemsFound` flow should update counts through its current debounced behavior
- the existing graph request logic must remain unchanged apart from using the new `applyFiltersAndShowGraph` helper

## Accessibility

1. The AI Query section must expose a labeled input and a clearly labeled submit action.
2. Loading and error states must be readable by assistive technology.
3. Option rows must be keyboard reachable and activatable.
4. Hover-only `Show graph` affordances must not hide the primary action from keyboard users.

## Non-Goals

- No conversational thread or follow-up turns.
- No automatic retry loop.
- No persistence of AI query history.
- No backend changes to result ranking.

## Acceptance Criteria

1. The sidebar shows `Titles Found` and `Persons Found` above AI Query, outside the Filters accordion.
2. AI Query and Filters are rendered as accordion sections, with AI Query open by default and Filters closed by default.
3. In the initial collapsed layout, Filters visually sits near the bottom of the sidebar.
4. Submitting a non-empty AI request sends `POST /query/human-to-llm` and shows a spinner while waiting.
5. When the API returns `person` or `film` with at least one result, the front-end applies the first returned result to `filters.search` and `filters.selectedSearchResult` and triggers graph loading immediately.
6. When the API returns `recommendation`, the front-end renders an `Options` section below the input with label plus `startYear` or `birthYear`.
7. When the API returns `person_search` or `film_search`, the front-end renders the same `Options` section and maps each row using that row's `entityType`.
8. Clicking an option row updates filters and triggers the same graph request path as the manual Show graph action.
9. The AI flow does not issue a graph request from stale filter state.
10. Existing non-search filters remain unchanged when AI applies a title or person anchor.
11. AI loading, empty, and error states are shown inline without exposing raw backend errors.
12. After AI applies a result, the existing Search autocomplete and item-count flows reflect the updated selection.
13. If the backend returns `429`, the front-end shows an inline rate-limit message and leaves current filters unchanged.