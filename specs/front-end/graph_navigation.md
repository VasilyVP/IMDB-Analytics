# Graph Navigation - Front-End Spec

## Goal

Allow users to navigate the graph by double-clicking a node so the clicked title or person becomes the active search anchor in Analytics and the graph reloads for that new anchor.

This interaction is a navigation action, not just a local selection action.

## Scope and Decisions (v1)

- Single click keeps its current meaning: select a node and show details.
- Double click performs graph navigation.
- Navigation updates the Analytics-owned search filter state.
- Navigation immediately triggers the same graph fetch flow as clicking Show graph.
- Existing non-search filters remain unchanged.

## Files

| File | Action |
|------|--------|
| `src/components/features/GraphVisualization.tsx` | Modify - detect node double click and notify parent with navigation payload |
| `src/routes/Analytics/Analytics.tsx` | Modify - apply navigation payload to filter state and trigger graph refetch |
| `src/components/features/FilterPanel.tsx` | No structural change required - controlled inputs should reflect updated Analytics state |

## Navigation Contract

Double-clicking a graph node must update the current search anchor represented by `filters.search` and `filters.selectedSearchResult`.

The clicked node maps to the existing search-selection shape:

```ts
type SearchResultItem = {
	id: string;
	name?: string;
	primaryTitle?: string;
};
```

Node mapping rules:

- Title node:
	- `filters.search = node.label`
	- `filters.selectedSearchResult = { id: node.id, primaryTitle: node.label }`
- Person node:
	- `filters.search = node.label`
	- `filters.selectedSearchResult = { id: node.id, name: node.label }`

Because Analytics already stores only one active search selection, navigation replaces the previous selected title or person automatically. No additional title-only or person-only filter fields are introduced.

## State Ownership

`Analytics` remains the single owner of canonical filter state.

Required ownership rules:

1. `GraphVisualization` must not mutate filter state directly.
2. `GraphVisualization` emits a navigation callback with the clicked node payload.
3. `Analytics` applies the filter-state update.
4. `Analytics` then updates submitted graph filters and increments `graphRequestToken`.
5. `FilterPanel` reflects the new search state through existing controlled props.

## Component Contract: `GraphVisualization`

Add a navigation callback prop owned by `Analytics`.

Suggested prop shape:

```ts
type GraphNavigationTarget = {
	id: string;
	label: string;
	type: "Title" | "Person";
};

type GraphVisualizationProps = {
	// existing props...
	onNavigateToNode: (target: GraphNavigationTarget) => void;
};
```

Interaction rules:

1. Single click continues to select a node only.
2. Double click on a node calls `onNavigateToNode` with that node's `id`, `label`, and `type`.
3. Double click on empty canvas does nothing.
4. Double click must not require the node to be pre-selected.
5. Navigation should work for both anchor and non-anchor nodes.

## Analytics Integration

`Analytics` already owns:

- `filters`
- `submittedGraphFilters`
- `graphRequestToken`

Navigation must reuse the existing Show graph request model instead of introducing a second fetch pathway.

Required flow:

1. Receive `GraphNavigationTarget` from `GraphVisualization`.
2. Update `filters.search` and `filters.selectedSearchResult` to match the clicked node.
3. Preserve all other filters (`topRated`, `mostPopular`, `genre`, `titleType`, `ratingRange`, `yearRange`).
4. Rebuild `submittedGraphFilters` from the updated filter state.
5. Increment `graphRequestToken` so `useGraphData` fetches the new graph.

Implementation requirement:

- The graph refetch triggered by navigation must be equivalent to clicking Show graph after manually selecting the same title or person in the search autocomplete.

## FilterPanel Behavior

No new FilterPanel-specific state is introduced.

Required visible behavior after navigation:

1. The Search input shows the clicked node label.
2. The controlled selected search item matches the clicked node type.
3. Existing item counts update through the normal debounced `useItemsFound` flow.
4. Existing Show graph enablement rules remain unchanged for manual use.

## Query and Refetch Behavior

Navigation-triggered graph reload must use the existing graph query contract.

Rules:

1. The new graph request uses the same active non-search filters as before navigation.
2. The request changes only the anchor portion of the query:
	 - Title navigation results in `titleId=<clicked id>` and no `nameId`
	 - Person navigation results in `nameId=<clicked id>` and no `titleId`
3. Navigation must issue exactly one graph refetch per completed double-click action.
4. The graph should render the new response when the fetch completes.

## UX Rules

1. Keep current single-click selection and details behavior unchanged.
2. A double-clicked node may remain selected, but navigation is the primary effect.
3. If the navigation-triggered request is loading, existing loading behavior for graph refresh remains in effect.
4. The interaction should feel immediate; no extra confirmation step is introduced.

## Error Handling

If the navigation-triggered graph request fails:

1. Preserve the updated filter state in Analytics.
2. Show the existing graph error state.
3. Retry must rerun the same navigation-derived graph request.

## Accessibility

1. Double-click behavior must not remove existing keyboard-accessible graph interactions.
2. If a keyboard navigation equivalent is added later, it must trigger the same `onNavigateToNode` callback and Analytics flow.
3. Search field updates caused by navigation must remain visible in the controlled FilterPanel UI.

## Non-Goals (v1)

- No backend API changes.
- No new dedicated title filter or person filter fields.
- No breadcrumb/history stack for graph navigation.
- No auto-clearing of genre, title type, rating, year, or quick-query toggles during navigation.
- No navigation action on single click.

## Acceptance Criteria

1. Double-clicking a title node updates the Analytics search state to that title and refetches graph data.
2. Double-clicking a person node updates the Analytics search state to that person and refetches graph data.
3. The Search input reflects the clicked node label after navigation.
4. The selected search result reflects the clicked node type using the existing `SearchResultItem` shape.
5. Existing non-search filters remain unchanged after navigation.
6. Single-click node selection and item-details behavior still work as before.
7. The graph request issued after navigation is equivalent to a manual search selection followed by Show graph.
8. Failed navigation-triggered requests show the existing graph error UI without losing the updated search state.
9. Double-clicking empty canvas does not change filters or trigger a fetch.