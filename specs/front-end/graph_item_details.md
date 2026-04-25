# Graph Item Details - Front-End Spec

## Goal

When a user selects a node in the graph visualization, show a human-readable description for that title or person in the existing selected-node details panel.

The description must be fetched from the existing back-end endpoint `/query/item-details` and rendered alongside the node metadata already shown in the graph footer.

## Files

| File | Action |
|------|--------|
| `src/hooks/useItemDetails.ts` | Create - TanStack Query hook for `/query/item-details` |
| `src/components/features/GraphVisualization.tsx` | Modify - trigger details lookup from selected node and render description states |

## API Contract Reference

Back-end behavior is already defined in `specs/back-end/item_details.md`.

Front-end request rules:

- Use exactly one query parameter:
	- `titleId=<tt...>` when selected node represents a title
	- `nameId=<nm...>` when selected node represents a person
- Never send both parameters together.

Front-end response shape:

```ts
type ItemDetailsResponse = {
	id: string;
	entityType: "title" | "person";
	description: string;
};
```

## Hook: `useItemDetails`

Follow existing hook patterns used in this project (`useSearch`, `useGraphData`, `useItemsFound`).

### Inputs

```ts
type ItemDetailsParams = {
	titleId?: string | null;
	nameId?: string | null;
};
```

Validation and execution rules:

1. The hook must only be enabled when exactly one valid identifier is present.
2. If both or neither are provided, no request is fired.
3. Query key must include request-shaping params.

Suggested query key:

```ts
["query", "item-details", params]
```

### Return Shape

Hook returns at least:

- `data: ItemDetailsResponse | undefined`
- `isLoading: boolean`
- `isFetching: boolean`
- `isError: boolean`
- `error: Error | null`
- `refetch: () => Promise<unknown>`

## Graph Integration

`GraphVisualization` currently owns `selectedNodeId` and selected-node rendering; this component remains the owner of item-details fetch state.

### Selection -> Request Mapping

When `selectedNode` changes:

1. If node type is `Title`, call `useItemDetails({ titleId: selectedNode.id, nameId: null })`.
2. If node type is `Person`, call `useItemDetails({ titleId: null, nameId: selectedNode.id })`.
3. If selection is cleared, details query is disabled and description block is hidden.

### Rendering Rules in Details Panel

Keep all existing selected-node fields. Add a new Description section at the end of the selected-node card.

Description state matrix:

| State | Condition | Rendering |
|------|-----------|-----------|
| Idle | no selected node | do not render description block |
| Loading | `isLoading` for selected node | show compact loading text (`Loading description...`) |
| Refreshing | `isFetching && !!data` | keep current description visible; optional subtle spinner/label |
| Success | `data?.description` available | render text in readable paragraph style |
| Not Found | HTTP 404 | show neutral message: `Description not found.` |
| Unavailable | HTTP 503 | show warning message: `Description service is temporarily unavailable.` |
| Invalid | HTTP 422 (unexpected from UI flow) | show generic retry-safe message |
| Error | any other failure | show generic fallback message |

The UI must not expose raw backend errors or stack traces.

## Interaction and Concurrency Rules

1. Rapid node switching should always settle on the latest selected node description.
2. Stale responses from older selections must not overwrite the latest visible description.
3. Clicking canvas to clear selection removes the description block immediately.
4. Existing graph drag/zoom behavior and node highlight behavior remain unchanged.

## Accessibility

1. Description section should use semantic text structure (label + body).
2. Loading and error messages should be announced through existing semantic status regions where applicable.
3. Description color contrast must remain readable on current dark surfaces.

## Non-Goals (v1)

- No prefetching descriptions for all visible nodes.
- No markdown or rich-text rendering in descriptions.
- No edits/corrections of description text from UI.
- No additional backend fields beyond current response contract.

## Acceptance Criteria

1. Selecting a title node performs a request to `/query/item-details` with `titleId` only.
2. Selecting a person node performs a request to `/query/item-details` with `nameId` only.
3. Description appears in the selected-node details card after successful response.
4. Existing selected-node metadata remains visible and unchanged.
5. Loading/error/unavailable/not-found states render user-safe messages.
6. Clearing node selection hides the description section.
7. Switching between nodes updates description to the latest selected node.
8. No request is sent when there is no selected node.