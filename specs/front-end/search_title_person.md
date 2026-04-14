# Search Autocomplete — Front-End Spec

## Goal

Replace the static search `Input` in the Filter panel's Search section with an autocomplete component that queries the `/query/search` API as the user types and displays matching titles and people in a dropdown.

## Files

| File | Action |
|------|--------|
| `src/hooks/useSearch.ts` | Create — TanStack Query hook for `/query/search` |
| `src/components/features/SearchAutocomplete.tsx` | Create — controlled search input + dropdown |
| `src/components/features/FilterPanel.tsx` | Modify — replace Search section; add two props |
| `src/routes/Analytics/Analytics.tsx` | Modify — add `selectedSearchResult` to `FilterState` initial value |

## Hook: `useSearch`

Follow the same pattern as `useFilterOptions` (TanStack Query, `getFetcher`, `keepPreviousData`).

```ts
type SearchResultItem = {
  id: string;
  name?: string;
  primaryTitle?: string;
};

type SearchResponse = {
  results: SearchResultItem[];
};

type SearchParams = {
  q: string;
  topRated: boolean;
  mostPopular: boolean;
  minRating?: number | null;
  maxRating?: number | null;
  startYearFrom?: number | null;
  startYearTo?: number | null;
  genre?: string | null;
  titleType?: string | null;
};
```

- Query key: `["search", params]` — include all request-shaping params.
- `enabled: params.q.trim().length >= 3` — no fetch below 3 characters.
- Pass filter state fields through to the API: `ratingRange[0]` → `minRating`, `ratingRange[1]` → `maxRating`, `yearRange[0]` → `startYearFrom`, `yearRange[1]` → `startYearTo`.

## FilterState addition

Add `selectedSearchResult` to the existing `FilterState` type in `FilterPanel.tsx` and to `INITIAL_FILTER_STATE` in `Analytics.tsx`:

```ts
type FilterState = {
  // ...existing fields...
  selectedSearchResult: SearchResultItem | null;
};

const INITIAL_FILTER_STATE = {
  // ...existing fields...
  selectedSearchResult: null,
};
```

## Component: `SearchAutocomplete`

Replaces the entire Search section block (the `<div className="space-y-3">` containing `Label` + `Input`) in `FilterPanel`. Visual output at idle state is identical to what existed before.

### Props

```ts
type SearchAutocompleteProps = {
  value: string;
  onChange: (value: string) => void;
  filters: FilterState;
  selectedItem: SearchResultItem | null;
  onSelect: (item: SearchResultItem | null) => void;
};
```

### Structure

```
<div className="space-y-3" ref={wrapperRef}>          // outer — same as old section
  <Label className="text-xs text-neutral-400">Search</Label>
  <div className="relative">
    <Input className="bg-neutral-900 border-neutral-800" ... />
    {showDropdown && (
      <div className="absolute top-full left-0 right-0 z-50 mt-1
                      rounded-md border border-neutral-800 bg-neutral-900 shadow-lg overflow-hidden">
        {/* rows */}
      </div>
    )}
  </div>
</div>
```

### Behaviour

- **Debounce**: 300 ms via `useEffect` + `setTimeout`/`clearTimeout`. `debouncedQuery` drives the hook.
- **Dropdown visibility**: open when `isOpen && debouncedQuery.trim().length >= 3`.
- **On input change**: call `onChange`, clear `selectedItem` if one is set, set `isOpen = true`.
- **On input focus**: open dropdown if `value.trim().length >= 3`.
- **On click outside**: close dropdown (`mousedown` listener via `useEffect` + `useRef`).
- Use `onMouseDown={e => e.preventDefault()}` on result buttons to prevent the input blur from closing the dropdown before `onClick` fires.

### Dropdown rows

| State | Content |
|-------|---------|
| `isLoading` | Single row: `"Searching…"` (`text-neutral-500`) |
| `isError` | Single row: `"Search failed"` (`text-red-400`) |
| No results | Single row: `"No results"` (`text-neutral-500`) |
| Results | One `<button>` per item (see below) |

Each result button:
- Full-width, `flex items-center justify-between px-3 py-2 text-xs text-neutral-300 hover:bg-neutral-800 transition-colors`.
- Left: truncated label (`item.primaryTitle ?? item.name`).
- Right: small badge `text-[10px] text-neutral-500` — `"Title"` when `primaryTitle` is present, `"Person"` when `name` is present.
- On click: call `onSelect(item)`, set input value to item's label, close dropdown.

## FilterPanel changes

- Remove the `Input` import (no longer used directly).
- Add import: `SearchAutocomplete` from `@/components/features/SearchAutocomplete`, `SearchResultItem` from `@/hooks/useSearch`.
- No new props — `selectedSearchResult` is already available via `filters.selectedSearchResult` and updated through `setFilters`.
- Replace the `{/* Search */}` section with:

```tsx
{/* Search */}
<SearchAutocomplete
  value={filters.search}
  onChange={(v) => handleFieldChange("search", v)}
  filters={filters}
  selectedItem={filters.selectedSearchResult}
  onSelect={(item) => setFilters((draft) => { draft.selectedSearchResult = item; })}
/>
```

## Analytics changes

- Import `SearchResultItem` from `@/hooks/useSearch`.
- Add `selectedSearchResult: SearchResultItem | null` to the `FilterState` type in `FilterPanel.tsx`.
- Add `selectedSearchResult: null` to `INITIAL_FILTER_STATE` in `Analytics.tsx`.
- No additional state or props needed — `setFilters` already propagates the change.

## Acceptance Criteria

1. Typing 1–2 characters makes no API call and shows no dropdown.
2. Typing 3+ characters triggers a debounced (≥ 300 ms) API call and opens the dropdown.
3. Loading row is shown while the request is in flight.
4. Error row is shown when the request fails.
5. "No results" row is shown when the API returns an empty list.
6. Each result row shows the correct label and a "Title" or "Person" badge.
7. Clicking a result closes the dropdown, fills the input with the item's label, and updates `selectedSearchResult` in `Analytics`.
8. Typing again after a selection clears `selectedSearchResult` and triggers new autocomplete.
9. Clicking outside the component closes the dropdown.
10. Active filter toggles (`topRated`, `mostPopular`) and filter ranges are forwarded to the search API.
11. At idle (no dropdown), the Search section looks identical to the previous plain `Input` layout.
