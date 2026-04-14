# Title and Person Search API - Development Spec

## Goal
Provide one autocomplete endpoint that returns matching titles and people in a unified response.

## Endpoint
- Method: GET
- Path: /query/search

## Query Parameters
- `q` (required, string)
- `limit` (optional, integer, default: `10`, max accepted: `50`)
- `topRated` (optional, boolean, default: `false`)
- `mostPopular` (optional, boolean, default: `false`)
- `minRating` (optional, float in range `1.0..10.0`)
- `maxRating` (optional, float in range `1.0..10.0`)
- `startYearFrom` (optional, integer year)
- `startYearTo` (optional, integer year)
- `genre` (optional, string)
- `titleType` (optional, string)

### Validation Rules
1. `q` is required.
2. Accepted `q` length is minimum `3` and maximum `20` characters.
3. `q` is trimmed before validation.
4. Whitespace-only `q` is invalid.
5. `limit` must be between `1` and `50`; values outside this range return a `422` validation error.
6. If both `minRating` and `maxRating` are provided, `minRating <= maxRating` must hold.
7. If both `startYearFrom` and `startYearTo` are provided, `startYearFrom <= startYearTo` must hold.

Examples:
- `/query/search?q=shaw&limit=10`
- `/query/search?q=nolan&topRated=true`
- `/query/search?q=brad&topRated=true&mostPopular=true&limit=30`
- `/query/search?q=dark&minRating=8.0&maxRating=10&startYearFrom=1990&startYearTo=2010`
- `/query/search?q=christopher&genre=Drama&titleType=movie`

## Response Shape
```json
{
	"results": [
		{
			"id": "tt0111161",
			"primaryTitle": "The Shawshank Redemption"
		},
		{
			"id": "nm0000209",
			"name": "Tim Robbins"
		}
	]
}
```

### Result Item Contract
- Shared:
  - `id`: unique identifier (`tconst` for titles, `nconst` for people)
- Person results:
  - `name`: person display label (`primaryName`)
- Title results:
  - `primaryTitle`: localized/primary title for UI display
- `type` is intentionally omitted. Result kind is inferred from present fields (`name` for people, `primaryTitle` for titles).

## Data Rules
0. Source relation selection (for title-derived filtering)
- If both flags are `false`, use base title relations.
- If `topRated=true` and `mostPopular=false`, use `top_rated_titles`.
- If `topRated=false` and `mostPopular=true`, use `most_popular_titles`.
- If `topRated=true` and `mostPopular=true`, use `top_rated_popular_titles`.
- Apply all additional filters (`minRating`, `maxRating`, `startYearFrom`, `startYearTo`, `genre`, `titleType`) on top of the selected source relation.

1. Title candidates
- Search `title_basics.primaryTitle` case-insensitively.
- Include matches where `primaryTitle` starts with `q` or contains `q`.
- Restrict matches by optional filters:
	- Rating range on joined `title_ratings.averageRating`.
	- Start year range on `title_basics.startYear`.
	- Genre inclusion against exploded `title_basics.genres` tokens.
	- Title type inclusion against `title_basics.titleType`.

2. Person candidates
- Search `name_unique.primaryName` case-insensitively.
- Include matches where `primaryName` starts with `q` or contains `q`.
- When any title-derived filters are active, only include persons linked to at least one title satisfying those filters (via `title_principals`).
- When no filters are active, all matching persons are included without restriction.

3. Ordering
- Results are ordered by `titleType`: persons sort before titles.
- No relevance or popularity ranking is applied within each group.

4. Result merge and limit
- Merge title and person candidates into one `results` array.
- Apply the validated `limit` to the merged output.

## Error Behavior
- Invalid query length (`q` shorter than 3 or longer than 20) returns validation error.
- Missing `q` returns validation error.
- Whitespace-only `q` returns validation error.
- Invalid range combinations (`minRating > maxRating`, `startYearFrom > startYearTo`) return validation error.
- Recommended status code for validation failures: `422` (FastAPI query validation).

## Non-Goals
- This endpoint does not return full detail payloads for titles or people.
- This endpoint does not perform fuzzy typo correction in v1.
