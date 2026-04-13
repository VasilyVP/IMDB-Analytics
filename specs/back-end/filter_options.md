# Filter Options API - Development Spec

## Goal
Provide one endpoint that returns all UI filter options for titles in a single response.

## Endpoint
- Method: GET
- Path: /api/titles/filters

## Query Parameters
- `topRated` (optional, boolean, default: `false`)
- `mostPopular` (optional, boolean, default: `false`)

These flags are independent and can be used in any combination.

Examples:
- `/api/titles/filters`
- `/api/titles/filters?topRated=true`
- `/api/titles/filters?mostPopular=true`
- `/api/titles/filters?topRated=true&mostPopular=true`

## Response Shape
```json
{
  "genres": ["Action", "Comedy"],
  "titleTypes": [
    { "value": "movie", "label": "Movie" },
    { "value": "tvSeries", "label": "TV Series" }
  ],
  "yearRange": { "min": 1894, "max": 2025 },
  "ratingRange": { "min": 1.0, "max": 10.0 }
}
```

## Data Rules
0. Source relation selection
- If both flags are `false`, use base tables (`title_basics`, `title_ratings`) as defined below.
- If `topRated=true` and `mostPopular=false`, use `top_rated_titles` view.
- If `topRated=false` and `mostPopular=true`, use `most_popular_titles` view.
- If `topRated=true` and `mostPopular=true`, use `top_rated_popular_titles` view.
- The selected source relation must be used consistently for all four filter groups (`genres`, `titleTypes`, `yearRange`, `ratingRange`).

1. Genres
```sql
SELECT DISTINCT genre
FROM (
  SELECT trim(g.genre) AS genre
  FROM title_basics tb,
  unnest(string_split(tb.genres, ',')) AS g(genre)
  WHERE tb.genres IS NOT NULL
    AND tb.genres <> ''
    AND tb.genres <> '\N'
) x
WHERE genre <> ''
ORDER BY genre;
```

2. Title types
```sql
SELECT DISTINCT titleType
FROM title_basics
WHERE titleType IS NOT NULL
  AND titleType <> ''
  AND titleType <> '\N'
ORDER BY titleType;
```

- Convert title type values to user-friendly labels.
- Example mapping: movie -> Movie, tvSeries -> TV Series.
- Keep unknown values readable using a fallback formatter.

3. Release year range
- Return minimum and maximum valid startYear.
- Cap the maximum at `min(max_year, current_year + 5)` to exclude far-future outliers.
- Ignore NULL and '\N' values.

4. Rating range
- Return minimum and maximum averageRating from title_ratings.
- Ignore NULL values.

## Performance and Caching
- Fetch the four filter groups in parallel in the service layer.
- Cache the final endpoint response in memory after first successful load.
- Cache keys must include query parameter state, so each flag combination is cached independently.
- Return cached data on subsequent requests.
- Suggested approach: functools.lru_cache (simple) or equivalent FastAPI-compatible in-memory cache.

## Acceptance Criteria
- Single request returns all filter options in one payload.
- Endpoint supports `topRated` and `mostPopular` boolean query parameters.
- `topRated` and `mostPopular` are independent (all 4 combinations are valid).
- Correct view/table source is selected for each flag combination.
- Empty/null/sentinel values are excluded.
- Title type labels are user-friendly.
- First request per flag combination loads from DB; later requests for the same combination use cache.
- Endpoint remains stable when optional datasets are sparse.
