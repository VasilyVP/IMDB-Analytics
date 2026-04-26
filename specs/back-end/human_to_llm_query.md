# Human to LLM Query API - Development Spec

## Goal
Provide a natural-language query endpoint that:
- Accepts one free-form user query.
- Uses an LLM to classify intent and extract structured fields.
- Routes the query to ChromaDB similarity search or DuckDB lookup based on the classified type.
- Returns normalized results for titles and persons.

## Endpoint
- Method: POST
- Path: /query/human-to-llm

## Request Body

```json
{
  "query": "recommend me actors similar to bruce willis",
  "limit": 10
}
```

### Request Fields
| Field | Type | Required | Description |
|---|---|---|---|
| query | string | yes | User free-form query |
| limit | integer | no | Max results to return (default: 10, allowed: 1..50) |

### Validation Rules
1. query is required.
2. query is trimmed before validation.
3. Empty or whitespace-only query returns 422.
4. limit must be in range 1..50; otherwise 422.

## Response Shape

```json
{
  "type": "recommendation",
  "parsed": {
    "role": "actor",
    "name": "Bruce Willis",
    "title": null,
    "details": "actors similar to Bruce Willis, action thriller style"
  },
  "results": [
    {
      "id": "nm0000246",
      "label": "Nicolas Cage",
      "entityType": "person",
      "birthYear": 1964,
      "startYear": null,
      "score": 0.9123
    }
  ]
}
```

### Response Fields
| Field | Type | Description |
|---|---|---|
| type | string | Classified query type |
| parsed | object | Structured LLM output |
| results | array | Routed search/lookup results |

### Allowed type values
- person_search
- film_search
- person
- film
- recommendation

### Parsed Contract
| Field | Type | Allowed values |
|---|---|---|
| role | string or null | actor, director, null |
| name | string or null | Specific person name if present |
| title | string or null | Specific film title if present |
| details | string or null | Semantic search phrase for similarity modes |

### Result Item Contract
| Field | Type | Description |
|---|---|---|
| id | string | nconst for people, tconst for titles |
| label | string | primaryName for people or primaryTitle for titles |
| entityType | string | person or title |
| birthYear | integer or null | Required in every result item; set for `entityType=person`, otherwise null |
| startYear | integer or null | Required in every result item; set for `entityType=title`, otherwise null |
| score | float or null | Similarity score for Chroma results; null for exact DB lookups |

## LLM Structured Output Contract
Use strict structured outputs (Pydantic model or equivalent schema) and reject non-conforming responses.

### LLM Decision Rules
1. If a specific person is requested, classify as person.
2. If a specific film is requested, classify as film.
3. If a person is described without a specific name, classify as person_search.
4. If a film is described without a specific title, classify as film_search.
5. If similarity/suggestion language is present, classify as recommendation.

### LLM Extraction Rules
1. role is set only if explicit or strongly implied.
2. name is set only for a specific person.
3. title is set only for a specific film.
4. details is set only for person_search, film_search, recommendation.
5. details should be short, semantic, and at most 20 words.
6. Missing values are null, never empty string.

## Routing and Retrieval Rules

### 1. person_search or film_search
- Default: use ChromaDB similarity search with parsed.details.
- Override for person_search: if both parsed.name and parsed.role are present, use DuckDB person lookup (same flow as `person`) to favor precise identity matches.
- Apply category filter when parsed.role is present:
  - actor -> category=actor
  - director -> category=director
- Similarity path returns top N ordered by similarity score descending, and includes birthYear/startYear according to entityType when available.
- DuckDB override path returns normalized person rows with score=null.

### 2. person
- Use DuckDB lookup by parsed.name.
- Query only person records with a non-null birthYear.
- If parsed.role is present, apply category/role restriction.
- Matching strategy:
  - first pass: case-insensitive exact match
  - second pass fallback: case-insensitive prefix match
- Return normalized id + label + birthYear, with startYear=null and score=null.

### 3. film
- Use DuckDB lookup by parsed.title.
- Matching strategy: single pass, case-insensitive substring match using `ILIKE '%{title}%'`.
- Filter to titleType `movie` or `tvSeries` only.
- startYear must be non-null.
- Order results by titleType ASC, startYear DESC.
- Return normalized id + label + startYear, with birthYear=null and score=null.

### 4. recommendation
- Use ChromaDB similarity search with parsed.details.
- If parsed.details is null/empty, fallback to original query text.
- Apply category filter when parsed.role is present.
- Return top N ordered by similarity score descending, and include birthYear/startYear according to entityType when available.

## N-Tier Structure
| Layer | Responsibility |
|---|---|
| endpoints/ | HTTP validation, request model binding, response model return |
| services/ | LLM call, response validation, routing decision, result normalization |
| repositories/ | DuckDB SQL templates and Chroma query wrappers |
| schemas/ | Request, LLM parsed model, result item, response model |

### Suggested module layout
- endpoints/query.py: add POST /human-to-llm handler
- services/human_to_llm_query_service.py
- repositories/human_to_llm_repository.py
- schemas/human_to_llm_query.py
- prompts/human_to_llm_query_system.txt

## SQL and Chroma Notes
1. DuckDB repository SQL should follow existing template style with module-level constants.
2. Case-insensitive lookup should use normalized comparisons (lower(...)).
3. Chroma similarity search should request at least limit candidates and map metadata safely.
4. Returned labels must always be non-empty; drop malformed records.
5. Result normalization must always include both `birthYear` and `startYear` keys (one may be null based on entityType).

## Error Behavior
- 422: invalid input (empty query, invalid limit).
- 502: LLM response cannot be parsed into required structured schema.
- 503: ChromaDB or DuckDB temporarily unavailable.
- 200 with empty results: valid request but no matches.

## Performance and Safety
1. Set a strict timeout for LLM request and fail fast on timeout.
2. Clamp limit to validated maximum.
3. Do not execute dynamic SQL from LLM output; only bind parameters.
4. Keep prompt and parser deterministic to reduce type drift.

## Example Cases
Input: who is Christopher Nolan
- type: person
- parsed.role: director
- retrieval: DuckDB person lookup

Input: film about a man aging backwards
- type: film_search
- parsed.details used for Chroma similarity

Input: movies like Inception
- type: recommendation
- parsed.title may be Inception
- retrieval: Chroma similarity using parsed.details

Input: actor who played Iron Man
- type: person_search
- parsed.role: actor
- retrieval: Chroma with category=actor filter

## Non-Goals
- No conversational memory across requests.
- No multilingual intent translation in v1.
- No blending/reranking across DuckDB and Chroma in v1.