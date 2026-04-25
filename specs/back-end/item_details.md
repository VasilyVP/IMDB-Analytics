# Item Details API - Development Spec

## Goal
Provide one endpoint that returns a human-readable description for a single IMDB entity, resolved from ChromaDB by either title ID (`tconst`) or person ID (`nconst`).

## Endpoint
- Method: GET
- Path: `/query/item-details`

## Query Parameters
Exactly one identifier must be provided.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `titleId` | string | conditional | IMDB title identifier (`tconst`, e.g., `tt0111161`) |
| `nameId` | string | conditional | IMDB person identifier (`nconst`, e.g., `nm0000209`) |

### Validation Rules
1. Exactly one of `titleId` or `nameId` must be present.
2. Providing both `titleId` and `nameId` is invalid.
3. Providing neither identifier is invalid.
4. `titleId` must match `^tt[0-9]+$`.
5. `nameId` must match `^nm[0-9]+$`.
6. Validation failures return `422`.

Examples:
- `/query/item-details?titleId=tt0111161`
- `/query/item-details?nameId=nm0000209`

## Response Shape
```json
{
	"id": "tt0111161",
	"entityType": "title",
	"description": "A banker convicted of murdering his wife forms an unlikely friendship in prison and spends years engineering an audacious path to freedom."
}
```

### Response Contract
- `id` (string): echoed identifier (`tconst` or `nconst`) used for lookup.
- `entityType` (string enum): `"title"` or `"person"`.
- `description` (string): normalized human-readable description text from ChromaDB.

## Data Rules

### 1. Collection selection
- If `titleId` is provided, query the title-description collection.
- If `nameId` is provided, query the person-description collection.

### 2. Lookup strategy
- Query ChromaDB by exact entity ID in collection metadata.
- Expected metadata key is `id` matching the requested `titleId` or `nameId`.
- If found, return `metadata.human_description`.

### 3. Cache-miss fallback (generate, save, return)
If no description exists in ChromaDB for a valid identifier:

1. Generate description on the fly.
2. Save generated payload to ChromaDB.
3. Return the generated human description in the same response shape.

### 4. Generation and persistence parity with seeding script
Fallback generation and save must follow the same logic as `scripts/chroma_seed/`:

- Route all LLM requests through a dedicated reusable service module (for example, `llm_description_service`) instead of invoking the LLM client directly from endpoint/service/repository code. This service must encapsulate prompt selection and LLM call behavior and be designed for reuse by future endpoints and jobs.

- Use the same prompt builders from `scripts/chroma_seed/prompts.py`:
	- Title: `build_title_description_prompt` and `build_title_embedding_prompt`
	- Person: `build_person_description_prompt` and `build_person_embedding_prompt`
- Use the same LLM client behavior as `scripts/chroma_seed/llm_client.py`:
	- OpenAI-compatible chat completions transport
	- `temperature=0`
	- retry policy (`MAX_RETRIES`) semantics
	- plain-text output handling
- Generate both artifacts, even though API response returns only one:
	- `human_description` (for API response + metadata)
	- `embedding_description` (stored as Chroma `document`)
- Save records to the same Chroma collection schema used by seeding:
	- Titles collection metadata keys: `titleId`, `title`, `startYear`, `human_description`
	- Persons collection metadata keys: `personId`, `name`, `birthYear`, `category`, `human_description`

### 5. Source data for fallback prompt input
- For title fallback, load prompt input fields required by seeding prompts (`title`, `start_year`) from canonical backend data sources.
- For person fallback, load prompt input fields required by seeding prompts (`name`, `birth_year`, `category`) from canonical backend data sources.
- If source entity does not exist for the requested ID, treat as not found.

## Error Behavior
- Invalid query combinations or malformed IDs: `422`.
- Identifier not found in canonical source data (cannot generate fallback): `404` with detail `"Description not found"`.
- ChromaDB unavailable, generation failure, or save failure during fallback: `503` with detail `"Description service is temporarily unavailable"`.

## N-Tier Structure

| Layer | Responsibility |
|---|---|
| `endpoints/` | Parse and validate `titleId`/`nameId`; call service |
| `services/` | Enforce mutual exclusivity; select collection; orchestrate cache read -> fallback generation -> save; map repository output to response model |
| `repositories/` | Execute ChromaDB operations for both exact-ID reads and persistence writes (upsert/add) using the seeding-compatible schema |
| `services/llm` | Dedicated reusable LLM integration layer that selects prompt builders and executes LLM requests with seeding-compatible behavior |
| `schemas/` | Define request query model and response schema |

## Non-Goals
- This endpoint does not return graph relationships, ratings, genres, or search suggestions.
- This endpoint does not support batch lookup.