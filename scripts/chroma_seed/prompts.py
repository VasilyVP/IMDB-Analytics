from __future__ import annotations

from .models import PersonRecord, TitleRecord

# ---------------------------------------------------------------------------
# Title — human description
# Spec: specs/data_seeding/chromadb_seed/title_description_prompt.md
# ---------------------------------------------------------------------------

_TITLE_DESCRIPTION_SYSTEM = """\
You are generating a short film description for a catalog.

Write a clear, neutral summary of the film.

Requirements:
- 3-4 sentences
- 70-100 words
- No opinions, ratings, or promotional language
- No actor names
- Focus on premise, main characters, and central conflict
- Mention setting only if relevant
- Use natural, readable language

Output plain text only.\
"""

# ---------------------------------------------------------------------------
# Title — embedding description
# Spec: specs/data_seeding/chromadb_seed/title_embedding_prompt.md
# ---------------------------------------------------------------------------

_TITLE_EMBEDDING_SYSTEM = """\
You are generating a structured semantic description of a film for embedding and similarity search.

Rules:
- Be concise, factual, and information-dense.
- Do NOT include opinions, ratings, or subjective language.
- Do NOT use marketing phrases.
- Use consistent vocabulary.
- Output in plain text (no JSON, no markdown).
- Max 120 words.
- Use simple vocabulary. Avoid rare words and synonyms.
- Prefer canonical genre and theme labels.

Fill in all fields in this structure:
Genres:
Setting:
Themes:
Plot:
Characters:
Style:
Director:
Writer:
Starring:\
"""

_TITLE_USER_TEMPLATE = """\
Film:
 - Title: {title}
 - Year: {start_year}\
"""

# ---------------------------------------------------------------------------
# Person — human description
# Spec: specs/data_seeding/chromadb_seed/person_description_prompt.md
# ---------------------------------------------------------------------------

_PERSON_DESCRIPTION_SYSTEM = """\
You are generating short, factual descriptions for a film database.

Task:
Write a concise, neutral description of a person working in film.

Rules:
- Up to 5 sentences only.
- Max 100 words total.
- Do NOT invent specific film titles unless highly confident.
- Focus on general career traits, style, and recognition.
- If unsure about details, stay generic but realistic.
- Do NOT include speculation or uncertainty markers.
- Do NOT mention that you are unsure.

Style:
- Encyclopedic tone (similar to a short IMDb bio).
- Avoid subjective adjectives like "legendary", "iconic", or "world-famous".
- No lists or bullet points.

Output format:
<Name> is a <nationality if known> <role> born in <birth year>. <Brief career description.> <Optional sentence about style, genres, or industry presence.>\
"""

_PERSON_USER_TEMPLATE = """\
Input:
 - Name: {name}
 - Role: {role}
 - Birth year: {birthYear}\
"""

# ---------------------------------------------------------------------------
# Person — embedding description
# Spec: specs/data_seeding/chromadb_seed/person_embedding_prompt.md
# ---------------------------------------------------------------------------

_PERSON_EMBEDDING_SYSTEM = """\
You are generating a structured semantic description for embedding and search.

Task:
Produce a compact, information-dense description of a film industry person to maximize semantic similarity matching.

Rules:
- Max 80 words.
- Use plain, declarative phrases (not narrative style).
- Prioritize semantic keywords over readability.
- Include: role, nationality (if known), career domain, typical genres, style traits, industry context.
- Do NOT invent specific film titles or roles.
- Avoid filler words and subjective language.
- Avoid repetition.

Style:
- Dense, keyword-rich, neutral.
- Prefer general categories (e.g., "drama", "thriller", "independent cinema", "commercial film").

Format:
Name: <name>; Role: <role>; Born: <birth year>; Description: <comma-separated or short phrases describing career, style, genres, industry context>\
"""

# ---------------------------------------------------------------------------
# Public builder functions — return (system_prompt, user_prompt)
# ---------------------------------------------------------------------------


def _build_prompt(
    system_template: str,
    user_template: str,
    **values: str | int,
) -> tuple[str, str]:
    return system_template, user_template.format(**values)


def build_title_description_prompt(title: TitleRecord) -> tuple[str, str]:
    return _build_prompt(
        _TITLE_DESCRIPTION_SYSTEM,
        _TITLE_USER_TEMPLATE,
        title=title.title,
        start_year=title.start_year,
    )


def build_title_embedding_prompt(title: TitleRecord) -> tuple[str, str]:
    return _build_prompt(
        _TITLE_EMBEDDING_SYSTEM,
        _TITLE_USER_TEMPLATE,
        title=title.title,
        start_year=title.start_year,
    )


def build_person_description_prompt(person: PersonRecord) -> tuple[str, str]:
    birth_year = "unknown" if person.birth_year is None else str(person.birth_year)
    return _build_prompt(
        _PERSON_DESCRIPTION_SYSTEM,
        _PERSON_USER_TEMPLATE,
        name=person.name,
        role=person.category,
        birthYear=birth_year,
    )


def build_person_embedding_prompt(person: PersonRecord) -> tuple[str, str]:
    birth_year = "unknown" if person.birth_year is None else str(person.birth_year)
    return _build_prompt(
        _PERSON_EMBEDDING_SYSTEM,
        _PERSON_USER_TEMPLATE,
        name=person.name,
        role=person.category,
        birthYear=birth_year,
    )
