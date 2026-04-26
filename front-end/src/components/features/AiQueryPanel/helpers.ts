import type { FilterState } from "@/components/features/FilterPanel";
import type {
  HumanToLlmQueryType,
  HumanToLlmResultItem,
} from "@/hooks/useHumanToLlmQuery";
import type { SearchResultItem } from "@/hooks/useSearch";

export function mapHumanToLlmResultToSearchResult(result: HumanToLlmResultItem): SearchResultItem {
  return result.entityType === "title"
    ? { id: result.id, primaryTitle: result.label }
    : { id: result.id, name: result.label };
}

export function applyAiResultToFilterState(filters: FilterState, result: HumanToLlmResultItem): FilterState {
  return {
    ...filters,
    search: result.label,
    selectedSearchResult: mapHumanToLlmResultToSearchResult(result),
  };
}

export function shouldRenderAiOptions(type: HumanToLlmQueryType): boolean {
  return type === "recommendation" || type === "person_search" || type === "film_search";
}

export function formatAiQueryResultYear(result: HumanToLlmResultItem): string | null {
  const year = result.entityType === "title" ? result.startYear : result.birthYear;
  return year === null ? null : String(year);
}

export function getAiQueryErrorMessage(status?: number): string {
  switch (status) {
    case 422:
      return "Enter a more specific request.";
    case 429:
      return "Please wait a moment before asking again.";
    case 502:
      return "The AI response could not be understood. Try again.";
    case 503:
      return "AI search is temporarily unavailable.";
    default:
      return "AI query failed. Try again.";
  }
}