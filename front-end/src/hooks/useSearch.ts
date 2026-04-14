import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { getFetcher } from "@/lib/utils";

export type SearchResultItem = {
  id: string;
  name?: string;
  primaryTitle?: string;
};

type SearchResponse = {
  results: SearchResultItem[];
};

export type SearchParams = {
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

const SEARCH_QUERY_KEY = "search";

export function useSearch(params: SearchParams) {
  const enabled = params.q.trim().length >= 3;

  return useQuery({
    queryKey: [SEARCH_QUERY_KEY, params],
    queryFn: getFetcher<SearchResponse>("/query/search", {
      params: {
        q: params.q,
        topRated: params.topRated,
        mostPopular: params.mostPopular,
        minRating: params.minRating ?? null,
        maxRating: params.maxRating ?? null,
        startYearFrom: params.startYearFrom ?? null,
        startYearTo: params.startYearTo ?? null,
        genre: params.genre ?? null,
        titleType: params.titleType ?? null,
        limit: 20,
      },
    }),
    placeholderData: keepPreviousData,
    enabled,
  });
}
