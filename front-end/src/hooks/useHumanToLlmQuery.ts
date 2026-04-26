import { useMutation } from "@tanstack/react-query";

import { postJson } from "@/lib/utils";

export type HumanToLlmQueryType =
  | "person_search"
  | "film_search"
  | "person"
  | "film"
  | "recommendation";

export type HumanToLlmParsed = {
  role: "actor" | "director" | null;
  name: string | null;
  title: string | null;
  details: string | null;
};

export type HumanToLlmResultItem = {
  id: string;
  label: string;
  entityType: "person" | "title";
  birthYear: number | null;
  startYear: number | null;
  score: number | null;
};

export type HumanToLlmResponse = {
  type: HumanToLlmQueryType;
  parsed: HumanToLlmParsed;
  results: HumanToLlmResultItem[];
};

export type HumanToLlmRequest = {
  query: string;
  limit?: number;
};

const DEFAULT_AI_QUERY_LIMIT = 10;

export function useHumanToLlmQuery() {
  return useMutation({
    mutationFn: async ({ query, limit = DEFAULT_AI_QUERY_LIMIT }: HumanToLlmRequest) =>
      postJson<HumanToLlmResponse, HumanToLlmRequest>("/query/human-to-llm", {
        query,
        limit,
      }),
    retry: false,
  });
}