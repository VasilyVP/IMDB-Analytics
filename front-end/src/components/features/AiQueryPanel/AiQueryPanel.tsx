import { useState } from "react";

import loadingSpinner from "@/assets/loading-svgrepo-com.svg";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  useHumanToLlmQuery,
  type HumanToLlmQueryType,
  type HumanToLlmResultItem,
} from "@/hooks/useHumanToLlmQuery";
import { ApiError } from "@/lib/exceptions";
import { cn } from "@/lib/utils";

import {
  applyAiResultToFilterState,
  formatAiQueryResultYear,
  getAiQueryErrorMessage,
  shouldRenderAiOptions,
} from "./helpers";
import type { FilterState } from "../FilterPanel";

export type AiQueryPanelProps = {
  filters: FilterState;
  applyFiltersAndShowGraph: (updater: (draft: FilterState) => void) => void;
  isGraphLoading: boolean;
};

type OptionState = {
  type: HumanToLlmQueryType;
  items: HumanToLlmResultItem[];
};

export function AiQueryPanel({
  filters,
  applyFiltersAndShowGraph,
  isGraphLoading,
}: AiQueryPanelProps) {
  const [queryText, setQueryText] = useState("");
  const [options, setOptions] = useState<OptionState | null>(null);
  const [inlineMessage, setInlineMessage] = useState<string | null>(null);

  const humanToLlmMutation = useHumanToLlmQuery();

  async function handleSubmit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();

    const trimmedQuery = queryText.trim();
    if (trimmedQuery.length === 0 || humanToLlmMutation.isPending) {
      return;
    }

    setOptions(null);
    setInlineMessage(null);

    try {
      const response = await humanToLlmMutation.mutateAsync({ query: trimmedQuery });

      if (response.results.length === 0) {
        setInlineMessage("No options found.");
        return;
      }

      if (shouldRenderAiOptions(response.type)) {
        setOptions({ type: response.type, items: response.results });
        return;
      }

      const [firstResult] = response.results;
      if (!firstResult) {
        setInlineMessage("No options found.");
        return;
      }

      applyFiltersAndShowGraph((draft) => {
        const nextFilters = applyAiResultToFilterState(draft, firstResult);
        draft.search = nextFilters.search;
        draft.selectedSearchResult = nextFilters.selectedSearchResult;
      });
    } catch (error) {
      const status = error instanceof ApiError ? error.status : undefined;
      setInlineMessage(getAiQueryErrorMessage(status));
    }
  }

  function handleOptionSelect(item: HumanToLlmResultItem) {
    setOptions(null);
    setInlineMessage(null);

    applyFiltersAndShowGraph((draft) => {
      const nextFilters = applyAiResultToFilterState(draft, item);
      draft.search = nextFilters.search;
      draft.selectedSearchResult = nextFilters.selectedSearchResult;
    });
  }

  return (
    <div className="flex flex-col gap-3">
      <form className="flex flex-col gap-3" onSubmit={handleSubmit}>
        <label htmlFor="ai-query-input" className="text-xs text-neutral-400">
          AI Query
        </label>
        <div className="flex gap-2">
          <Input
            id="ai-query-input"
            value={queryText}
            onChange={(event) => setQueryText(event.target.value)}
            placeholder="Ask for a movie, person, or recommendation..."
            aria-describedby="ai-query-help"
            className="border-neutral-800 bg-neutral-900"
          />
          <Button type="submit" size="default" disabled={humanToLlmMutation.isPending}>
            {humanToLlmMutation.isPending && (
              <img
                src={loadingSpinner}
                alt=""
                aria-hidden="true"
                className="h-3 w-3 animate-[spin_2.4s_linear_infinite]"
              />
            )}
            Ask AI
          </Button>
        </div>
      </form>

      {inlineMessage && !humanToLlmMutation.isPending && (
        <div
          role={humanToLlmMutation.isError ? "alert" : "status"}
          aria-live="polite"
          className={cn(
            "rounded-md border px-3 py-2 text-xs",
            humanToLlmMutation.isError
              ? "border-red-500/30 bg-red-500/10 text-red-200"
              : "border-neutral-800 bg-neutral-900/60 text-neutral-300",
          )}
        >
          {inlineMessage}
        </div>
      )}

      {options && (
        <div className="flex flex-col gap-2">
          <div className="text-xs font-medium text-neutral-300">Options</div>
          <div className="flex flex-col gap-2">
            {options.items.map((item) => {
              const year = formatAiQueryResultYear(item);

              return (
                <button
                  key={`${options.type}:${item.id}`}
                  type="button"
                  onClick={() => handleOptionSelect(item)}
                  className="group flex w-full items-center justify-between rounded-lg border border-neutral-800 bg-neutral-900/50 px-3 py-3 text-left transition-colors hover:border-neutral-700 hover:bg-neutral-900 focus-visible:border-neutral-500 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-neutral-500/40"
                >
                  <div className="flex min-w-0 items-center gap-3">
                    <span className="truncate text-sm text-neutral-200">{item.label}</span>
                    {year && <span className="shrink-0 text-xs text-neutral-500">{year}</span>}
                  </div>
                </button>
              );
            })}
          </div>
        </div>
      )}

      {!humanToLlmMutation.isPending && !isGraphLoading && options === null && filters.search.length > 0 && humanToLlmMutation.isSuccess && !inlineMessage && (
        <div className="text-xs text-neutral-500">Applied to current filters.</div>
      )}
    </div>
  );
}