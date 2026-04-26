import { ResizablePanelGroup, ResizablePanel, ResizableHandle } from "@/components/ui/resizable";
import { GraphVisualization, type GraphNavigationTarget } from "@/components/features/GraphVisualization";
import { FilterPanel, type FilterState } from "@/components/features/FilterPanel";
import { useImmer } from "use-immer";
import { useState } from "react";
import { useGraphData, type GraphFilters } from "@/hooks/useGraphData";

const INITIAL_FILTER_STATE: FilterState = {
  topRated: false,
  mostPopular: false,
  search: "",
  selectedSearchResult: null,
  genre: null,
  titleType: null,
  ratingRange: null,
  yearRange: null,
};

function toGraphFilters(filters: FilterState): GraphFilters {
  return {
    selectedSearchResult: filters.selectedSearchResult,
    titleType: filters.titleType,
    genre: filters.genre,
    ratingRange: filters.ratingRange,
    yearRange: filters.yearRange,
    topRated: filters.topRated,
    mostPopular: filters.mostPopular,
  };
}

function cloneFilterState(filters: FilterState): FilterState {
  return {
    ...filters,
    selectedSearchResult: filters.selectedSearchResult ? { ...filters.selectedSearchResult } : null,
    ratingRange: filters.ratingRange ? [...filters.ratingRange] as [number, number] : null,
    yearRange: filters.yearRange ? [...filters.yearRange] as [number, number] : null,
  };
}

export default function Analytics() {
  const [filters, setFilters] = useImmer<FilterState>(INITIAL_FILTER_STATE);
  const [graphRequestToken, setGraphRequestToken] = useState(0);
  const [submittedGraphFilters, setSubmittedGraphFilters] = useState<GraphFilters>(() => toGraphFilters(INITIAL_FILTER_STATE));

  const graphDataQuery = useGraphData(submittedGraphFilters, graphRequestToken);

  const applyFiltersAndShowGraph = (updater: (draft: FilterState) => void) => {
    let nextFilters: FilterState | null = null;

    setFilters((draft) => {
      updater(draft);
      nextFilters = cloneFilterState(draft);
    });

    if (nextFilters === null) {
      return;
    }

    setSubmittedGraphFilters(toGraphFilters(nextFilters));
    setGraphRequestToken((prev) => prev + 1);
  };

  const handleNavigateToNode = (target: GraphNavigationTarget) => {
    const selectedSearchResult =
      target.type === "Title"
        ? { id: target.id, primaryTitle: target.label }
        : { id: target.id, name: target.label };

    applyFiltersAndShowGraph((draft) => {
      draft.search = target.label;
      draft.selectedSearchResult = selectedSearchResult;
    });
  };

  return (
    <ResizablePanelGroup orientation="horizontal" className="size-full overflow-hidden">
        {/* Left Panel - Graph Visualization */}
        <ResizablePanel defaultSize={65} minSize={40}>
          <GraphVisualization
            data={graphDataQuery.data}
            isLoading={graphDataQuery.isLoading || graphDataQuery.isFetching}
            isError={graphDataQuery.isError}
            error={graphDataQuery.error}
            onRetry={() => {
              void graphDataQuery.refetch();
            }}
            hasRequested={graphRequestToken > 0}
            onNavigateToNode={handleNavigateToNode}
          />
        </ResizablePanel>

        <ResizableHandle className="w-px bg-neutral-800" />

        {/* Right Panel - Filters & Queries */}
        <ResizablePanel defaultSize={35} minSize={25}>
          <div className="h-full flex flex-col overflow-auto">
            {/* Filters Section */}
            <div className="p-6">
              <FilterPanel
                filters={filters}
                setFilters={setFilters}
                applyFiltersAndShowGraph={applyFiltersAndShowGraph}
                isGraphLoading={graphDataQuery.isLoading || graphDataQuery.isFetching}
                hasGraphRequested={graphRequestToken > 0}
              />
            </div>
          </div>
        </ResizablePanel>
      </ResizablePanelGroup>
  );
}
