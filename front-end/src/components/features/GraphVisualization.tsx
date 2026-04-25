import { useMemo, useRef, useState } from "react";
import { InteractiveNvlWrapper, type MouseEventCallbacks } from "@neo4j-nvl/react";
import NVL, { type Layout, type Node as NvlNode, type Relationship as NvlRelationship } from "@neo4j-nvl/base";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Button } from "@/components/ui/button";
import { ZoomIn, ZoomOut, Maximize2, AlertTriangle } from "lucide-react";
import type { GraphDataResponse, GraphEdge, GraphNode } from "@/hooks/useGraphData";
import { useItemDetails } from "@/hooks/useItemDetails";

type LayoutMode = "force" | "hierarchical" | "circular" | "radial";
type RelationshipFilter = "all" | "actors" | "directors" | "genres";
type GraphSelectionCallbacks = Pick<MouseEventCallbacks, "onNodeClick" | "onNodeDoubleClick" | "onCanvasClick">;

export type GraphNavigationTarget = {
  id: string;
  label: string;
  type: "Title" | "Person";
};

type GraphVisualizationProps = {
  data: GraphDataResponse | undefined;
  isLoading: boolean;
  isError: boolean;
  error: Error | null;
  onRetry: () => void;
  hasRequested: boolean;
  onNavigateToNode: (target: GraphNavigationTarget) => void;
};

const LAYOUT_TO_NVL: Record<LayoutMode, Layout> = {
  force: "forceDirected",
  hierarchical: "hierarchical",
  circular: "circular",
  radial: "circular",
};

function buildGraphMouseEventCallbacks(selectionCallbacks: GraphSelectionCallbacks): MouseEventCallbacks {
  return {
    // Enable built-in NVL wheel zoom and click-drag panning interactions.
    onZoomAndPan: true,
    onPan: true,
    onDrag: true,
    ...selectionCallbacks,
  };
}

function relationshipMatchesFilter(edge: GraphEdge, filter: RelationshipFilter): boolean {
  if (filter === "all") {
    return true;
  }

  if (filter === "actors") {
    return edge.type === "ACTED_IN";
  }

  if (filter === "directors") {
    return edge.type === "DIRECTED";
  }

  return true;
}

function formatErrorMessage(error: Error | null): string {
  if (!error) {
    return "Unable to load graph data.";
  }

  if (error.message.includes("(503)")) {
    return "Graph service is currently unavailable. Please try again.";
  }

  if (error.message.includes("(422)")) {
    return "Current filters are invalid for graph query. Adjust filters and retry.";
  }

  return "Unable to load graph data. Please try again.";
}

function getNodeTypeLabel(node: GraphNode): string {
  return node.type === "Title" ? "Title" : "Person";
}

function formatItemDetailsErrorMessage(error: Error | null): string {
  if (!error) {
    return "Unable to load description. Please try again.";
  }

  if (error.message.includes("(404)")) {
    return "Description not found.";
  }

  if (error.message.includes("(503)")) {
    return "Description service is temporarily unavailable.";
  }

  if (error.message.includes("(422)")) {
    return "Description is currently unavailable. Please try again.";
  }

  return "Unable to load description. Please try again.";
}

export function GraphVisualization({
  data,
  isLoading,
  isError,
  error,
  onRetry,
  hasRequested,
  onNavigateToNode,
}: GraphVisualizationProps) {
  const [layoutMode, setLayoutMode] = useState<LayoutMode>("force");
  const [relationshipFilter, setRelationshipFilter] = useState<RelationshipFilter>("all");
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const nvlRef = useRef<NVL | null>(null);

  const filteredGraph = useMemo(() => {
    if (!data) {
      return { nodes: [] as GraphNode[], edges: [] as GraphEdge[] };
    }

    let edges = data.edges.filter((edge) => relationshipMatchesFilter(edge, relationshipFilter));
    let visibleNodeIds = new Set(edges.flatMap((edge) => [edge.source, edge.target]));

    if (relationshipFilter === "genres") {
      const titleIds = new Set(
        data.nodes
          .filter((node) => node.type === "Title" && (node.genres?.length ?? 0) > 0)
          .map((node) => node.id),
      );
      edges = data.edges.filter((edge) => titleIds.has(edge.target));
      visibleNodeIds = new Set(edges.flatMap((edge) => [edge.source, edge.target]));
      for (const titleId of titleIds) {
        visibleNodeIds.add(titleId);
      }
    }

    const nodes = data.nodes.filter((node) => visibleNodeIds.has(node.id));
    return { nodes, edges };
  }, [data, relationshipFilter]);

  const selectedNode = useMemo(
    () => filteredGraph.nodes.find((node) => node.id === selectedNodeId) ?? null,
    [filteredGraph.nodes, selectedNodeId],
  );

  const selectedNodeItemDetailsParams = useMemo(
    () => ({
      titleId: selectedNode?.type === "Title" ? selectedNode.id : null,
      nameId: selectedNode?.type === "Person" ? selectedNode.id : null,
    }),
    [selectedNode],
  );

  const itemDetailsQuery = useItemDetails(selectedNodeItemDetailsParams);

  const nvlNodes = useMemo<NvlNode[]>(() => {
    return filteredGraph.nodes.map((node) => ({
      id: node.id,
      caption: node.label,
      color: node.type === "Title" ? "#60A5FA" : "#22C55E",
      size: node.isAnchor ? 38 : 30,
      selected: node.id === selectedNodeId,
    }));
  }, [filteredGraph.nodes, selectedNodeId]);

  const nvlRels = useMemo<NvlRelationship[]>(() => {
    return filteredGraph.edges.map((edge) => ({
      id: edge.id,
      from: edge.source,
      to: edge.target,
      type: edge.type,
      caption: edge.type,
      color: "#52525B",
      selected: false,
    }));
  }, [filteredGraph.edges]);

  const graphNodeIds = useMemo(() => nvlNodes.map((node) => node.id), [nvlNodes]);

  const mouseEventCallbacks = useMemo(
    () =>
      buildGraphMouseEventCallbacks({
        onNodeClick: (node) => {
          setSelectedNodeId(node.id);
        },
        onNodeDoubleClick: (node) => {
          const graphNode = filteredGraph.nodes.find((n) => n.id === node.id);
          if (!graphNode) {
            return;
          }
          onNavigateToNode({ id: graphNode.id, label: graphNode.label, type: graphNode.type });
        },
        onCanvasClick: () => {
          setSelectedNodeId(null);
        },
      }),
    [filteredGraph.nodes, onNavigateToNode],
  );

  const handleZoomIn = () => {
    if (!nvlRef.current) {
      return;
    }
    const currentScale = nvlRef.current.getScale();
    nvlRef.current.setZoom(currentScale * 1.2);
  };

  const handleZoomOut = () => {
    if (!nvlRef.current) {
      return;
    }
    const currentScale = nvlRef.current.getScale();
    nvlRef.current.setZoom(currentScale / 1.2);
  };

  const handleResetView = () => {
    if (!nvlRef.current) {
      return;
    }
    if (graphNodeIds.length > 0) {
      nvlRef.current.fit(graphNodeIds);
      return;
    }
    nvlRef.current.resetZoom();
  };

  const isEmptyData = !isLoading && !isError && hasRequested && (filteredGraph.nodes.length === 0 || filteredGraph.edges.length === 0);
  const errorMessage = formatErrorMessage(error);

  return (
    <div className="h-full flex flex-col bg-neutral-950">
      <div className="h-12 border-b border-neutral-800 flex items-center justify-between px-4 gap-3">
        <div className="flex items-center gap-3 min-w-0">
          <Select value={layoutMode} onValueChange={(value) => setLayoutMode(value as LayoutMode)}>
            <SelectTrigger className="w-40 h-8 bg-neutral-900 border-neutral-800" aria-label="Graph layout">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="force">Force Layout</SelectItem>
              <SelectItem value="hierarchical">Hierarchical</SelectItem>
              <SelectItem value="circular">Circular</SelectItem>
              <SelectItem value="radial">Radial</SelectItem>
            </SelectContent>
          </Select>

          <Select value={relationshipFilter} onValueChange={(value) => setRelationshipFilter(value as RelationshipFilter)}>
            <SelectTrigger className="w-44 h-8 bg-neutral-900 border-neutral-800" aria-label="Relationship filter">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All Relationships</SelectItem>
              <SelectItem value="actors">Actors</SelectItem>
              <SelectItem value="directors">Directors</SelectItem>
              <SelectItem value="genres">Genres</SelectItem>
            </SelectContent>
          </Select>

          {data?.meta.truncated && (
            <span className="inline-flex items-center gap-1 rounded-md border border-amber-600/50 bg-amber-500/10 px-2 py-1 text-[10px] text-amber-300" role="status">
              <AlertTriangle className="h-3 w-3" />
              Truncated ({data.meta.returnedNodes} nodes / {data.meta.returnedEdges} edges)
            </span>
          )}
        </div>

        <div className="flex items-center gap-2">
          <Button variant="ghost" size="icon" className="h-8 w-8" onClick={handleZoomIn} aria-label="Zoom in">
            <ZoomIn className="w-4 h-4" />
          </Button>
          <Button variant="ghost" size="icon" className="h-8 w-8" onClick={handleZoomOut} aria-label="Zoom out">
            <ZoomOut className="w-4 h-4" />
          </Button>
          <Button variant="ghost" size="icon" className="h-8 w-8" onClick={handleResetView} aria-label="Reset graph view">
            <Maximize2 className="w-4 h-4" />
          </Button>
        </div>
      </div>

      <div className="flex-1 relative bg-neutral-900/50">
        {!hasRequested && (
          <div className="absolute inset-0 flex items-center justify-center p-6">
            <div className="text-center space-y-2">
              <p className="text-sm text-neutral-400">Adjust filters and click Show graph</p>
              <p className="text-xs text-neutral-600">The graph loads on demand to keep exploration fast.</p>
            </div>
          </div>
        )}

        {hasRequested && isLoading && (
          <div className="absolute inset-0 flex items-center justify-center">
            <div className="text-sm text-neutral-400">Loading graph...</div>
          </div>
        )}

        {hasRequested && isError && (
          <div className="absolute inset-0 flex items-center justify-center p-6">
            <div className="max-w-md rounded-lg border border-red-900/60 bg-red-950/30 p-4 text-center space-y-3">
              <p className="text-sm text-red-300">{errorMessage}</p>
              <Button size="sm" onClick={onRetry}>Retry</Button>
            </div>
          </div>
        )}

        {hasRequested && isEmptyData && (
          <div className="absolute inset-0 flex items-center justify-center p-6">
            <div className="text-center space-y-2">
              <p className="text-sm text-neutral-400">No graph data for current filters</p>
              <p className="text-xs text-neutral-600">Try broader filters or another search anchor.</p>
            </div>
          </div>
        )}

        {hasRequested && !isLoading && !isError && !isEmptyData && (
          <InteractiveNvlWrapper
            ref={nvlRef}
            className="absolute inset-0"
            layout={LAYOUT_TO_NVL[layoutMode]}
            nodes={nvlNodes}
            rels={nvlRels}
            nvlOptions={{
              disableTelemetry: true,
              renderer: "canvas",
              layout: LAYOUT_TO_NVL[layoutMode],
              allowDynamicMinZoom: true,
            }}
            interactionOptions={{
              selectOnClick: false,
            }}
            mouseEventCallbacks={mouseEventCallbacks}
          />
        )}

      </div>

      <div className="border-t border-neutral-800 px-4 py-2 text-xs text-neutral-500 space-y-2" role="status">
        <div className="flex gap-6 flex-wrap">
          <span>Nodes: <span className="text-neutral-300">{filteredGraph.nodes.length}</span></span>
          <span>Edges: <span className="text-neutral-300">{filteredGraph.edges.length}</span></span>
          <span>Selected: <span className="text-neutral-300">{selectedNode?.label ?? "None"}</span></span>
        </div>
        {selectedNode && (
          <div className="rounded-md border border-neutral-800 bg-neutral-900/60 px-3 py-2 text-[11px] text-neutral-300 grid grid-cols-1 md:grid-cols-2 gap-2">
            <span>ID: {selectedNode.id}</span>
            <span>Type: {getNodeTypeLabel(selectedNode)}</span>
            {typeof selectedNode.isAnchor === "boolean" && <span>Anchor: {selectedNode.isAnchor ? "Yes" : "No"}</span>}
            {selectedNode.titleType && <span>Title Type: {selectedNode.titleType}</span>}
            {selectedNode.genres && selectedNode.genres.length > 0 && <span>Genres: {selectedNode.genres.join(", ")}</span>}
            {typeof selectedNode.startYear === "number" && <span>Start Year: {selectedNode.startYear}</span>}
            {typeof selectedNode.averageRating === "number" && <span>Rating: {selectedNode.averageRating.toFixed(1)}</span>}
            {typeof selectedNode.numVotes === "number" && <span>Votes: {selectedNode.numVotes.toLocaleString()}</span>}
            {selectedNode.primaryProfession && selectedNode.primaryProfession.length > 0 && <span>Professions: {selectedNode.primaryProfession.join(", ")}</span>}
            {typeof selectedNode.birthYear === "number" && <span>Birth Year: {selectedNode.birthYear}</span>}
            {typeof selectedNode.deathYear === "number" && <span>Death Year: {selectedNode.deathYear}</span>}

            <div className="md:col-span-2 mt-1 border-t border-neutral-800 pt-2" aria-live="polite">
              <p className="text-neutral-400">Description</p>
              {itemDetailsQuery.isLoading && <p className="mt-1 text-neutral-300">Loading description...</p>}

              {!itemDetailsQuery.isLoading && itemDetailsQuery.data?.description && (
                <div className="mt-1 space-y-1">
                  <p className="text-neutral-200 leading-relaxed">{itemDetailsQuery.data.description}</p>
                  {itemDetailsQuery.isFetching && <p className="text-[10px] text-neutral-500">Refreshing...</p>}
                </div>
              )}

              {!itemDetailsQuery.isLoading && !itemDetailsQuery.data?.description && itemDetailsQuery.isError && (
                <p className="mt-1 text-amber-300">{formatItemDetailsErrorMessage(itemDetailsQuery.error)}</p>
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
