import { ResizablePanelGroup, ResizablePanel, ResizableHandle } from "@/components/ui/resizable";
import { Separator } from "@/components/ui/separator";
import { Search } from "lucide-react";
import { GraphVisualization } from "@/components/features/GraphVisualization";
import { FilterPanel } from "@/components/features/FilterPanel";
import { QueryPanel } from "@/components/features/QueryPanel";

export default function Analytics() {
  return (
    <ResizablePanelGroup orientation="horizontal" className="size-full overflow-hidden">
        {/* Left Panel - Graph Visualization */}
        <ResizablePanel defaultSize={65} minSize={40}>
          <GraphVisualization />
        </ResizablePanel>

        <ResizableHandle className="w-px bg-neutral-800" />

        {/* Right Panel - Filters & Queries */}
        <ResizablePanel defaultSize={35} minSize={25}>
          <div className="h-full flex flex-col overflow-auto">
            {/* Filters Section */}
            <div className="p-6">
              <FilterPanel />
            </div>

            <Separator className="bg-neutral-800" />

            {/* Query Section */}
            <div className="p-6">
              <div className="flex items-center gap-2 mb-4">
                <Search className="w-4 h-4 text-neutral-400" />
                <h2 className="text-sm tracking-tight text-neutral-300 m-0 font-normal">Instant Queries</h2>
              </div>
              <QueryPanel />
            </div>
          </div>
        </ResizablePanel>
      </ResizablePanelGroup>
  );
}
