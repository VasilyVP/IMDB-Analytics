import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";

export function QueryPanel() {
  return (
    <div className="space-y-6">
      {/* Aggregation Builder */}
      <div className="space-y-3">
        <Label className="text-xs text-neutral-400">Aggregation</Label>
        <div className="space-y-2">
          <Select defaultValue="count">
            <SelectTrigger className="bg-neutral-900 border-neutral-800">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="count">COUNT</SelectItem>
              <SelectItem value="avg">AVG</SelectItem>
              <SelectItem value="sum">SUM</SelectItem>
              <SelectItem value="min">MIN</SelectItem>
              <SelectItem value="max">MAX</SelectItem>
            </SelectContent>
          </Select>
          <Select defaultValue="rating">
            <SelectTrigger className="bg-neutral-900 border-neutral-800">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="rating">Rating</SelectItem>
              <SelectItem value="votes">Number of Votes</SelectItem>
              <SelectItem value="runtime">Runtime</SelectItem>
              <SelectItem value="year">Year</SelectItem>
            </SelectContent>
          </Select>
        </div>
      </div>

      {/* Group By */}
      <div className="space-y-3">
        <Label className="text-xs text-neutral-400">Group By</Label>
        <Select defaultValue="genre">
          <SelectTrigger className="bg-neutral-900 border-neutral-800">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="genre">Genre</SelectItem>
            <SelectItem value="year">Year</SelectItem>
            <SelectItem value="director">Director</SelectItem>
            <SelectItem value="actor">Main Actor</SelectItem>
            <SelectItem value="language">Language</SelectItem>
            <SelectItem value="country">Country</SelectItem>
          </SelectContent>
        </Select>
      </div>

      {/* Custom Query */}
      <div className="space-y-3">
        <Label className="text-xs text-neutral-400">Custom Query</Label>
        <Input
          placeholder="Enter SQL-like query..."
          className="bg-neutral-900 border-neutral-800 font-mono text-xs"
        />
      </div>

      {/* Execute Button */}
      <Button className="w-full" size="sm">Execute Query</Button>

      {/* Results Preview */}
      <div className="space-y-2">
        <Label className="text-xs text-neutral-400">Results</Label>
        <div className="rounded-lg border border-neutral-800 bg-neutral-900/30 p-3">
          <div className="text-xs text-neutral-500 text-center py-4">No results yet</div>
        </div>
      </div>
    </div>
  );
}
