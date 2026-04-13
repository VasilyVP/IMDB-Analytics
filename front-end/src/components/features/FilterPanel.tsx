import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Slider } from "@/components/ui/slider";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Filter, TrendingUp, Users } from "lucide-react";

type FilterPanelProps = {
  titleCount?: number | null;
  personCount?: number | null;
};

const quickQueries = [
  { icon: TrendingUp, label: "Top Rated", description: "Highest rated titles" },
  { icon: Users, label: "Most Popular", description: "By number of votes" },
];

export function FilterPanel({ titleCount = null, personCount = null }: FilterPanelProps) {
  const formatCount = (value: number | null | undefined): string => {
    return typeof value === "number" ? value.toLocaleString() : "N/A";
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-2">
        <Filter className="h-4 w-4 text-neutral-400" />
        <h2 className="m-0 text-sm font-normal tracking-tight text-neutral-300">Filters</h2>
      </div>

      {/* Status */}
      <div className="rounded-lg border border-neutral-800 bg-neutral-900/40 px-3 py-2">
        <div className="grid grid-cols-2 gap-4 text-xs">
          <div className="flex items-center justify-between">
            <span className="text-neutral-500">Titles Found</span>
            <span className="font-medium text-neutral-200">{formatCount(titleCount)}</span>
          </div>
          <div className="flex items-center justify-between">
            <span className="text-neutral-500">Persons Found</span>
            <span className="font-medium text-neutral-200">{formatCount(personCount)}</span>
          </div>
        </div>
      </div>

      {/* Search */}
      <div className="space-y-3">
        <Label htmlFor="filter-search" className="text-xs text-neutral-400">Search</Label>
        <Input
          id="filter-search"
          placeholder="Search by title or person..."
          className="bg-neutral-900 border-neutral-800"
        />
      </div>

      {/* Quick Queries */}
      <div className="space-y-3">
        <Label className="text-xs text-neutral-400">Quick Queries</Label>
        <div className="grid grid-cols-2 gap-2">
          {quickQueries.map((query, idx) => (
            <button
              key={idx}
              className="p-3 rounded-lg border border-neutral-800 bg-neutral-900/50 hover:bg-neutral-900 hover:border-neutral-700 transition-colors text-left"
            >
              <query.icon className="w-4 h-4 text-neutral-400 mb-2" />
              <div className="text-xs text-neutral-300">{query.label}</div>
              <div className="text-[10px] text-neutral-600">{query.description}</div>
            </button>
          ))}
        </div>
      </div>

      {/* Rating Range */}
      <div className="space-y-3">
        <Label className="text-xs text-neutral-400">IMDB Rating</Label>
        <div className="space-y-2">
          <Slider defaultValue={[7.0, 9.0]} min={0} max={10} step={0.1} className="w-full" />
          <div className="flex justify-between text-xs text-neutral-500">
            <span>7.0</span>
            <span>9.0</span>
          </div>
        </div>
      </div>

      {/* Year Range */}
      <div className="space-y-3">
        <Label className="text-xs text-neutral-400">Release Year</Label>
        <div className="space-y-2">
          <Slider defaultValue={[1990, 2024]} min={1900} max={2024} step={1} className="w-full" />
          <div className="flex justify-between text-xs text-neutral-500">
            <span>1990</span>
            <span>2024</span>
          </div>
        </div>
      </div>

      {/* Genre + Type */}
      <div className="grid grid-cols-2 gap-3">
        <div className="space-y-3">
          <Label className="text-xs text-neutral-400">Genre</Label>
          <Select>
            <SelectTrigger className="w-full bg-neutral-900 border-neutral-800">
              <SelectValue placeholder="All genres" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All genres</SelectItem>
              <SelectItem value="action">Action</SelectItem>
              <SelectItem value="comedy">Comedy</SelectItem>
              <SelectItem value="drama">Drama</SelectItem>
              <SelectItem value="horror">Horror</SelectItem>
              <SelectItem value="scifi">Sci-Fi</SelectItem>
              <SelectItem value="thriller">Thriller</SelectItem>
            </SelectContent>
          </Select>
        </div>

        <div className="space-y-3">
          <Label className="text-xs text-neutral-400">Type</Label>
          <Select>
            <SelectTrigger className="w-full bg-neutral-900 border-neutral-800">
              <SelectValue placeholder="All types" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All types</SelectItem>
              <SelectItem value="movie">Movie</SelectItem>
              <SelectItem value="tvSeries">TV Series</SelectItem>
            </SelectContent>
          </Select>
        </div>
      </div>

      <Button size="lg" className="w-full">
        Show graph
      </Button>

    </div>
  );
}
