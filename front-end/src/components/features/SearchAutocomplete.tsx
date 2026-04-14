import { useEffect, useRef, useState } from "react";
import { useDebounce } from "@uidotdev/usehooks";
import { Label } from "@/components/ui/label";
import { Input } from "@/components/ui/input";
import { useSearch, type SearchResultItem } from "@/hooks/useSearch";
import type { FilterState } from "@/components/features/FilterPanel";

type SearchAutocompleteProps = {
  value: string;
  onChange: (value: string) => void;
  filters: FilterState;
  selectedItem: SearchResultItem | null;
  onSelect: (item: SearchResultItem | null) => void;
};

export function SearchAutocomplete({
  value,
  onChange,
  filters,
  selectedItem,
  onSelect,
}: SearchAutocompleteProps) {
  const debouncedQuery = useDebounce(value, 500);
  const [isOpen, setIsOpen] = useState(false);
  const [activeIndex, setActiveIndex] = useState(-1);
  const wrapperRef = useRef<HTMLDivElement>(null);
  const listRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    function handleMouseDown(event: MouseEvent) {
      if (wrapperRef.current && !wrapperRef.current.contains(event.target as Node)) {
        setIsOpen(false);
      }
    }
    document.addEventListener("mousedown", handleMouseDown);
    return () => document.removeEventListener("mousedown", handleMouseDown);
  }, []);

  const { data, isFetching, isError } = useSearch({
    q: debouncedQuery,
    topRated: filters.topRated,
    mostPopular: filters.mostPopular,
    minRating: filters.ratingRange?.[0] ?? null,
    maxRating: filters.ratingRange?.[1] ?? null,
    startYearFrom: filters.yearRange?.[0] ?? null,
    startYearTo: filters.yearRange?.[1] ?? null,
    genre: filters.genre,
    titleType: filters.titleType,
  });

  const showDropdown = isOpen && debouncedQuery.trim().length >= 3;
  const results = data?.results ?? [];

  useEffect(() => {
    if (activeIndex < 0 || !listRef.current) return;
    const el = listRef.current.children[activeIndex] as HTMLElement | undefined;
    el?.scrollIntoView({ block: "nearest" });
  }, [activeIndex]);

  function handleKeyDown(event: React.KeyboardEvent<HTMLInputElement>) {
    if (!showDropdown) return;
    if (event.key === "ArrowDown") {
      event.preventDefault();
      setActiveIndex((i) => Math.min(i + 1, results.length - 1));
    } else if (event.key === "ArrowUp") {
      event.preventDefault();
      setActiveIndex((i) => Math.max(i - 1, -1));
    } else if (event.key === "Enter") {
      if (activeIndex >= 0 && results[activeIndex]) {
        event.preventDefault();
        handleResultClick(results[activeIndex]);
      }
    } else if (event.key === "Escape") {
      setIsOpen(false);
      setActiveIndex(-1);
    }
  }

  function handleInputChange(event: React.ChangeEvent<HTMLInputElement>) {
    onChange(event.target.value);
    if (selectedItem !== null) {
      onSelect(null);
    }
    setIsOpen(true);
    setActiveIndex(-1);
  }

  function handleFocus() {
    if (value.trim().length >= 3) {
      setIsOpen(true);
    }
  }

  function handleResultClick(item: SearchResultItem) {
    onSelect(item);
    onChange(item.primaryTitle ?? item.name ?? "");
    setIsOpen(false);
    setActiveIndex(-1);
  }

  return (
    <div className="space-y-3" ref={wrapperRef}>
      <Label htmlFor="filter-search" className="text-xs text-neutral-400">Search</Label>
      <div className="relative">
        <Input
          id="filter-search"
          placeholder="Search by title or person..."
          className="bg-neutral-900 border-neutral-800"
          value={value}
          onChange={handleInputChange}
          onFocus={handleFocus}
          onKeyDown={handleKeyDown}
          autoComplete="off"
        />
        {showDropdown && (
          <div className="absolute top-full left-0 right-0 z-50 mt-1 rounded-md border border-neutral-800 bg-neutral-900 shadow-lg overflow-hidden">
            {isFetching && (
              <div className="px-3 py-2 text-xs text-neutral-500">Searching…</div>
            )}
            {isError && !isFetching && (
              <div className="px-3 py-2 text-xs text-red-400">Search failed</div>
            )}
            {!isFetching && !isError && data?.results.length === 0 && (
              <div className="px-3 py-2 text-xs text-neutral-500">No results</div>
            )}
            <div className="max-h-80 overflow-y-auto" ref={listRef}>
              {!isError && results.map((item, index) => {
                const label = item.primaryTitle ?? item.name ?? item.id;
                const badge = item.primaryTitle !== undefined ? "Title" : "Person";
                return (
                  <button
                    key={item.id}
                    type="button"
                    className={`w-full flex items-center justify-between px-3 py-2 text-xs text-neutral-300 transition-colors text-left ${
                      index === activeIndex ? "bg-neutral-700" : "hover:bg-neutral-800"
                    }`}
                    onMouseDown={(e) => e.preventDefault()}
                    onMouseEnter={() => setActiveIndex(index)}
                    onClick={() => handleResultClick(item)}
                  >
                    <span className="truncate">{label}</span>
                    <span className="ml-2 shrink-0 text-[10px] text-neutral-500">{badge}</span>
                  </button>
                );
              })}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
