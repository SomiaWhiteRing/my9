"use client";

import { forwardRef, type ComponentPropsWithoutRef } from "react";
import { Loader2, Search, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { cn } from "@/lib/utils";

type SearchFieldProps = Omit<ComponentPropsWithoutRef<typeof Input>, "value" | "onChange"> & {
  value: string;
  onValueChange: (value: string) => void;
  onClear: () => void;
  onSearch: () => void;
  loading: boolean;
  searchDisabled?: boolean;
};

export const SearchField = forwardRef<HTMLInputElement, SearchFieldProps>(function SearchField(
  { value, onValueChange, onClear, onSearch, loading, searchDisabled, className, onKeyDown, ...inputProps },
  ref,
) {
  const submitDisabled = loading || searchDisabled;

  return (
    <div className="flex gap-2">
      <div className="relative min-w-0 flex-1">
        <Input
          {...inputProps}
          ref={ref}
          value={value}
          onChange={(event) => onValueChange(event.target.value)}
          onKeyDown={(event) => {
            if (event.nativeEvent.isComposing || event.keyCode === 229) return;
            onKeyDown?.(event);
            if (event.key === "Enter" && !event.defaultPrevented) {
              event.preventDefault();
              if (!submitDisabled) onSearch();
            }
          }}
          className={cn("pr-8", className)}
        />
        {value ? (
          <button
            type="button"
            aria-label="清空搜索"
            title="清空搜索"
            onClick={onClear}
            className="absolute right-2 top-1/2 -translate-y-1/2 text-muted-foreground transition-colors hover:text-foreground"
          >
            <X className="h-4 w-4" aria-hidden="true" />
          </button>
        ) : null}
      </div>
      <Button type="button" onClick={onSearch} disabled={submitDisabled} className="w-24 shrink-0 px-3">
        {loading ? <Loader2 className="h-4 w-4 animate-spin" aria-hidden="true" /> : <Search className="h-4 w-4" aria-hidden="true" />}
        {loading ? "搜索中" : "搜索"}
      </Button>
    </div>
  );
});
