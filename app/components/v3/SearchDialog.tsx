"use client";

import { useEffect, useMemo } from "react";
import Image from "next/image";
import { AlertCircle, Loader2, RefreshCw, Search } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { SubjectKindIcon } from "@/components/subject/SubjectKindIcon";
import { SubjectKind } from "@/lib/subject-kind";
import { normalizeSearchQuery } from "@/lib/search/query";
import { ShareGame } from "@/lib/share/types";
import { cn } from "@/lib/utils";

interface SearchDialogProps {
  kind: SubjectKind;
  subjectLabel: string;
  dialogTitle: string;
  inputPlaceholder: string;
  idleHint: string;
  committedQuery: string;
  open: boolean;
  onOpenChange: (open: boolean) => void;
  query: string;
  onQueryChange: (value: string) => void;
  loading: boolean;
  error: string;
  results: ShareGame[];
  topPickIds: Array<string | number>;
  suggestions: string[];
  noResultQuery: string | null;
  activeIndex: number;
  onActiveIndexChange: (index: number) => void;
  onSubmitSearch: () => void;
  onPickGame: (game: ShareGame) => void;
}

type ViewState = "idle" | "searching" | "success" | "error" | "no-results";

function displayName(game: ShareGame) {
  return game.localizedName?.trim() || game.name;
}

function toIdKey(value: string | number) {
  return String(value);
}

export function SearchDialog({
  kind,
  subjectLabel,
  dialogTitle,
  inputPlaceholder,
  idleHint,
  committedQuery,
  open,
  onOpenChange,
  query,
  onQueryChange,
  loading,
  error,
  results,
  topPickIds,
  suggestions,
  noResultQuery,
  activeIndex,
  onActiveIndexChange,
  onSubmitSearch,
  onPickGame,
}: SearchDialogProps) {
  const trimmedQuery = query.trim();

  const orderedResults = useMemo(() => {
    if (results.length === 0 || topPickIds.length === 0) {
      return results;
    }

    const topSet = new Set(topPickIds.map(toIdKey));
    const top: ShareGame[] = [];
    const rest: ShareGame[] = [];

    for (const item of results) {
      if (topSet.has(String(item.id))) {
        top.push(item);
      } else {
        rest.push(item);
      }
    }

    return [...top, ...rest];
  }, [results, topPickIds]);

  const hasSearchedCurrentQuery = useMemo(() => {
    const committed = normalizeSearchQuery(committedQuery);
    const current = normalizeSearchQuery(trimmedQuery);
    return committed.length > 0 && committed === current;
  }, [committedQuery, trimmedQuery]);

  const state: ViewState = useMemo(() => {
    if (loading) return "searching";
    if (error) return "error";
    if (trimmedQuery.length === 0) return "idle";
    if (hasSearchedCurrentQuery && orderedResults.length > 0) return "success";
    if (hasSearchedCurrentQuery && orderedResults.length === 0) return "no-results";
    return "idle";
  }, [error, hasSearchedCurrentQuery, loading, orderedResults.length, trimmedQuery]);

  useEffect(() => {
    if (!open) return;

    if (orderedResults.length === 0) {
      if (activeIndex !== -1) {
        onActiveIndexChange(-1);
      }
      return;
    }

    if (activeIndex < 0 || activeIndex >= orderedResults.length) {
      onActiveIndexChange(0);
    }
  }, [activeIndex, onActiveIndexChange, open, orderedResults.length]);

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="w-[95vw] max-h-[90vh] overflow-y-auto sm:max-w-md md:max-w-lg lg:max-w-xl">
        <DialogHeader>
          <DialogTitle>{dialogTitle}</DialogTitle>
        </DialogHeader>

        <div className="mb-4">
          <div className="flex gap-2">
            <div className="relative flex-1">
              <Input
                value={query}
                role="combobox"
                aria-expanded={open}
                aria-controls="search-results-list"
                aria-label={`${subjectLabel}搜索输入框`}
                placeholder={inputPlaceholder}
                onChange={(event) => onQueryChange(event.target.value)}
                onKeyDown={(event) => {
                  if (event.key === "ArrowDown") {
                    event.preventDefault();
                    if (orderedResults.length === 0) return;
                    const nextIndex = Math.min((activeIndex < 0 ? -1 : activeIndex) + 1, orderedResults.length - 1);
                    onActiveIndexChange(nextIndex);
                    return;
                  }

                  if (event.key === "ArrowUp") {
                    event.preventDefault();
                    if (orderedResults.length === 0) return;
                    const nextIndex = Math.max((activeIndex < 0 ? 0 : activeIndex) - 1, 0);
                    onActiveIndexChange(nextIndex);
                    return;
                  }

                  if (event.key === "Enter") {
                    event.preventDefault();
                    if (loading) {
                      return;
                    }
                    const normalizedCommitted = normalizeSearchQuery(committedQuery);
                    const resultsMatchCurrentQuery =
                      normalizedCommitted.length > 0 &&
                      normalizedCommitted === normalizeSearchQuery(trimmedQuery);
                    if (
                      resultsMatchCurrentQuery &&
                      activeIndex >= 0 &&
                      orderedResults[activeIndex]
                    ) {
                      onPickGame(orderedResults[activeIndex]);
                      return;
                    }
                    if (normalizeSearchQuery(trimmedQuery).length > 0) {
                      onSubmitSearch();
                    }
                    return;
                  }

                  if (event.key === "Escape") {
                    onOpenChange(false);
                  }
                }}
                disabled={loading}
                className="pr-8"
                autoFocus
              />
              {query ? (
                <button
                  type="button"
                  aria-label="清空搜索"
                  onClick={() => {
                    onQueryChange("");
                    onActiveIndexChange(-1);
                  }}
                  className="absolute right-2 top-1/2 -translate-y-1/2 text-muted-foreground transition-colors hover:text-foreground"
                >
                  ✕
                </button>
              ) : null}
            </div>
            <Button
              type="button"
              onClick={onSubmitSearch}
              disabled={loading || normalizeSearchQuery(query).length === 0}
            >
              {loading ? (
                <>
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  搜索中
                </>
              ) : (
                <>
                  <Search className="mr-2 h-4 w-4" />
                  搜索
                </>
              )}
            </Button>
          </div>
        </div>

        <div className="max-h-[40vh] overflow-y-auto sm:max-h-[300px] md:max-h-[350px] lg:max-h-[400px]" id="search-results-list" role="listbox">
          {state === "success" ? (
            <div className="grid grid-cols-2 gap-2 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4">
              {orderedResults.map((game, index) => (
                <button
                  key={`${String(game.id)}-${index}`}
                  type="button"
                  onMouseEnter={() => onActiveIndexChange(index)}
                  onClick={() => onPickGame(game)}
                  className={cn(
                    "cursor-pointer rounded border p-1 transition-colors sm:p-2",
                    index === activeIndex
                      ? "border-sky-300 bg-sky-50 dark:border-sky-800 dark:bg-sky-950/40"
                      : "border-border hover:bg-accent"
                  )}
                  title={displayName(game)}
                >
                  <div className="relative h-0 w-full overflow-hidden rounded bg-muted pb-[133.33%]">
                    {game.cover ? (
                      <Image
                        src={game.cover}
                        alt={displayName(game)}
                        fill
                        className="object-cover"
                        sizes="(max-width: 768px) 40vw, 20vw"
                        loading="lazy"
                      />
                    ) : (
                      <div className="absolute inset-0 flex items-center justify-center">
                        <SubjectKindIcon kind={kind} className="h-7 w-7 text-muted-foreground" />
                      </div>
                    )}
                  </div>
                  <p className="mt-1 truncate text-xs sm:mt-2 sm:text-sm">{displayName(game)}</p>
                </button>
              ))}
            </div>
          ) : (
            <SearchStatus
              kind={kind}
              subjectLabel={subjectLabel}
              idleHint={idleHint}
              state={state}
              error={error}
              loading={loading}
              noResultQuery={noResultQuery}
              suggestions={suggestions}
              onRetry={onSubmitSearch}
            />
          )}
        </div>

        <DialogFooter className="mt-2 flex flex-col justify-between border-t pt-2 sm:flex-row sm:justify-between">
          <div className="mb-2 text-xs text-muted-foreground sm:mb-0">
            {orderedResults.length > 0 ? `共 ${orderedResults.length} 条结果` : ""}
          </div>
          <Button
            variant="outline"
            size="sm"
            onClick={() => onOpenChange(false)}
            className="hidden sm:inline-flex"
          >
            关闭
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function SearchStatus(props: {
  kind: SubjectKind;
  subjectLabel: string;
  idleHint: string;
  state: Exclude<ViewState, "success">;
  error: string;
  loading: boolean;
  noResultQuery: string | null;
  suggestions: string[];
  onRetry: () => void;
}) {
  const {
    kind,
    subjectLabel,
    idleHint,
    state,
    error,
    loading,
    noResultQuery,
    suggestions,
    onRetry,
  } = props;

  if (state === "searching") {
    return (
      <div className="flex flex-col items-center justify-center py-10 text-muted-foreground" aria-live="polite">
        <Loader2 className="mb-2 h-8 w-8 animate-spin" />
        <p>正在搜索...</p>
      </div>
    );
  }

  if (state === "error") {
    return (
      <div className="flex flex-col items-center justify-center py-10 text-red-500" aria-live="polite">
        <AlertCircle className="mb-2 h-8 w-8" />
        <p>{error || "搜索失败，请检查网络连接后重试"}</p>
        <Button variant="outline" className="mt-4" onClick={onRetry} disabled={loading}>
          <RefreshCw className="mr-2 h-4 w-4" />
          重试
        </Button>
      </div>
    );
  }

  if (state === "no-results") {
    return (
      <div className="flex flex-col items-center justify-center py-10 text-muted-foreground" aria-live="polite">
        <SubjectKindIcon kind={kind} className="mb-2 h-8 w-8 opacity-50" />
        <p>{noResultQuery ? `未找到“${noResultQuery}”` : `未找到相关${subjectLabel}`}</p>
        <p className="mt-2 text-sm">{(suggestions[0] || "尝试更换关键词后重试").replace(/^[\-•]\s*/, "")}</p>
      </div>
    );
  }

  return (
    <div className="flex flex-col items-center justify-center py-10 text-muted-foreground" aria-live="polite">
      <Search className="mb-2 h-12 w-12 opacity-30" />
      <p>{idleHint}</p>
    </div>
  );
}
