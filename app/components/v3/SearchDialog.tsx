"use client";

import { useEffect, useMemo } from "react";
import Image from "next/image";
import { Search } from "lucide-react";
import { SearchDialogContent } from "@/components/search/SearchDialogContent";
import { SearchFeedback } from "@/components/search/SearchFeedback";
import { SearchField } from "@/components/search/SearchField";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { SubjectKindIcon } from "@/components/subject/SubjectKindIcon";
import { SubjectKind } from "@/lib/subject-kind";
import { toProxiedBangumiImageUrl } from "@/lib/image-proxy";
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

function shouldTopCropCover(kind: SubjectKind) {
  return kind === "character" || kind === "person";
}

function getNoResultHint(kind: SubjectKind, subjectLabel: string): string {
  if (kind === "song" || kind === "album") {
    return "尝试歌手名 + 歌曲/专辑名进行搜索";
  }
  return `尝试${subjectLabel}正式名或别名`;
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
  noResultQuery,
  activeIndex,
  onActiveIndexChange,
  onSubmitSearch,
  onPickGame,
}: SearchDialogProps) {
  const trimmedQuery = query.trim();
  const orderedResults = results;

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
      <SearchDialogContent>
        <DialogHeader>
          <DialogTitle>{dialogTitle}</DialogTitle>
        </DialogHeader>

        <div>
          <SearchField
            value={query}
            role="combobox"
            aria-expanded={open}
            aria-controls="search-results-list"
            aria-label={`${subjectLabel}搜索输入框`}
            placeholder={inputPlaceholder}
            onValueChange={onQueryChange}
            onClear={() => {
              onQueryChange("");
              onActiveIndexChange(-1);
            }}
            onSearch={onSubmitSearch}
            loading={loading}
            searchDisabled={normalizeSearchQuery(query).length === 0}
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
            autoFocus
          />
        </div>

        <div className="min-h-0 overflow-y-auto overscroll-contain" id="search-results-list" role="listbox">
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
                        src={toProxiedBangumiImageUrl(game.cover) ?? game.cover}
                        alt={displayName(game)}
                        fill
                        className={cn("object-cover", shouldTopCropCover(kind) && "object-top")}
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
              noResultQuery={noResultQuery}
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
      </SearchDialogContent>
    </Dialog>
  );
}

function SearchStatus(props: {
  kind: SubjectKind;
  subjectLabel: string;
  idleHint: string;
  state: Exclude<ViewState, "success">;
  error: string;
  noResultQuery: string | null;
  onRetry: () => void;
}) {
  const {
    kind,
    subjectLabel,
    idleHint,
    state,
    error,
    noResultQuery,
    onRetry,
  } = props;

  if (state === "searching" || state === "error") {
    return (
      <SearchFeedback
        loading={state === "searching"}
        error={error || "搜索失败，请检查网络连接后重试"}
        onRetry={onRetry}
      />
    );
  }

  if (state === "no-results") {
    const noResultHint = getNoResultHint(kind, subjectLabel);
    return (
      <div className="flex flex-col items-center justify-center py-10 text-muted-foreground" aria-live="polite">
        <SubjectKindIcon kind={kind} className="mb-2 h-8 w-8 opacity-50" />
        <p>{noResultQuery ? `未找到“${noResultQuery}”` : `未找到相关${subjectLabel}`}</p>
        <p className="mt-2 text-sm">{noResultHint}</p>
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
