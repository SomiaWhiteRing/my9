"use client";

import { useMemo, useRef } from "react";
import Image from "next/image";
import { AlertCircle, ImagePlus, Loader2, RefreshCw, Search } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import {
  CustomPendingSelection,
  CustomSearchItem,
  CustomSearchSource,
} from "@/lib/custom/types";
import { normalizeSearchQuery } from "@/lib/search/query";
import { cn } from "@/lib/utils";

export type CustomSearchTab = CustomSearchSource | "upload";

interface CustomSearchDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  activeTab: CustomSearchTab;
  onTabChange: (tab: CustomSearchTab) => void;
  query: string;
  committedQuery: string;
  onQueryChange: (value: string) => void;
  loading: boolean;
  error: string;
  results: CustomSearchItem[];
  noResultQuery: string | null;
  pendingSelection: CustomPendingSelection | null;
  pendingTitle: string;
  selectedSlotIndex: number | null;
  onPendingTitleChange: (value: string) => void;
  onSubmitSearch: () => void;
  onPickResult: (item: CustomSearchItem) => void;
  onConfirmSelection: () => void;
  onSelectUploadFile: (file: File) => void;
}

type ViewState = "idle" | "searching" | "success" | "error" | "no-results";

const TAB_LABEL: Record<CustomSearchTab, string> = {
  bangumi: "Bangumi",
  tmdb: "TMDB",
  apple: "Apple Music",
  upload: "上传",
};

function displayName(item: CustomSearchItem) {
  return item.localizedName?.trim() || item.name;
}

function SearchStatus(props: {
  state: Exclude<ViewState, "success">;
  error: string;
  loading: boolean;
  noResultQuery: string | null;
  onRetry: () => void;
}) {
  const { state, error, loading, noResultQuery, onRetry } = props;

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
        <p>{error || "搜索失败，请稍后重试"}</p>
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
        <Search className="mb-2 h-8 w-8 opacity-50" />
        <p>{noResultQuery ? `未找到“${noResultQuery}”` : "未找到结果"}</p>
        <p className="mt-2 text-sm">可以切换别的来源试试，或改用上传图片。</p>
      </div>
    );
  }

  return (
    <div className="flex flex-col items-center justify-center py-10 text-muted-foreground" aria-live="polite">
      <Search className="mb-2 h-12 w-12 opacity-30" />
      <p>输入关键词开始搜索</p>
    </div>
  );
}

export function CustomSearchDialog({
  open,
  onOpenChange,
  activeTab,
  onTabChange,
  query,
  committedQuery,
  onQueryChange,
  loading,
  error,
  results,
  noResultQuery,
  pendingSelection,
  pendingTitle,
  selectedSlotIndex,
  onPendingTitleChange,
  onSubmitSearch,
  onPickResult,
  onConfirmSelection,
  onSelectUploadFile,
}: CustomSearchDialogProps) {
  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const trimmedQuery = query.trim();
  const hasSearchedCurrentQuery = useMemo(() => {
    const committed = normalizeSearchQuery(committedQuery);
    const current = normalizeSearchQuery(trimmedQuery);
    return committed.length > 0 && committed === current;
  }, [committedQuery, trimmedQuery]);
  const state: ViewState = useMemo(() => {
    if (activeTab === "upload") return "idle";
    if (loading) return "searching";
    if (error) return "error";
    if (trimmedQuery.length === 0) return "idle";
    if (hasSearchedCurrentQuery && results.length > 0) return "success";
    if (hasSearchedCurrentQuery && results.length === 0) return "no-results";
    return "idle";
  }, [activeTab, error, hasSearchedCurrentQuery, loading, results.length, trimmedQuery]);

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="w-[94vw] max-h-[88vh] overflow-y-auto p-4 sm:max-w-3xl sm:p-5">
        <DialogHeader>
          <DialogTitle>选择作品来源</DialogTitle>
        </DialogHeader>

        <div className="space-y-3">
          <div className="overflow-x-auto">
            <div className="inline-flex min-w-max gap-2">
              {(["bangumi", "tmdb", "apple", "upload"] as const).map((tab) => {
                const active = tab === activeTab;
                return (
                  <Button
                    key={tab}
                    type="button"
                    variant="outline"
                    onClick={() => onTabChange(tab)}
                    className={cn(
                      "h-7 rounded-full px-2.5 text-[11px] font-semibold sm:h-8 sm:px-3 sm:text-xs",
                      active && "border-sky-300 bg-sky-50 text-sky-700 dark:border-sky-800 dark:bg-sky-950/50 dark:text-sky-200"
                    )}
                  >
                    {TAB_LABEL[tab]}
                  </Button>
                );
              })}
            </div>
          </div>

          {activeTab !== "upload" ? (
            <div className="flex gap-2">
              <div className="relative flex-1">
                <Input
                  value={query}
                  placeholder={`搜索 ${TAB_LABEL[activeTab]} 条目`}
                  onChange={(event) => onQueryChange(event.target.value)}
                  onKeyDown={(event) => {
                    if (event.key === "Enter") {
                      event.preventDefault();
                      onSubmitSearch();
                    }
                  }}
                  className="pr-8"
                  autoFocus
                />
                {query ? (
                  <button
                    type="button"
                    aria-label="清空搜索"
                    onClick={() => onQueryChange("")}
                    className="absolute right-2 top-1/2 -translate-y-1/2 text-muted-foreground transition-colors hover:text-foreground"
                  >
                    ✕
                  </button>
                ) : null}
              </div>
              <Button type="button" onClick={onSubmitSearch} disabled={loading || trimmedQuery.length === 0}>
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
          ) : (
            <div className="rounded-2xl border border-dashed border-border bg-muted/40 p-4">
              <input
                ref={fileInputRef}
                type="file"
                accept="image/*"
                className="hidden"
                onChange={(event) => {
                  const file = event.target.files?.[0];
                  if (file) {
                    onSelectUploadFile(file);
                  }
                  event.currentTarget.value = "";
                }}
              />
              <div className="flex flex-col items-center gap-3 text-center">
                <ImagePlus className="h-9 w-9 text-muted-foreground" />
                <p className="text-sm font-semibold text-foreground">上传本地图片</p>
                <Button type="button" onClick={() => fileInputRef.current?.click()}>
                  选择图片
                </Button>
              </div>
            </div>
          )}

          {activeTab !== "upload" ? (
            <div className="max-h-[42vh] overflow-y-auto" id="custom-search-results">
              {state === "success" ? (
                <div className="grid grid-cols-2 gap-2 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4">
                  {results.map((item) => (
                    <button
                      key={`${item.source}:${item.id}`}
                      type="button"
                      onClick={() => onPickResult(item)}
                      className="cursor-pointer rounded border border-border p-1 text-left transition-colors hover:bg-accent sm:p-2"
                      title={displayName(item)}
                    >
                      <div className="relative h-0 w-full overflow-hidden rounded bg-muted pb-[133.33%]">
                        {item.cover ? (
                          <Image
                            src={item.cover}
                            alt={displayName(item)}
                            fill
                            unoptimized
                            className="object-cover"
                            sizes="(max-width: 768px) 42vw, 20vw"
                          />
                        ) : (
                          <div className="flex h-full items-center justify-center text-xs text-muted-foreground">无图</div>
                        )}
                      </div>
                      <p className="mt-1 truncate text-xs text-card-foreground sm:mt-2 sm:text-sm">{displayName(item)}</p>
                    </button>
                  ))}
                </div>
              ) : (
                <SearchStatus
                  state={state}
                  error={error}
                  loading={loading}
                  noResultQuery={noResultQuery}
                  onRetry={onSubmitSearch}
                />
              )}
            </div>
          ) : null}

          {pendingSelection ? (
            <div className="space-y-3 rounded-2xl border border-sky-200 bg-sky-50/70 p-3 dark:border-sky-900 dark:bg-sky-950/30">
              <div className="flex items-start gap-3">
                <div className="relative h-20 w-16 flex-shrink-0 overflow-hidden rounded-lg border border-border bg-muted">
                  {pendingSelection.cover ? (
                    <Image
                      src={pendingSelection.cover}
                      alt={pendingSelection.fallbackName || "待填入作品"}
                      fill
                      unoptimized
                      className="object-contain"
                    />
                  ) : (
                    <div className="flex h-full items-center justify-center text-[11px] text-muted-foreground">无图</div>
                  )}
                </div>
                <div className="min-w-0 flex-1 space-y-2">
                  <div className="space-y-1">
                    <label className="text-xs font-semibold text-foreground" htmlFor="custom-selection-title">
                      条目标题
                    </label>
                    <Input
                      id="custom-selection-title"
                      value={pendingTitle}
                      onChange={(event) => onPendingTitleChange(event.target.value.slice(0, 60))}
                      placeholder="可留空"
                    />
                  </div>
                  {pendingSelection.fallbackName ? (
                    <p className="text-xs text-muted-foreground">原始名称：{pendingSelection.fallbackName}</p>
                  ) : null}
                </div>
              </div>

              <div className="flex justify-end">
                <Button type="button" size="sm" onClick={onConfirmSelection}>
                  填入第 {selectedSlotIndex !== null ? selectedSlotIndex + 1 : "?"} 格
                </Button>
              </div>
            </div>
          ) : null}
        </div>

        <DialogFooter className="mt-1 flex-col border-t pt-2 sm:flex-row sm:justify-between">
          <div className="text-xs text-muted-foreground">
            {activeTab === "upload" ? "" : results.length > 0 ? `共 ${results.length} 条结果` : ""}
          </div>
          <Button variant="outline" size="sm" onClick={() => onOpenChange(false)}>
            关闭
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
