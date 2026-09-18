"use client";

import Link from "next/link";
import { useCallback, useEffect, useRef, useState } from "react";
import { ArrowRight, CalendarDays, Eye, Loader2, Search, SearchX } from "lucide-react";
import { SearchDialogContent } from "@/components/search/SearchDialogContent";
import { SearchFeedback } from "@/components/search/SearchFeedback";
import { SearchField } from "@/components/search/SearchField";
import { Button } from "@/components/ui/button";
import { Dialog, DialogFooter, DialogHeader, DialogTitle, DialogTrigger } from "@/components/ui/dialog";
import { getSubjectKindMeta } from "@/lib/subject-kind";
import { SHARE_KIND_COLORS, type ShareDiscoveryItem, type ShareDiscoveryLoader, type ShareDiscoveryPage } from "@/lib/share/discovery";

type Props = {
  loadPage: ShareDiscoveryLoader;
  getShareHref: (item: ShareDiscoveryItem, query: string) => string;
  initiallyOpen?: boolean;
  initialQuery?: string;
};

const dateFormatter = new Intl.DateTimeFormat("zh-CN", {
  timeZone: "Asia/Shanghai", year: "numeric", month: "2-digit", day: "2-digit",
});
const countFormatter = new Intl.NumberFormat("zh-CN");

export function ShareDiscoveryDialog({ loadPage, getShareHref, initiallyOpen = false, initialQuery = "" }: Props) {
  const [open, setOpen] = useState(initiallyOpen);
  const [query, setQuery] = useState(initialQuery);
  const [committedQuery, setCommittedQuery] = useState(initialQuery);
  const [page, setPage] = useState<ShareDiscoveryPage | null>(null);
  const [loading, setLoading] = useState(false);
  const [loadingMore, setLoadingMore] = useState(false);
  const [error, setError] = useState("");
  const [retryCursor, setRetryCursor] = useState<string | null>(null);
  const cache = useRef(new Map<string, ShareDiscoveryPage>());
  const request = useRef<AbortController | null>(null);
  const input = useRef<HTMLInputElement>(null);
  const list = useRef<HTMLDivElement>(null);

  const search = useCallback(async (rawQuery: string, cursor: string | null = null) => {
    const nextQuery = rawQuery.trim();
    request.current?.abort();
    const controller = new AbortController();
    request.current = controller;
    setCommittedQuery(nextQuery);
    setError("");
    setRetryCursor(cursor);

    if (!cursor) {
      list.current?.scrollTo({ top: 0 });
      const cached = cache.current.get(nextQuery);
      if (cached) {
        setPage(cached);
        setLoading(false);
        setLoadingMore(false);
        return;
      }
      setPage(null);
    }
    setLoading(!cursor);
    setLoadingMore(Boolean(cursor));

    try {
      const result = await loadPage({ query: nextQuery, cursor, signal: controller.signal });
      if (controller.signal.aborted) return;
      const previous = cursor ? cache.current.get(nextQuery) : undefined;
      const nextPage = { ...result, items: [...(previous?.items ?? []), ...result.items] };
      cache.current.set(nextQuery, nextPage);
      setPage(nextPage);
    } catch (cause) {
      if (controller.signal.aborted) return;
      setError(cause instanceof Error ? cause.message : "暂时无法读取构成，请稍后重试。");
    } finally {
      if (!controller.signal.aborted) {
        setLoading(false);
        setLoadingMore(false);
      }
    }
  }, [loadPage]);

  useEffect(() => {
    if (open && !page && !loading && !error) void search(committedQuery);
  }, [committedQuery, error, loading, open, page, search]);

  useEffect(() => () => request.current?.abort(), []);

  function changeOpen(nextOpen: boolean) {
    setOpen(nextOpen);
    if (!nextOpen) {
      request.current?.abort();
      setLoading(false);
      setLoadingMore(false);
    }
  }

  function clearQuery() {
    setQuery("");
    void search("");
    input.current?.focus();
  }

  const results = page?.items ?? [];

  return (
    <Dialog open={open} onOpenChange={changeOpen}>
      <DialogTrigger asChild>
        <button
          type="button"
          className="inline-flex items-center justify-center gap-1.5 rounded-full border border-orange-200 bg-orange-50 px-4 py-1.5 text-base font-semibold text-orange-700 transition-colors hover:bg-orange-100 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-orange-500 focus-visible:ring-offset-2 dark:border-orange-800 dark:bg-orange-950/50 dark:text-orange-200 dark:hover:bg-orange-900/60"
        >
          <Search className="h-4 w-4" aria-hidden="true" />
          查找构成
        </button>
      </DialogTrigger>
      <SearchDialogContent
        aria-describedby={undefined}
        onOpenAutoFocus={(event) => { event.preventDefault(); input.current?.focus(); }}
      >
        <DialogHeader>
          <DialogTitle>查找构成</DialogTitle>
        </DialogHeader>

        <div role="search">
          <SearchField
            ref={input}
            value={query}
            type="text"
            autoComplete="off"
            maxLength={40}
            aria-label="创作者名称"
            placeholder="输入你用过的名称，查找曾创作的构成……"
            onValueChange={(value) => {
              setQuery(value);
              if (!value) void search("");
            }}
            onClear={clearQuery}
            onSearch={() => void search(query)}
            loading={loading}
            searchDisabled={loadingMore || query.trim().length === 0}
          />
        </div>

        <div ref={list} className="min-h-0 overflow-y-auto overscroll-contain" aria-busy={loading || loadingMore}>
          {loading ? (
            <SearchFeedback loading error="" onRetry={() => void search(committedQuery, retryCursor)} />
          ) : (
            <>
              {results.length ? (
                <>
                  <div className="flex items-center justify-between gap-3 pb-2">
                    <h2 className="min-w-0 break-all text-sm font-semibold">{committedQuery ? `“${committedQuery}”的构成` : "热门构成"}</h2>
                    <span className="shrink-0 text-xs text-muted-foreground">{committedQuery ? "最新创建" : "浏览量前 20"}</span>
                  </div>
                  <ul className="divide-y divide-border">
                    {results.map((item) => {
                      const name = item.creatorName || "未署名";
                      return (
                        <li key={item.shareId} className="grid grid-cols-[48px_minmax(0,1fr)_auto] items-center gap-2.5 py-2.5 sm:grid-cols-[52px_minmax(0,1fr)_auto] sm:gap-3">
                          <div className="flex aspect-square w-full items-center justify-center rounded-md px-1 text-xs font-semibold text-white" style={{ backgroundColor: SHARE_KIND_COLORS[item.kind] }}>
                            {getSubjectKindMeta(item.kind).label}
                          </div>
                          <div className="min-w-0 space-y-0.5">
                            <p className="break-all text-sm font-semibold leading-5">{name}</p>
                            <p className="flex flex-wrap items-center gap-x-3 gap-y-0.5 text-xs text-muted-foreground">
                              <span className="inline-flex items-center gap-1" title="创建日期">
                                <CalendarDays className="h-3.5 w-3.5 shrink-0" aria-hidden="true" />
                                <time dateTime={new Date(item.createdAt).toISOString()}>{dateFormatter.format(item.createdAt)}</time>
                              </span>
                              <span className="inline-flex items-center gap-1 tabular-nums" title="累计浏览量">
                                <Eye className="h-3.5 w-3.5 shrink-0" aria-hidden="true" />
                                {countFormatter.format(item.viewCount)} 次浏览
                              </span>
                            </p>
                          </div>
                          <Button asChild variant="outline" size="sm" className="h-8 shrink-0 gap-1 px-2 text-xs sm:px-3">
                            <Link href={getShareHref(item, committedQuery)} prefetch={false} aria-label={`查看${name}的${getSubjectKindMeta(item.kind).label}构成详情`}>
                              查看详情 <ArrowRight className="!h-3.5 !w-3.5" aria-hidden="true" />
                            </Link>
                          </Button>
                        </li>
                      );
                    })}
                  </ul>
                </>
              ) : !error ? (
                <div className="flex flex-col items-center justify-center py-10 text-center text-muted-foreground" role="status">
                  <SearchX className="mb-2 h-8 w-8 opacity-50" aria-hidden="true" />
                  <p className="max-w-full break-all">{committedQuery ? `没有找到“${committedQuery}”的构成` : "暂时还没有热门构成"}</p>
                  {committedQuery ? <Button variant="outline" className="mt-4" onClick={clearQuery}>返回热门构成</Button> : null}
                </div>
              ) : null}
              {error ? (
                <SearchFeedback loading={false} error={error} onRetry={() => void search(committedQuery, retryCursor)} />
              ) : page?.nextCursor ? (
                <div className="flex justify-center py-4">
                  <Button variant="outline" className="min-w-28" disabled={loadingMore} onClick={() => void search(committedQuery, page.nextCursor)}>
                    {loadingMore ? <Loader2 className="animate-spin" aria-hidden="true" /> : null}
                    加载更多
                  </Button>
                </div>
              ) : null}
            </>
          )}
        </div>

        <DialogFooter className="mt-2 flex flex-col justify-between border-t pt-2 sm:flex-row sm:justify-between">
          <div className="mb-2 flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-muted-foreground sm:mb-0" aria-live="polite">
            <span>{loading ? "" : results.length ? `已显示 ${results.length} 份构成` : ""}</span>
            <span>{page ? `浏览量截至 ${page.viewsThrough}` : ""}</span>
          </div>
          <Button variant="outline" size="sm" onClick={() => changeOpen(false)} className="hidden sm:inline-flex">关闭</Button>
        </DialogFooter>
      </SearchDialogContent>
    </Dialog>
  );
}
