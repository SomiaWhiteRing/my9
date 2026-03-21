"use client";

import Image from "next/image";
import Link from "next/link";
import { useEffect, useMemo, useRef, useState } from "react";
import { useSearchParams } from "next/navigation";
import { ArrowUp, ChevronsUpDown, Globe } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { SubjectKindIcon } from "@/components/subject/SubjectKindIcon";
import { SiteFooter } from "@/components/layout/SiteFooter";
import { SupportButton } from "@/components/SupportButton";
import {
  SubjectKind,
  SUBJECT_KIND_ORDER,
  getSubjectKindMeta,
  parseSubjectKind,
} from "@/lib/subject-kind";
import type { TrendGameItem, TrendResponse, TrendPeriod, TrendView, TrendYearPage } from "@/lib/share/types";
import { resolveSubjectLink } from "@/lib/subject-source";
import { cn } from "@/lib/utils";

type TrendsApiResponse = TrendResponse & { ok: boolean };

type TrendsClientCacheEntry = {
  expiresAt: number;
  response: TrendResponse;
};

const DAY_MS = 24 * 60 * 60 * 1000;
// 北京时间 2026-03-09 10:00（UTC+8）= UTC 2026-03-09 02:00
const PROJECT_LAUNCHED_AT_MS = Date.UTC(2026, 2, 9, 2, 0, 0, 0);

const PERIOD_OPTIONS: Array<{ value: TrendPeriod; label: string; requiredMs: number }> = [
  { value: "today", label: "今天", requiredMs: 0 },
  { value: "24h", label: "24小时", requiredMs: DAY_MS },
  { value: "7d", label: "7天", requiredMs: 7 * DAY_MS },
  { value: "30d", label: "30天", requiredMs: 30 * DAY_MS },
  { value: "90d", label: "90天", requiredMs: 90 * DAY_MS },
  { value: "180d", label: "180天", requiredMs: 180 * DAY_MS },
  { value: "all", label: "全部", requiredMs: 0 },
];

const VIEW_OPTIONS: Array<{ value: TrendView; label: string }> = [
  { value: "overall", label: "综合" },
  { value: "genre", label: "类型" },
  { value: "decade", label: "年代Top5" },
  { value: "year", label: "年份Top5" },
];

const OVERALL_PAGE_SIZE = 20;
const OVERALL_PAGE_COUNT = 5;
const GROUPED_BUCKET_LIMIT = 20;
const GROUPED_GAMES_PER_BUCKET = 5;
const BANGUMI_TRENDS_COVER_WIDTH = 100;
const TMDB_TRENDS_COVER_WIDTH = "w92";
const APPLE_MUSIC_TRENDS_COVER_EDGE = 120;
const TOP_FAB_SHOW_AFTER_PX = 360;
const TOP_FAB_DIRECTION_EPSILON_PX = 2;
const TRENDS_CLIENT_CACHE_TTL_MS = 2 * 60 * 1000;
const TRENDS_CLIENT_CACHE_MAX = 96;
const OVERALL_PAGE_GROUPS = Array.from({ length: OVERALL_PAGE_COUNT }, (_, index) => {
  const startRank = index * OVERALL_PAGE_SIZE + 1;
  const endRank = (index + 1) * OVERALL_PAGE_SIZE;
  return {
    page: index + 1,
    label: `${startRank}-${endRank}`,
  };
});
const YEAR_PAGE_OPTIONS: Array<{ value: TrendYearPage; label: string }> = [
  { value: "recent", label: "现代" },
  { value: "legacy", label: "经典" },
];
const DEFAULT_TREND_KIND: SubjectKind = "game";
const DEFAULT_TREND_PERIOD: TrendPeriod = "7d";
const DEFAULT_TREND_VIEW: TrendView = "overall";
const DEFAULT_TREND_OVERALL_PAGE = 1;
const DEFAULT_TREND_YEAR_PAGE: TrendYearPage = "recent";

function formatDateTime(value: number | null) {
  if (!value) return "暂无";
  return new Date(value).toLocaleString("zh-CN", { hour12: false });
}

function formatPeriodLabel(period: TrendPeriod): string {
  switch (period) {
    case "today":
      return "今天（自然日）";
    case "24h":
      return "最近24小时";
    case "7d":
      return "最近7天";
    case "30d":
      return "最近30天";
    case "90d":
      return "最近90天";
    case "180d":
      return "最近180天";
    case "all":
    default:
      return "全周期";
  }
}

function shouldTopCropCover(kind: SubjectKind) {
  return kind === "character" || kind === "person";
}

function isOverallOnlyKind(kind: SubjectKind): boolean {
  return kind === "character" || kind === "person";
}

function resolveViewByKind(kind: SubjectKind, view: TrendView): TrendView {
  if (isOverallOnlyKind(kind)) {
    return "overall";
  }
  return view;
}

function parseTrendKindParam(value: string | null): SubjectKind {
  return parseSubjectKind(value) ?? DEFAULT_TREND_KIND;
}

function parseTrendPeriodParam(value: string | null): TrendPeriod {
  if (PERIOD_OPTIONS.some((option) => option.value === value)) {
    return value as TrendPeriod;
  }
  return DEFAULT_TREND_PERIOD;
}

function parseTrendViewParam(value: string | null): TrendView {
  if (VIEW_OPTIONS.some((option) => option.value === value)) {
    return value as TrendView;
  }
  return DEFAULT_TREND_VIEW;
}

function parseTrendOverallPageParam(value: string | null): number {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 1 || parsed > OVERALL_PAGE_COUNT) {
    return DEFAULT_TREND_OVERALL_PAGE;
  }
  return parsed;
}

function parseTrendYearPageParam(value: string | null): TrendYearPage {
  return value === "legacy" ? "legacy" : DEFAULT_TREND_YEAR_PAGE;
}

function toTrendsCoverUrl(cover: string | null | undefined): string | null {
  if (!cover) return null;

  const trimmed = cover.trim();
  if (!trimmed) return null;

  try {
    const normalized = trimmed.startsWith("//") ? `https:${trimmed}` : trimmed;
    const parsed = new URL(normalized);

    if (parsed.hostname === "lain.bgm.tv") {
      const normalizedPath = parsed.pathname.replace(/^\/+/, "");
      const pathWithoutResize = normalizedPath.replace(/^r\/\d+\//, "");
      parsed.pathname = `/r/${BANGUMI_TRENDS_COVER_WIDTH}/${pathWithoutResize}`;
      return parsed.toString();
    }

    if (parsed.hostname === "image.tmdb.org") {
      parsed.pathname = parsed.pathname.replace(/^\/t\/p\/[^/]+\//, `/t/p/${TMDB_TRENDS_COVER_WIDTH}/`);
      return parsed.toString();
    }

    if (parsed.hostname.endsWith("mzstatic.com")) {
      parsed.pathname = parsed.pathname.replace(
        /\/\d+x\d+([a-z]*)\.(jpg|jpeg|png|webp)$/i,
        `/${APPLE_MUSIC_TRENDS_COVER_EDGE}x${APPLE_MUSIC_TRENDS_COVER_EDGE}$1.$2`
      );
      return parsed.toString();
    }

    return parsed.toString();
  } catch {
    return trimmed;
  }
}

function isPeriodDisabled(option: { value: TrendPeriod; requiredMs: number }, nowMs: number) {
  if (nowMs < PROJECT_LAUNCHED_AT_MS) {
    return option.value !== "today";
  }

  if (option.value === "today" || option.value === "all") {
    return false;
  }

  const elapsedSinceLaunchMs = nowMs - PROJECT_LAUNCHED_AT_MS;
  return option.requiredMs > elapsedSinceLaunchMs;
}

function isGroupedView(view: TrendView): boolean {
  return view === "genre" || view === "decade" || view === "year";
}

function groupedBucketHint(view: TrendView): string {
  switch (view) {
    case "genre":
      return `该分类下作品 Top${GROUPED_GAMES_PER_BUCKET}`;
    case "decade":
      return `该年代下作品 Top${GROUPED_GAMES_PER_BUCKET}`;
    case "year":
      return `该年份下作品 Top${GROUPED_GAMES_PER_BUCKET}`;
    default:
      return `该分组下作品 Top${GROUPED_GAMES_PER_BUCKET}`;
  }
}

function buildTrendsClientCacheKey(
  kind: SubjectKind,
  period: TrendPeriod,
  view: TrendView,
  overallPage: number,
  yearPage: TrendYearPage
) {
  return `${kind}:${period}:${view}:op${overallPage}:yp${yearPage}`;
}

function normalizeTrendsApiResponse(
  response: Partial<TrendsApiResponse> & { error?: string }
): TrendResponse {
  return {
    period: response.period as TrendPeriod,
    view: response.view as TrendView,
    sampleCount: Number(response.sampleCount || 0),
    range: {
      from: typeof response.range?.from === "number" ? response.range.from : null,
      to: typeof response.range?.to === "number" ? response.range.to : null,
    },
    lastUpdatedAt: Number(response.lastUpdatedAt || Date.now()),
    items: Array.isArray(response.items) ? response.items : [],
  };
}

function pruneExpiredTrendsClientCache(cache: Map<string, TrendsClientCacheEntry>, now = Date.now()) {
  const expiredKeys: string[] = [];
  cache.forEach((value, key) => {
    if (!value || typeof value.expiresAt !== "number" || value.expiresAt <= now) {
      expiredKeys.push(key);
    }
  });

  for (const key of expiredKeys) {
    cache.delete(key);
  }
}

function trimTrendsClientCache(cache: Map<string, TrendsClientCacheEntry>) {
  while (cache.size > TRENDS_CLIENT_CACHE_MAX) {
    const firstKey = cache.keys().next().value;
    if (!firstKey) return;
    cache.delete(firstKey);
  }
}

interface TrendGameMiniCardProps {
  kind: SubjectKind;
  rank: number;
  game: TrendGameItem | null;
  count: number;
  tagLabel?: string | null;
  showReleaseYear?: boolean;
}

function TrendGameMiniCard({ kind, rank, game, count, tagLabel, showReleaseYear = true }: TrendGameMiniCardProps) {
  const coverUrl = game ? toTrendsCoverUrl(game.cover) : null;
  const title = game ? game.localizedName || game.name : "暂无条目";
  const subjectResolution = game
    ? resolveSubjectLink({
        kind,
        subject: {
          id: game.id,
          name: game.name,
          localizedName: game.localizedName,
        },
      })
    : null;
  const subjectUrl = subjectResolution?.url ?? null;
  const sourceLabel = subjectResolution?.sourceLabel ?? "Bangumi";
  const subtitle = game && game.localizedName && game.localizedName !== game.name ? game.name : null;

  return (
    <article className="rounded-xl border border-border bg-card p-3 transition-colors hover:bg-accent/40">
      <div className="flex items-start gap-2.5">
        <span className="w-8 flex-shrink-0 pt-0.5 text-xs font-bold text-sky-500">#{rank}</span>

        {game ? (
          <>
            <div className="flex min-w-0 flex-1 items-start gap-2.5">
              <div className="h-16 w-12 flex-shrink-0 overflow-hidden rounded border border-border bg-muted">
                {coverUrl ? (
                  <Image
                    src={coverUrl}
                    alt={game.name}
                    width={48}
                    height={64}
                    unoptimized
                    className={cn("h-full w-full object-cover", shouldTopCropCover(kind) && "object-top")}
                  />
                ) : (
                  <div className="flex h-full w-full items-center justify-center text-[10px] text-muted-foreground">无图</div>
                )}
              </div>
              <div className="min-w-0 space-y-1">
                <p className="truncate text-sm font-semibold text-card-foreground">
                  {title}
                  {showReleaseYear && game.releaseYear ? ` (${game.releaseYear})` : ""}
                </p>
                {subtitle ? <p className="truncate text-xs text-muted-foreground">{subtitle}</p> : null}
                <span className="inline-flex items-center rounded-full border border-border bg-muted px-2 py-0.5 text-[11px] text-muted-foreground">
                  选定 {count.toLocaleString("zh-CN")}
                </span>
                {tagLabel ? (
                  <div>
                    <span className="inline-flex items-center rounded-full border border-sky-200 bg-sky-50 px-2 py-0.5 text-[11px] font-semibold text-sky-700">
                      {tagLabel}
                    </span>
                  </div>
                ) : null}
              </div>
            </div>

            {subjectUrl ? (
              <a
                href={subjectUrl}
                target="_blank"
                rel="noopener noreferrer"
                title={`在 ${sourceLabel} 查看`}
                className="rounded-md border border-border bg-muted p-1.5 text-muted-foreground transition-colors hover:bg-accent hover:text-accent-foreground"
              >
                <Globe className="h-4 w-4" />
              </a>
            ) : (
              <span className="rounded-md border border-border bg-muted p-1.5 text-muted-foreground/50">
                <Globe className="h-4 w-4" />
              </span>
            )}
          </>
        ) : (
          <div className="flex min-w-0 flex-1 items-center justify-between gap-2">
            <p className="text-xs text-muted-foreground">{title}</p>
            <span className="rounded-md border border-border bg-muted p-1.5 text-muted-foreground/50">
              <Globe className="h-4 w-4" />
            </span>
          </div>
        )}
      </div>
    </article>
  );
}

interface TrendsClientPageProps {
  initialKind?: SubjectKind;
  initialPeriod?: TrendPeriod;
  initialView?: TrendView;
  initialOverallPage?: number;
  initialYearPage?: TrendYearPage;
  initialData?: TrendResponse | null;
  initialError?: string;
}

export default function TrendsClientPage({
  initialKind = DEFAULT_TREND_KIND,
  initialPeriod = DEFAULT_TREND_PERIOD,
  initialView = DEFAULT_TREND_VIEW,
  initialOverallPage = DEFAULT_TREND_OVERALL_PAGE,
  initialYearPage = DEFAULT_TREND_YEAR_PAGE,
  initialData = null,
  initialError = "",
}: TrendsClientPageProps) {
  const searchParams = useSearchParams();
  const searchParamsKey = searchParams.toString();
  const urlState = useMemo(() => {
    const params = new URLSearchParams(searchParamsKey);
    const nextKind = parseTrendKindParam(params.get("kind"));
    const nextView = resolveViewByKind(
      nextKind,
      parseTrendViewParam(params.get("view"))
    );

    return {
      kind: nextKind,
      period: parseTrendPeriodParam(params.get("period")),
      view: nextView,
      overallPage: parseTrendOverallPageParam(params.get("overallPage")),
      yearPage: parseTrendYearPageParam(params.get("yearPage")),
    };
  }, [searchParamsKey]);
  const nowMs = Date.now();
  const shouldRefetchOnMount = Boolean(
    !initialData ||
      initialError ||
      (initialData && initialData.sampleCount >= 30 && Array.isArray(initialData.items) && initialData.items.length === 0)
  );
  const [kind, setKind] = useState<SubjectKind>(initialKind ?? urlState.kind);
  const [period, setPeriod] = useState<TrendPeriod>(initialPeriod ?? urlState.period);
  const [view, setView] = useState<TrendView>(
    resolveViewByKind(initialKind ?? urlState.kind, initialView ?? urlState.view)
  );
  const [overallPage, setOverallPage] = useState<number>(initialOverallPage ?? urlState.overallPage);
  const [yearPage, setYearPage] = useState<TrendYearPage>(initialYearPage ?? urlState.yearPage);
  const [data, setData] = useState<TrendResponse | null>(initialData);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(initialError);
  const [showTopFab, setShowTopFab] = useState(false);
  const [kindPickerOpen, setKindPickerOpen] = useState(false);
  const skipFirstEffectRef = useRef(!shouldRefetchOnMount);
  const trendsClientCacheRef = useRef<Map<string, TrendsClientCacheEntry>>(new Map());
  const trendsRequestAbortRef = useRef<AbortController | null>(null);
  const kindTabsScrollerRef = useRef<HTMLDivElement | null>(null);
  const kindTabRefs = useRef<Partial<Record<SubjectKind, HTMLButtonElement | null>>>({});
  const kindTabsAutoScrolledRef = useRef(false);
  const isCurrentKindOverallOnly = isOverallOnlyKind(kind);
  const requestOverallPage = view === "overall" ? overallPage : 1;
  const requestYearPage: TrendYearPage = view === "year" ? yearPage : "recent";

  useEffect(() => {
    setKind(urlState.kind);
    setPeriod(urlState.period);
    setView(urlState.view);
    setOverallPage(urlState.overallPage);
    setYearPage(urlState.yearPage);
  }, [urlState.kind, urlState.overallPage, urlState.period, urlState.view, urlState.yearPage]);

  useEffect(() => {
    // Keep mount-time recovery refetch path intact for stale-empty/error SSR payloads.
    if (!initialData || shouldRefetchOnMount) return;

    const initialRequestOverallPage = initialView === "overall" ? initialOverallPage : 1;
    const initialRequestYearPage: TrendYearPage = initialView === "year" ? initialYearPage : "recent";
    const cacheKey = buildTrendsClientCacheKey(
      initialKind,
      initialPeriod,
      initialView,
      initialRequestOverallPage,
      initialRequestYearPage
    );

    trendsClientCacheRef.current.set(cacheKey, {
      expiresAt: Date.now() + TRENDS_CLIENT_CACHE_TTL_MS,
      response: initialData,
    });
    pruneExpiredTrendsClientCache(trendsClientCacheRef.current);
    trimTrendsClientCache(trendsClientCacheRef.current);
  }, [initialData, initialKind, initialOverallPage, initialPeriod, initialView, initialYearPage, shouldRefetchOnMount]);

  useEffect(() => {
    if (skipFirstEffectRef.current) {
      skipFirstEffectRef.current = false;
      return;
    }

    const cacheKey = buildTrendsClientCacheKey(kind, period, view, requestOverallPage, requestYearPage);
    const now = Date.now();
    pruneExpiredTrendsClientCache(trendsClientCacheRef.current, now);
    const cached = trendsClientCacheRef.current.get(cacheKey);
    if (cached && cached.expiresAt > now) {
      setError("");
      setLoading(false);
      setData(cached.response);
      return;
    }

    let active = true;
    const abortController = new AbortController();
    if (trendsRequestAbortRef.current) {
      trendsRequestAbortRef.current.abort();
    }
    trendsRequestAbortRef.current = abortController;

    async function loadTrends() {
      setLoading(true);
      setError("");

      try {
        const params = new URLSearchParams({
          kind,
          period,
          view,
        });
        if (view === "overall") {
          params.set("overallPage", String(requestOverallPage));
        }
        if (view === "year") {
          params.set("yearPage", requestYearPage);
        }
        const response = await fetch(`/api/trends?${params.toString()}`, {
          signal: abortController.signal,
        });
        const json = (await response.json()) as Partial<TrendsApiResponse> & { error?: string };

        if (!active || abortController.signal.aborted) return;
        if (!response.ok || !json.ok) {
          setError(json.error || "趋势数据加载失败");
          setData(null);
          return;
        }

        const normalizedResponse = normalizeTrendsApiResponse(json);
        trendsClientCacheRef.current.set(cacheKey, {
          expiresAt: Date.now() + TRENDS_CLIENT_CACHE_TTL_MS,
          response: normalizedResponse,
        });
        pruneExpiredTrendsClientCache(trendsClientCacheRef.current);
        trimTrendsClientCache(trendsClientCacheRef.current);

        setData(normalizedResponse);
      } catch {
        if (!active || abortController.signal.aborted) return;
        setError("趋势数据加载失败");
        setData(null);
      } finally {
        if (active && !abortController.signal.aborted) {
          setLoading(false);
        }
      }
    }

    loadTrends();
    return () => {
      active = false;
      abortController.abort();
      if (trendsRequestAbortRef.current === abortController) {
        trendsRequestAbortRef.current = null;
      }
    };
  }, [kind, period, requestOverallPage, requestYearPage, view]);

  useEffect(() => {
    setOverallPage(1);
    setYearPage("recent");
  }, [kind, period]);

  useEffect(() => {
    if (!isCurrentKindOverallOnly || view === "overall") {
      return;
    }
    setView("overall");
  }, [isCurrentKindOverallOnly, view]);

  useEffect(() => {
    const scroller = kindTabsScrollerRef.current;
    const activeButton = kindTabRefs.current[kind];
    if (!scroller || !activeButton) return;

    const scrollerRect = scroller.getBoundingClientRect();
    const activeRect = activeButton.getBoundingClientRect();
    const targetLeft =
      scroller.scrollLeft +
      (activeRect.left - scrollerRect.left) -
      (scrollerRect.width / 2 - activeRect.width / 2);
    const maxLeft = Math.max(0, scroller.scrollWidth - scroller.clientWidth);
    const clampedLeft = Math.min(maxLeft, Math.max(0, targetLeft));

    scroller.scrollTo({
      left: clampedLeft,
      behavior: kindTabsAutoScrolledRef.current ? "smooth" : "auto",
    });
    kindTabsAutoScrolledRef.current = true;
  }, [kind]);

  useEffect(() => {
    let ticking = false;
    let lastScrollY = Math.max(window.scrollY, 0);

    const updateTopFabVisible = () => {
      const currentScrollY = Math.max(window.scrollY, 0);
      const passedThreshold = currentScrollY > TOP_FAB_SHOW_AFTER_PX;
      const scrollingDown = currentScrollY - lastScrollY > TOP_FAB_DIRECTION_EPSILON_PX;
      const scrollingUp = lastScrollY - currentScrollY > TOP_FAB_DIRECTION_EPSILON_PX;

      setShowTopFab((prev) => {
        if (!passedThreshold) return false;
        if (scrollingDown) return true;
        if (scrollingUp) return false;
        return prev;
      });

      lastScrollY = currentScrollY;
    };

    const onScroll = () => {
      if (ticking) return;
      ticking = true;
      window.requestAnimationFrame(() => {
        updateTopFabVisible();
        ticking = false;
      });
    };

    updateTopFabVisible();
    window.addEventListener("scroll", onScroll, { passive: true });
    return () => {
      window.removeEventListener("scroll", onScroll);
    };
  }, []);

  const hasInsufficientSamples = (data?.sampleCount ?? 0) < 30;
  const rankingItems = useMemo(() => data?.items ?? [], [data?.items]);

  const visibleItems = useMemo(() => {
    if (view === "genre") {
      return rankingItems.slice(0, GROUPED_BUCKET_LIMIT);
    }
    return rankingItems;
  }, [rankingItems, view]);

  const nonGenreVisibleItems = useMemo(() => {
    if (isGroupedView(view)) {
      return [];
    }
    return visibleItems;
  }, [view, visibleItems]);

  const overallRankOffset = view === "overall" ? (overallPage - 1) * OVERALL_PAGE_SIZE : 0;
  const showOverallPagination = Boolean(view === "overall" && !error && data && !hasInsufficientSamples);
  const showYearPagination = Boolean(view === "year" && !error && data && !hasInsufficientSamples);
  const trendsTitle = useMemo(() => {
    return `构成大家的${getSubjectKindMeta(kind).trendLabel}`;
  }, [kind]);

  const topCardSummary = useMemo(() => {
    return `目标周期：${formatPeriodLabel(data?.period ?? period)}`;
  }, [data?.period, period]);

  useEffect(() => {
    document.title = trendsTitle;
  }, [trendsTitle]);

  function handleBackToTop() {
    window.scrollTo({ top: 0, behavior: "smooth" });
  }

  function switchKind(nextKind: SubjectKind) {
    setKindPickerOpen(false);
    if (nextKind === kind) return;
    const nextView = resolveViewByKind(nextKind, view);
    if (nextView !== view) {
      setView(nextView);
    }
    setKind(nextKind);
  }

  return (
    <main className="min-h-screen bg-background text-foreground">
      <section className="w-full border-b border-border bg-card shadow-sm">
        <div className="mx-auto w-full max-w-6xl px-4 py-5 sm:px-6">
          <Link
            href={`/${kind}`}
            className="inline-flex items-center rounded-full border border-border bg-muted px-3 py-1 text-xs font-semibold text-muted-foreground transition-colors hover:bg-accent hover:text-accent-foreground"
          >
            返回主页面
          </Link>

          <div className="mt-4 flex flex-wrap flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
            <div className="space-y-1 flex-shrink-0">
              <h1 className="text-3xl font-bold tracking-tight text-foreground">大家的构成</h1>
              <p className="text-sm text-muted-foreground">{topCardSummary}</p>
              <p className="text-xs text-amber-600 dark:text-amber-400">
                每小时30分更新。
              </p>
              <SupportButton/>
              <p className="text-xs text-muted-foreground">
                当前类别样本数：{data?.sampleCount ?? "-"}
                {/* 集计区间：{formatDateTime(data?.range.from ?? null)} ～ {formatDateTime(data?.range.to ?? null)} */}
              </p>
              <p className="text-xs text-muted-foreground">最后更新：{formatDateTime(data?.lastUpdatedAt ?? null)}</p>
            </div>

            <div className="mt-auto flex flex-col items-end space-y-2 ml-auto">
              <div className="self-end md:hidden">
                <button
                  type="button"
                  onClick={() => setKindPickerOpen(true)}
                  className="inline-flex h-8 items-center justify-center gap-1.5 rounded-full border border-border bg-card px-2.5 text-xs font-semibold text-card-foreground transition-colors hover:bg-accent hover:text-accent-foreground"
                  aria-label="切换观测类别"
                >
                  <SubjectKindIcon kind={kind} className="h-3.5 w-3.5" />
                  {getSubjectKindMeta(kind).label}
                  <ChevronsUpDown className="h-3.5 w-3.5 text-muted-foreground" />
                </button>
              </div>

              <div ref={kindTabsScrollerRef} className="hidden w-full max-w-full overflow-x-auto md:block">
                <div className="inline-flex min-w-max overflow-hidden rounded-full border border-border bg-card">
                  {SUBJECT_KIND_ORDER.map((option) => {
                    const optionMeta = getSubjectKindMeta(option);
                    return (
                      <button
                        key={option}
                        type="button"
                        ref={(element) => {
                          kindTabRefs.current[option] = element;
                        }}
                        className={cn(
                          "inline-flex h-8 cursor-pointer items-center justify-center gap-1.5 whitespace-nowrap border-l border-border px-2.5 text-xs font-semibold transition-colors first:border-l-0",
                          option === kind
                            ? "bg-foreground text-background"
                            : "bg-card text-card-foreground hover:bg-accent hover:text-accent-foreground"
                        )}
                        onClick={() => switchKind(option)}
                      >
                        <SubjectKindIcon kind={option} className="h-3.5 w-3.5" />
                        {optionMeta.label}
                      </button>
                    );
                  })}
                </div>
              </div>

              <div className="self-end overflow-x-auto md:overflow-visible">
                <div className="inline-flex overflow-hidden rounded-full border border-border bg-card">
                  {PERIOD_OPTIONS.map((option) => {
                    const disabled = isPeriodDisabled(option, nowMs);
                    return (
                      <button
                        key={option.value}
                        type="button"
                        disabled={disabled}
                        className={cn(
                          "inline-flex h-8 cursor-pointer items-center justify-center whitespace-nowrap border-l border-border px-2.5 text-xs font-semibold transition-colors first:border-l-0",
                          option.value === period
                            ? "bg-foreground text-background"
                            : "bg-card text-card-foreground hover:bg-accent hover:text-accent-foreground",
                          disabled && "cursor-not-allowed bg-muted text-muted-foreground/70 hover:bg-muted"
                        )}
                        onClick={() => setPeriod(option.value)}
                      >
                        {option.label}
                      </button>
                    );
                  })}
                </div>
              </div>
            </div>
          </div>
        </div>
      </section>

      <div className="mx-auto w-full max-w-6xl px-4 pb-12 pt-6 sm:px-6">
        <section className="mb-5 rounded-2xl border border-border bg-card p-5 shadow-sm">
          <div className="mb-4">
            <h2 className="text-lg font-bold text-foreground">排行榜</h2>
            {!isCurrentKindOverallOnly ? (
              <div className="mt-2 flex flex-wrap gap-2">
                {VIEW_OPTIONS.map((option) => (
                  <Button
                    key={option.value}
                    size="sm"
                    variant={option.value === view ? "default" : "outline"}
                    className={
                      option.value === view
                        ? "rounded-full border border-foreground bg-foreground px-3 py-1.5 text-xs font-semibold text-background"
                        : "rounded-full border border-border bg-card px-3 py-1.5 text-xs font-semibold text-card-foreground hover:bg-accent hover:text-accent-foreground"
                    }
                    onClick={() => setView(option.value)}
                  >
                    {option.label}
                  </Button>
                ))}
              </div>
            ) : null}

            {showOverallPagination ? (
              <div className="mt-3 flex flex-wrap gap-2">
                {OVERALL_PAGE_GROUPS.map((group) => {
                  const active = group.page === overallPage;
                  return (
                    <Button
                      key={group.label}
                      size="sm"
                      variant={active ? "default" : "outline"}
                      className={cn(
                        "rounded-full px-2 py-1.5 text-xs font-semibold",
                        active
                          ? "border-foreground bg-foreground text-background"
                          : "border-border bg-card text-card-foreground hover:bg-accent hover:text-accent-foreground"
                      )}
                      onClick={() => setOverallPage(group.page)}
                    >
                      {group.label}
                    </Button>
                  );
                })}
              </div>
            ) : null}

            {showYearPagination ? (
              <div className="mt-3 flex flex-wrap gap-2">
                {YEAR_PAGE_OPTIONS.map((option) => {
                  const active = option.value === yearPage;
                  return (
                    <Button
                      key={option.value}
                      size="sm"
                      variant={active ? "default" : "outline"}
                      className={cn(
                        "rounded-full px-3 py-1.5 text-xs font-semibold",
                        active
                          ? "border-foreground bg-foreground text-background"
                          : "border-border bg-card text-card-foreground hover:bg-accent hover:text-accent-foreground"
                      )}
                      onClick={() => setYearPage(option.value)}
                    >
                      {option.label}
                    </Button>
                  );
                })}
              </div>
            ) : null}
          </div>

          {loading ? <p className="text-sm text-muted-foreground">加载中...</p> : null}
          {!loading && error ? <p className="text-sm text-rose-600">{error}</p> : null}

          {!loading && !error && data && hasInsufficientSamples ? (
            <div className="rounded-2xl border border-dashed border-border bg-muted p-10 text-center text-sm text-muted-foreground">
              数据暂缺，请稍后再试
            </div>
          ) : null}

          {!loading && !error && data && !hasInsufficientSamples ? (
            isGroupedView(view) ? (
              <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
                {visibleItems.length === 0 ? (
                  <p className="text-sm text-muted-foreground">暂无排行数据。</p>
                ) : (
                  visibleItems.map((bucket, bucketIndex) => {
                    const topGames = bucket.games.slice(0, GROUPED_GAMES_PER_BUCKET);
                    return (
                      <article
                        key={bucket.key}
                        className="rounded-2xl border border-border bg-muted/40 p-4 shadow-sm"
                      >
                        <div className="mb-3 flex items-start justify-between gap-3">
                          <div className="min-w-0">
                            <p className="truncate text-xl font-bold text-foreground">
                              {view === "genre" ? `#${bucketIndex + 1} ${bucket.label}` : bucket.label}
                            </p>
                            <p className="mt-1 text-xs text-muted-foreground">{groupedBucketHint(view)}</p>
                          </div>
                          <div className="shrink-0 rounded-xl border border-border bg-card px-3 py-2 text-base font-bold text-card-foreground">
                            选定数：{bucket.count.toLocaleString("zh-CN")}
                          </div>
                        </div>

                        <div className="space-y-2">
                          {topGames.map((game, gameIndex) => (
                            <TrendGameMiniCard
                              kind={kind}
                              key={`${bucket.key}:${game.id}:${gameIndex}`}
                              rank={gameIndex + 1}
                              game={game}
                              count={game.count}
                              showReleaseYear={view !== "year"}
                            />
                          ))}
                        </div>
                      </article>
                    );
                  })
                )}
              </div>
            ) : (
              <div className="grid grid-cols-1 gap-3 md:grid-cols-2 lg:grid-cols-3">
                {nonGenreVisibleItems.length === 0 ? (
                  <p className="text-sm text-muted-foreground">暂无排行数据。</p>
                ) : (
                  nonGenreVisibleItems.map((bucket, bucketIndex) => {
                    const game = bucket.games[0] ?? null;
                    const rank = overallRankOffset + bucketIndex + 1;
                    const tagLabel = view === "overall" ? null : bucket.label;
                    return (
                      <TrendGameMiniCard
                        kind={kind}
                        key={bucket.key}
                        rank={rank}
                        game={game}
                        count={bucket.count}
                        tagLabel={tagLabel}
                      />
                    );
                  })
                )}
              </div>
            )
          ) : null}
        </section>

        <SiteFooter kind={kind} />
      </div>

      <Dialog open={kindPickerOpen} onOpenChange={setKindPickerOpen}>
        <DialogContent className="w-[86vw] max-w-[21rem] rounded-2xl p-4 sm:max-w-md sm:p-6">
          <DialogHeader>
            <DialogTitle>切换观测类别</DialogTitle>
          </DialogHeader>
          <div className="grid grid-cols-2 gap-2 sm:grid-cols-2">
            {SUBJECT_KIND_ORDER.map((item) => {
              const meta = getSubjectKindMeta(item);
              const active = item === kind;
              return (
                <Button
                  key={item}
                  type="button"
                  variant="outline"
                  onClick={() => switchKind(item)}
                  className={cn(
                    "h-auto justify-start gap-3 rounded-xl px-4 py-3 text-left",
                    active && "border-sky-300 bg-sky-50 text-sky-700 dark:border-sky-800 dark:bg-sky-950/50 dark:text-sky-200"
                  )}
                >
                  <SubjectKindIcon kind={item} className="h-4 w-4" />
                  <span className="font-semibold">{meta.label}</span>
                </Button>
              );
            })}
          </div>
        </DialogContent>
      </Dialog>

      <button
        type="button"
        aria-label="回到顶部"
        onClick={handleBackToTop}
        className={cn(
          "fixed bottom-6 right-6 z-40 inline-flex h-14 w-14 items-center justify-center rounded-full",
          "bg-sky-600 text-white shadow-[0_6px_10px_rgba(0,0,0,0.22),0_2px_4px_rgba(0,0,0,0.2)]",
          "transition-all duration-200 hover:bg-sky-500 dark:hover:bg-sky-500/90 active:scale-95",
          "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-sky-300 focus-visible:ring-offset-2 focus-visible:ring-offset-background",
          "md:bottom-8 md:right-8",
          showTopFab
            ? "pointer-events-auto translate-y-0 opacity-100"
            : "pointer-events-none translate-y-3 opacity-0"
        )}
      >
        <div className="flex flex-col items-center leading-none">
          <ArrowUp className="h-4 w-4" />
          <span className="mt-0.5 text-[10px] font-semibold tracking-[0.08em]">TOP</span>
        </div>
      </button>
    </main>
  );
}
