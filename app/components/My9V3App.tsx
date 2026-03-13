"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { usePathname, useRouter } from "next/navigation";
import { ChevronRight, ChevronsUpDown } from "lucide-react";
import { Input } from "@/components/ui/input";
import { SharePlatformActions } from "@/components/share/SharePlatformActions";
import { SiteFooter } from "@/components/layout/SiteFooter";
import { SubjectKindIcon } from "@/components/subject/SubjectKindIcon";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { ActionCluster } from "@/app/components/v3/ActionCluster";
import { CommentDialog } from "@/app/components/v3/CommentDialog";
import { InlineToast, ToastKind } from "@/app/components/v3/InlineToast";
import { NineGridBoard } from "@/app/components/v3/NineGridBoard";
import { SearchDialog } from "@/app/components/v3/SearchDialog";
import { SelectedGamesList } from "@/app/components/v3/SelectedGamesList";
import { SupportButton } from "@/components/SupportButton";
import {
  SUBJECT_KIND_ORDER,
  SubjectKind,
  getSubjectKindMeta,
  getSubjectKindShareTitle,
  parseSubjectKind,
} from "@/lib/subject-kind";
import { normalizeSearchQuery } from "@/lib/search/query";
import { SubjectSearchResponse, ShareGame } from "@/lib/share/types";
import { cn } from "@/lib/utils";

type ToastState = {
  kind: ToastKind;
  message: string;
} | null;

type DraftSnapshot = {
  games: Array<ShareGame | null>;
  creatorName: string;
};

type SearchMeta = {
  noResultQuery: string | null;
};

type InitialReadonlyShareData = {
  shareId: string;
  kind: SubjectKind;
  creatorName: string | null;
  games: Array<ShareGame | null>;
};

function createSearchMeta(noResultQuery: string | null = null): SearchMeta {
  return {
    noResultQuery,
  };
}

function createEmptyGames() {
  return Array.from({ length: 9 }, () => null as ShareGame | null);
}

function cloneGames(games: Array<ShareGame | null>) {
  return games.map((item) => (item ? { ...item } : null));
}

function normalizeGamesForState(games?: Array<ShareGame | null>) {
  if (!Array.isArray(games) || games.length !== 9) {
    return createEmptyGames();
  }
  return cloneGames(games);
}

const CREATOR_STORAGE_KEY = "my-nine-creator:v1";
const SEARCH_CLIENT_CACHE_SESSION_KEY = "my-nine-search-cache:v1";
const SEARCH_CLIENT_CACHE_TTL_MS = 15 * 60 * 1000;
const SEARCH_CLIENT_CACHE_MAX = 192;
const SEARCH_REQUEST_COOLDOWN_MS = 400;
const SHARE_NAVIGATION_FALLBACK_MS = 1400;

type SearchClientCacheEntry = {
  expiresAt: number;
  response: SubjectSearchResponse;
};

function buildSearchClientCacheKey(kind: SubjectKind, query: string) {
  return `${kind}:${normalizeSearchQuery(query)}`;
}

function pruneExpiredSearchClientCache(cache: Map<string, SearchClientCacheEntry>, now = Date.now()) {
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

function trimSearchClientCache(cache: Map<string, SearchClientCacheEntry>) {
  while (cache.size > SEARCH_CLIENT_CACHE_MAX) {
    const firstKey = cache.keys().next().value;
    if (!firstKey) return;
    cache.delete(firstKey);
  }
}

function readSearchClientCacheFromSession() {
  if (typeof window === "undefined") return new Map<string, SearchClientCacheEntry>();

  try {
    const raw = sessionStorage.getItem(SEARCH_CLIENT_CACHE_SESSION_KEY);
    if (!raw) return new Map<string, SearchClientCacheEntry>();

    const parsed = JSON.parse(raw) as unknown;
    if (!Array.isArray(parsed)) return new Map<string, SearchClientCacheEntry>();

    const restored = new Map<string, SearchClientCacheEntry>();
    for (const item of parsed) {
      if (!Array.isArray(item) || item.length !== 2) continue;
      const [key, value] = item as [unknown, unknown];
      if (typeof key !== "string" || !value || typeof value !== "object") continue;
      const entry = value as Partial<SearchClientCacheEntry>;
      if (typeof entry.expiresAt !== "number" || !entry.response) continue;
      restored.set(key, {
        expiresAt: entry.expiresAt,
        response: entry.response as SubjectSearchResponse,
      });
    }

    pruneExpiredSearchClientCache(restored);
    trimSearchClientCache(restored);
    return restored;
  } catch {
    return new Map<string, SearchClientCacheEntry>();
  }
}

function writeSearchClientCacheToSession(cache: Map<string, SearchClientCacheEntry>) {
  if (typeof window === "undefined") return;
  try {
    const serialized = JSON.stringify(Array.from(cache.entries()));
    sessionStorage.setItem(SEARCH_CLIENT_CACHE_SESSION_KEY, serialized);
  } catch {
    // ignore write errors
  }
}

interface My9V3AppProps {
  kind: SubjectKind;
  initialShareId?: string | null;
  initialShareData?: InitialReadonlyShareData | null;
  readOnlyShare?: boolean;
}

export default function My9V3App({
  kind,
  initialShareId = null,
  initialShareData = null,
  readOnlyShare = false,
}: My9V3AppProps) {
  const router = useRouter();
  const pathname = usePathname();
  const kindMeta = useMemo(() => getSubjectKindMeta(kind), [kind]);

  const [games, setGames] = useState<Array<ShareGame | null>>(() =>
    normalizeGamesForState(initialShareData?.games)
  );
  const [creatorName, setCreatorName] = useState(initialShareData?.creatorName || "");
  const [shareId, setShareId] = useState<string | null>(initialShareData?.shareId || initialShareId);
  const [loadingShare, setLoadingShare] = useState(Boolean(initialShareId) && !initialShareData);
  const [savingShare, setSavingShare] = useState(false);
  const [draftHydrated, setDraftHydrated] = useState(false);
  const [kindPickerOpen, setKindPickerOpen] = useState(false);

  const [toast, setToast] = useState<ToastState>(null);
  const [singleUndoSnapshot, setSingleUndoSnapshot] = useState<DraftSnapshot | null>(null);

  const [selectedSlot, setSelectedSlot] = useState<number | null>(null);
  const [searchOpen, setSearchOpen] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");
  const [searchLoading, setSearchLoading] = useState(false);
  const [searchError, setSearchError] = useState("");
  const [searchResults, setSearchResults] = useState<ShareGame[]>([]);
  const [searchActiveIndex, setSearchActiveIndex] = useState(-1);
  const [searchCommittedQuery, setSearchCommittedQuery] = useState("");
  const searchClientCacheRef = useRef<Map<string, SearchClientCacheEntry>>(new Map());
  const searchClientCacheHydratedRef = useRef(false);
  const lastSearchRequestRef = useRef<{ key: string; requestedAt: number } | null>(null);
  const navigationFallbackTimerRef = useRef<number | null>(null);
  const navigationFallbackTargetRef = useRef<string | null>(null);
  const [searchMeta, setSearchMeta] = useState<SearchMeta>(
    createSearchMeta()
  );

  const [commentOpen, setCommentOpen] = useState(false);
  const [commentText, setCommentText] = useState("");
  const [commentSpoiler, setCommentSpoiler] = useState(false);
  const [commentSlot, setCommentSlot] = useState<number | null>(null);
  const [spoilerExpandedSet, setSpoilerExpandedSet] = useState<Set<number>>(new Set());

  const filledCount = useMemo(() => games.filter((item) => item !== null).length, [games]);
  const allSelected = filledCount === 9;
  const isReadonly = readOnlyShare;

  const draftStorageKey = kindMeta.draftStorageKey;
  const selectionUnit = kindMeta.selectionUnit;
  const shareTitle = getSubjectKindShareTitle(kind);
  function ensureSearchClientCacheHydrated() {
    if (searchClientCacheHydratedRef.current) return;
    searchClientCacheRef.current = readSearchClientCacheFromSession();
    searchClientCacheHydratedRef.current = true;
  }

  function persistSearchClientCache() {
    writeSearchClientCacheToSession(searchClientCacheRef.current);
  }

  function clearNavigationFallback() {
    if (navigationFallbackTimerRef.current !== null) {
      window.clearTimeout(navigationFallbackTimerRef.current);
      navigationFallbackTimerRef.current = null;
    }
    navigationFallbackTargetRef.current = null;
  }

  useEffect(() => {
    if (!toast) return;
    const timer = setTimeout(() => setToast(null), 2800);
    return () => clearTimeout(timer);
  }, [toast]);

  useEffect(() => {
    if (searchClientCacheHydratedRef.current) return;
    searchClientCacheRef.current = readSearchClientCacheFromSession();
    searchClientCacheHydratedRef.current = true;
  }, []);

  useEffect(() => {
    const pendingTarget = navigationFallbackTargetRef.current;
    if (!pendingTarget) return;
    if (pathname !== pendingTarget) return;
    if (navigationFallbackTimerRef.current !== null) {
      window.clearTimeout(navigationFallbackTimerRef.current);
      navigationFallbackTimerRef.current = null;
    }
    navigationFallbackTargetRef.current = null;
  }, [pathname]);

  useEffect(
    () => () => {
      if (navigationFallbackTimerRef.current !== null) {
        window.clearTimeout(navigationFallbackTimerRef.current);
      }
      navigationFallbackTimerRef.current = null;
      navigationFallbackTargetRef.current = null;
    },
    []
  );

  useEffect(() => {
    if (!initialShareData) return;
    if (initialShareData.kind !== kind) return;

    setGames(normalizeGamesForState(initialShareData.games));
    setCreatorName(initialShareData.creatorName || "");
    setShareId(initialShareData.shareId);
    setLoadingShare(false);
  }, [initialShareData, kind]);

  useEffect(() => {
    if (!initialShareId) return;
    if (initialShareData) return;
    const currentShareId: string = initialShareId;
    let active = true;

    async function loadShared() {
      setLoadingShare(true);
      try {
        const response = await fetch(`/api/share?id=${encodeURIComponent(currentShareId)}`);
        const json = await response.json();
        if (!active) return;
        if (!response.ok || !json?.ok) {
          setToast({ kind: "error", message: json?.error || "共享页面加载失败" });
          setLoadingShare(false);
          return;
        }

        const responseKind = parseSubjectKind(json.kind) ?? "game";
        if (responseKind !== kind) {
          setToast({ kind: "error", message: "分享类型与页面不匹配" });
          setLoadingShare(false);
          router.replace(`/${responseKind}/s/${json.shareId || currentShareId}`);
          return;
        }

        const payloadGames = Array.isArray(json.games) ? json.games : createEmptyGames();
        setGames(payloadGames.length === 9 ? payloadGames : createEmptyGames());
        setCreatorName(typeof json.creatorName === "string" ? json.creatorName : "");
        setShareId(json.shareId || currentShareId);
      } catch {
        if (!active) return;
        setToast({ kind: "error", message: "共享页面加载失败" });
      } finally {
        if (active) {
          setLoadingShare(false);
        }
      }
    }

    loadShared();
    return () => {
      active = false;
    };
  }, [initialShareData, initialShareId, kind, router]);

  useEffect(() => {
    if (isReadonly || initialShareId) {
      setDraftHydrated(true);
      return;
    }

    try {
      const raw = localStorage.getItem(draftStorageKey);
      const creatorRaw = localStorage.getItem(CREATOR_STORAGE_KEY);
      if (raw) {
        const parsed = JSON.parse(raw);
        const savedGames = Array.isArray(parsed?.games) ? parsed.games : null;
        if (savedGames && savedGames.length === 9) {
          setGames(savedGames);
        } else {
          setGames(createEmptyGames());
        }
      } else {
        setGames(createEmptyGames());
      }

      if (typeof creatorRaw === "string") {
        setCreatorName(creatorRaw);
      } else {
        setCreatorName("");
      }
    } catch {
      setGames(createEmptyGames());
      setCreatorName("");
    } finally {
      setDraftHydrated(true);
    }
  }, [draftStorageKey, initialShareId, isReadonly]);

  useEffect(() => {
    if (isReadonly || initialShareId || !draftHydrated) return;
    try {
      localStorage.setItem(
        draftStorageKey,
        JSON.stringify({
          games,
        })
      );
      localStorage.setItem(CREATOR_STORAGE_KEY, creatorName);
    } catch {
      // ignore write errors
    }
  }, [games, creatorName, draftHydrated, draftStorageKey, initialShareId, isReadonly]);

  function pushToast(kindValue: ToastKind, message: string) {
    setToast({ kind: kindValue, message });
  }

  function makeUndoSnapshot() {
    setSingleUndoSnapshot({
      games: cloneGames(games),
      creatorName,
    });
  }

  function guardReadonly() {
    if (!isReadonly) return false;
    pushToast("info", "共享页面不可编辑");
    return true;
  }

  function handleReorder(newGames: Array<ShareGame | null>) {
    makeUndoSnapshot();
    setGames(newGames);
    setSpoilerExpandedSet(new Set());
  }

  function updateSlot(index: number, game: ShareGame | null) {
    makeUndoSnapshot();
    setGames((prev) => {
      const next = [...prev];
      next[index] = game;
      return next;
    });

    setSpoilerExpandedSet((prev) => {
      if (!prev.has(index)) return prev;
      const next = new Set(prev);
      next.delete(index);
      return next;
    });
  }

  function handleUndo() {
    if (guardReadonly()) return;
    if (!singleUndoSnapshot) return;
    setGames(singleUndoSnapshot.games);
    setCreatorName(singleUndoSnapshot.creatorName);
    setSingleUndoSnapshot(null);
    setSpoilerExpandedSet(new Set());
    pushToast("success", "已撤销上一步操作");
  }

  function handleClear() {
    if (guardReadonly()) return;
    if (filledCount === 0) return;
    makeUndoSnapshot();
    setGames(createEmptyGames());
    setSpoilerExpandedSet(new Set());
    pushToast("info", `已清空已选${kindMeta.label}`);
  }

  async function handleSearch() {
    const normalizedQuery = normalizeSearchQuery(searchQuery);
    if (!normalizedQuery) {
      setSearchError("请输入关键词");
      return;
    }

    ensureSearchClientCacheHydrated();

    const cacheKey = buildSearchClientCacheKey(kind, normalizedQuery);
    const now = Date.now();
    const lastRequest = lastSearchRequestRef.current;
    if (
      lastRequest &&
      lastRequest.key === cacheKey &&
      now - lastRequest.requestedAt < SEARCH_REQUEST_COOLDOWN_MS
    ) {
      return;
    }

    const cached = searchClientCacheRef.current.get(cacheKey);
    if (cached && cached.expiresAt > now) {
      const response = cached.response;
      setSearchError("");
      setSearchCommittedQuery(normalizedQuery);
      setSearchResults(Array.isArray(response.items) ? response.items : []);
      setSearchMeta({
        noResultQuery: typeof response.noResultQuery === "string" ? response.noResultQuery : null,
      });
      setSearchActiveIndex(response.items.length > 0 ? 0 : -1);
      return;
    }

    if (cached) {
      searchClientCacheRef.current.delete(cacheKey);
      persistSearchClientCache();
    }

    lastSearchRequestRef.current = {
      key: cacheKey,
      requestedAt: now,
    };
    setSearchLoading(true);
    setSearchError("");
    setSearchActiveIndex(-1);
    setSearchCommittedQuery(normalizedQuery);

    try {
      const response = await fetch(
        `/api/subjects/search?q=${encodeURIComponent(normalizedQuery)}&kind=${encodeURIComponent(kind)}`
      );
      const json = (await response.json()) as Partial<SubjectSearchResponse> & {
        ok?: boolean;
        error?: string;
      };

      if (!response.ok || !json?.ok) {
        setSearchError(json?.error || "搜索失败，请稍后再试");
        setSearchResults([]);
        setSearchMeta(createSearchMeta(normalizedQuery));
        return;
      }

      const nextResponse: SubjectSearchResponse = {
        ok: true,
        source:
          json.source === "tmdb"
            ? "tmdb"
            : json.source === "itunes"
              ? "itunes"
              : "bangumi",
        kind,
        items: Array.isArray(json.items) ? json.items : [],
        noResultQuery: typeof json.noResultQuery === "string" ? json.noResultQuery : null,
      };

      searchClientCacheRef.current.set(cacheKey, {
        expiresAt: Date.now() + SEARCH_CLIENT_CACHE_TTL_MS,
        response: nextResponse,
      });
      pruneExpiredSearchClientCache(searchClientCacheRef.current);
      trimSearchClientCache(searchClientCacheRef.current);
      persistSearchClientCache();

      setSearchResults(nextResponse.items);
      setSearchMeta({
        noResultQuery: nextResponse.noResultQuery,
      });
      setSearchActiveIndex(nextResponse.items.length > 0 ? 0 : -1);
    } catch {
      setSearchError("搜索失败，请稍后再试");
      setSearchResults([]);
      setSearchMeta(createSearchMeta(normalizedQuery));
    } finally {
      setSearchLoading(false);
    }
  }

  function openSearch(index: number) {
    if (guardReadonly()) return;
    setSelectedSlot(index);
    window.setTimeout(() => setSearchOpen(true), 0);
  }

  function selectSearchResult(game: ShareGame) {
    if (selectedSlot === null) return;
    const targetSlot = selectedSlot;

    const duplicateIndex = games.findIndex(
      (item, index) => index !== targetSlot && item && String(item.id) === String(game.id)
    );

    if (duplicateIndex >= 0) {
      makeUndoSnapshot();
      setGames((prev) => {
        const next = [...prev];
        const current = next[targetSlot];
        const duplicate = next[duplicateIndex];
        next[targetSlot] = duplicate ? { ...duplicate } : null;
        next[duplicateIndex] = current ? { ...current } : null;
        return next;
      });
      setSpoilerExpandedSet((prev) => {
        if (!prev.has(targetSlot) && !prev.has(duplicateIndex)) return prev;
        const next = new Set(prev);
        next.delete(targetSlot);
        next.delete(duplicateIndex);
        return next;
      });
      setSearchOpen(false);
      setSelectedSlot(null);
      pushToast("success", `已与第 ${duplicateIndex + 1} 格互换`);
      return;
    }

    updateSlot(targetSlot, {
      ...game,
      comment: games[targetSlot]?.comment,
      spoiler: games[targetSlot]?.spoiler,
    });

    setSearchOpen(false);
    setSelectedSlot(null);
    pushToast("success", `已填入第 ${targetSlot + 1} 格`);
  }

  function openComment(index: number) {
    if (guardReadonly()) return;
    const game = games[index];
    if (!game) return;

    setCommentSlot(index);
    setCommentText(game.comment || "");
    setCommentSpoiler(Boolean(game.spoiler));
    setCommentOpen(true);
  }

  function saveComment() {
    if (commentSlot === null) return;
    const game = games[commentSlot];
    if (!game) return;

    updateSlot(commentSlot, {
      ...game,
      comment: commentText.trim().slice(0, 140),
      spoiler: commentSpoiler,
    });

    setCommentOpen(false);
    pushToast("success", "评论已保存");
  }

  async function handleSaveShare() {
    if (guardReadonly()) return;
    if (!allSelected) {
      const confirmed = window.confirm(
        `当前仅选择了 ${filledCount}/9${selectionUnit}${kindMeta.label}，确认继续保存吗？`
      );
      if (!confirmed) return;
    }

    const sharePayloadGames = games.map((game) => {
      if (!game) return null;
      return {
        id: game.id,
        name: game.name,
        localizedName: game.localizedName,
        cover: game.cover,
        releaseYear: game.releaseYear,
        genres: game.genres,
        storeUrls: game.storeUrls,
        comment: game.comment,
        spoiler: game.spoiler,
      };
    });

    setSavingShare(true);
    try {
      const response = await fetch("/api/share", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          kind,
          creatorName: creatorName.trim() || null,
          games: sharePayloadGames,
        }),
      });

      const json = await response.json();
      if (!response.ok || !json?.ok) {
        pushToast("error", json?.error || "分享创建失败");
        return;
      }

      const targetKind = parseSubjectKind(json.kind) ?? kind;
      setShareId(json.shareId);
      pushToast("success", json.deduped ? "分享页面已创建" : "分享页面已创建");
      const target = `/${targetKind}/s/${json.shareId}`;
      clearNavigationFallback();
      navigationFallbackTargetRef.current = target;
      router.replace(target);
      navigationFallbackTimerRef.current = window.setTimeout(() => {
        const fallbackTarget = navigationFallbackTargetRef.current;
        if (!fallbackTarget) return;
        if (window.location.pathname !== fallbackTarget) {
          window.location.assign(fallbackTarget);
        }
        clearNavigationFallback();
      }, SHARE_NAVIGATION_FALLBACK_MS);
    } catch {
      pushToast("error", "分享创建失败，请稍后重试");
    } finally {
      setSavingShare(false);
    }
  }

  function handleNotice(kindValue: ToastKind, message: string) {
    pushToast(kindValue, message);
  }

  function handleToggleSpoiler(index: number) {
    const game = games[index];
    if (!game || !game.spoiler) return;

    if (isReadonly && !spoilerExpandedSet.has(index)) {
      const confirmed = window.confirm("包含剧透内容，确认展开吗？");
      if (!confirmed) return;
    }

    setSpoilerExpandedSet((prev) => {
      const next = new Set(prev);
      if (next.has(index)) {
        next.delete(index);
      } else {
        next.add(index);
      }
      return next;
    });
  }

  function switchKind(nextKind: SubjectKind) {
    if (nextKind === kind) {
      setKindPickerOpen(false);
      return;
    }
    setKindPickerOpen(false);
    router.push(`/${nextKind}`);
  }

  return (
    <main className="min-h-screen bg-background px-4 py-16 text-foreground">
      <div className="mx-auto flex w-full max-w-2xl flex-col items-center gap-4">
        <header className="space-y-3 text-center">
          <div className="inline-flex items-center gap-2 sm:gap-3">
            <h1 className="whitespace-nowrap text-3xl font-bold leading-tight tracking-tight text-foreground sm:text-4xl">
              {shareTitle}
            </h1>
            {!isReadonly ? (
              <button
                type="button"
                onClick={() => setKindPickerOpen(true)}
                className="inline-flex items-center gap-1 rounded-full border border-border bg-card px-2.5 py-1 text-xs font-semibold text-card-foreground transition-colors hover:bg-accent hover:text-accent-foreground sm:px-3 sm:py-1.5 sm:text-sm"
                aria-label="切换填写类型"
              >
                <ChevronsUpDown className="h-3.5 w-3.5 text-muted-foreground" />
                切换
              </button>
            ) : null}
          </div>
          <p className="text-sm text-muted-foreground">{kindMeta.subtitle}</p>
          <button
            type="button"
            className="inline-flex items-center justify-center gap-1 rounded-full border border-sky-200 bg-sky-50 px-4 py-1.5 text-base font-semibold text-sky-700 transition-colors hover:bg-sky-100 dark:border-sky-800 dark:bg-sky-950/50 dark:text-sky-200 dark:hover:bg-sky-900/60"
            onClick={() => router.push(`/trends?kind=${kind}`)}
          >
            大家的构成
            <ChevronRight className="h-4 w-4 text-sky-500 dark:text-sky-300" aria-hidden="true" />
          </button>
          <p className="text-sm text-amber-600 dark:text-amber-400">作品分类现已支持添加影视音乐！</p>
          <SupportButton/>
        </header>

        {toast ? (
          <div className="pointer-events-none fixed -left-[200vw] top-0 opacity-0" aria-live="polite">
            <InlineToast kind={toast.kind} message={toast.message} />
          </div>
        ) : null}

        {isReadonly ? (
          <div className="flex flex-col items-center gap-2">
            <p className="rounded-full border border-amber-200 bg-amber-50 px-4 py-1.5 text-xs font-semibold text-amber-700 dark:border-amber-900 dark:bg-amber-950/50 dark:text-amber-300">
              这是共享页面（只读）
            </p>
            <p className="text-sm text-muted-foreground">创作者: {creatorName.trim() || "匿名"}</p>
            <button
              type="button"
              className="inline-flex items-center justify-center gap-2 rounded-full border border-border bg-card px-5 py-2 text-sm font-bold text-card-foreground transition-colors hover:bg-accent hover:text-accent-foreground"
              onClick={() => router.push(`/${kind}`)}
            >
              前往填写页面
            </button>
          </div>
        ) : (
          <div className="w-full max-w-xl">
            <label className="mb-2 block text-sm font-semibold text-foreground">创作者（推荐填写）</label>
            <Input
              value={creatorName}
              onChange={(event) => setCreatorName(event.target.value.slice(0, 40))}
              placeholder="输入你的昵称"
              className="w-full rounded-xl border-border bg-card px-4 py-2.5 text-sm text-foreground placeholder:text-muted-foreground focus-visible:ring-sky-200 dark:focus-visible:ring-sky-900"
            />
            <p className="mt-1 text-right text-xs text-muted-foreground">{creatorName.length}/40</p>
          </div>
        )}

        {loadingShare ? (
          <p className="text-sm text-muted-foreground">正在加载共享页面...</p>
        ) : (
          <div className="mx-auto w-full rounded-xl border-4 border-background bg-card p-1 shadow-2xl ring-1 ring-border/70 sm:p-4">
            <NineGridBoard
              games={games}
              subjectLabel={kindMeta.label}
              kind={kind}
              readOnly={isReadonly}
              onSelectSlot={openSearch}
              onRemoveSlot={(index) => {
                if (guardReadonly()) return;
                updateSlot(index, null);
              }}
              onOpenComment={openComment}
              onReorder={isReadonly ? undefined : handleReorder}
            />
          </div>
        )}

        {!isReadonly ? (
          <ActionCluster
            filledCount={filledCount}
            remainingUnit={selectionUnit}
            readOnly={isReadonly}
            saving={savingShare}
            canUndo={Boolean(singleUndoSnapshot)}
            canClear={filledCount > 0}
            onUndo={handleUndo}
            onClear={handleClear}
            onSave={handleSaveShare}
          />
        ) : null}

        {isReadonly ? (
          <div className="flex w-full flex-col items-center gap-3">
            <SharePlatformActions
              kind={kind}
              shareId={shareId}
              games={games}
              creatorName={creatorName}
              onNotice={handleNotice}
            />
          </div>
        ) : null}

        <SelectedGamesList
          games={games}
          subjectLabel={kindMeta.label}
          bangumiSearchCat={kindMeta.search.bangumiSearchCat}
          kind={kind}
          readOnly={isReadonly}
          spoilerExpandedSet={spoilerExpandedSet}
          onToggleSpoiler={handleToggleSpoiler}
          onOpenComment={openComment}
        />

        <SiteFooter className="w-full" kind={kind} />
      </div>

      <SearchDialog
        kind={kind}
        subjectLabel={kindMeta.label}
        dialogTitle={kindMeta.searchDialogTitle}
        inputPlaceholder={kindMeta.searchPlaceholder}
        idleHint={kindMeta.searchIdleHint}
        committedQuery={searchCommittedQuery}
        open={searchOpen}
        onOpenChange={(open) => {
          setSearchOpen(open);
          if (!open) {
            setSelectedSlot(null);
          }
        }}
        query={searchQuery}
        onQueryChange={(value) => {
          setSearchQuery(value);
          setSearchError("");
          setSearchActiveIndex(-1);
          if (value.trim().length === 0) {
            setSearchResults([]);
            setSearchCommittedQuery("");
            setSearchMeta(createSearchMeta());
          }
        }}
        loading={searchLoading}
        error={searchError}
        results={searchResults}
        noResultQuery={searchMeta.noResultQuery}
        activeIndex={searchActiveIndex}
        onActiveIndexChange={setSearchActiveIndex}
        onSubmitSearch={handleSearch}
        onPickGame={selectSearchResult}
      />

      <CommentDialog
        open={commentOpen}
        onOpenChange={setCommentOpen}
        value={commentText}
        spoiler={commentSpoiler}
        onChangeValue={setCommentText}
        onChangeSpoiler={setCommentSpoiler}
        onSave={saveComment}
      />

      <Dialog open={kindPickerOpen} onOpenChange={setKindPickerOpen}>
        <DialogContent className="w-[86vw] max-w-[21rem] rounded-2xl p-4 sm:max-w-md sm:p-6">
          <DialogHeader>
            <DialogTitle>切换填写类型</DialogTitle>
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
    </main>
  );
}
