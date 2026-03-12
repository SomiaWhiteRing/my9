"use client";

import { useEffect, useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import { SharePlatformActions } from "@/components/share/SharePlatformActions";
import { SiteFooter } from "@/components/layout/SiteFooter";
import { InlineToast, ToastKind } from "@/app/components/v3/InlineToast";
import { NineGridBoard } from "@/app/components/v3/NineGridBoard";
import { SelectedGamesList } from "@/app/components/v3/SelectedGamesList";
import { SubjectKind, getSubjectKindMeta, getSubjectKindShareTitle, parseSubjectKind } from "@/lib/subject-kind";
import { ShareGame } from "@/lib/share/types";

type ToastState = {
  kind: ToastKind;
  message: string;
} | null;

export type InitialReadonlyShareData = {
  shareId: string;
  kind: SubjectKind;
  creatorName: string | null;
  games: Array<ShareGame | null>;
};

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

interface My9ReadonlyAppProps {
  kind: SubjectKind;
  initialShareId: string;
  initialShareData?: InitialReadonlyShareData | null;
}

export default function My9ReadonlyApp({
  kind,
  initialShareId,
  initialShareData = null,
}: My9ReadonlyAppProps) {
  const router = useRouter();
  const kindMeta = useMemo(() => getSubjectKindMeta(kind), [kind]);
  const shareTitle = useMemo(() => getSubjectKindShareTitle(kind), [kind]);
  const [games, setGames] = useState<Array<ShareGame | null>>(() =>
    normalizeGamesForState(initialShareData?.games)
  );
  const [creatorName, setCreatorName] = useState(initialShareData?.creatorName || "");
  const [shareId, setShareId] = useState<string | null>(initialShareData?.shareId || initialShareId);
  const [loadingShare, setLoadingShare] = useState(Boolean(initialShareId) && !initialShareData);
  const [toast, setToast] = useState<ToastState>(null);
  const [spoilerExpandedSet, setSpoilerExpandedSet] = useState<Set<number>>(new Set());

  useEffect(() => {
    if (!toast) return;
    const timer = setTimeout(() => setToast(null), 2800);
    return () => clearTimeout(timer);
  }, [toast]);

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

    const currentShareId = initialShareId;
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
        setGames(normalizeGamesForState(payloadGames));
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

  function handleNotice(kindValue: ToastKind, message: string) {
    setToast({ kind: kindValue, message });
  }

  function handleToggleSpoiler(index: number) {
    const game = games[index];
    if (!game || !game.spoiler) return;

    if (!spoilerExpandedSet.has(index)) {
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

  return (
    <main className="min-h-screen bg-background px-4 py-16 text-foreground">
      <div className="mx-auto flex w-full max-w-2xl flex-col items-center gap-4">
        <header className="space-y-3 text-center">
          <h1 className="whitespace-nowrap text-3xl font-bold leading-tight tracking-tight text-foreground sm:text-4xl">
            {shareTitle}
          </h1>
          <p className="text-sm text-muted-foreground">{kindMeta.subtitle}</p>
          <button
            type="button"
            className="inline-flex items-center justify-center gap-1 rounded-full border border-sky-200 bg-sky-50 px-4 py-1.5 text-xs font-semibold text-sky-700 transition-colors hover:bg-sky-100 dark:border-sky-800 dark:bg-sky-950/50 dark:text-sky-200 dark:hover:bg-sky-900/60"
            onClick={() => router.push(`/trends?kind=${kind}`)}
          >
            大家的构成
            <span className="text-red-500">(New!)</span>
          </button>
        </header>

        {toast ? (
          <div className="pointer-events-none fixed -left-[200vw] top-0 opacity-0" aria-live="polite">
            <InlineToast kind={toast.kind} message={toast.message} />
          </div>
        ) : null}

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

        {loadingShare ? (
          <p className="text-sm text-muted-foreground">正在加载共享页面...</p>
        ) : (
          <div className="mx-auto w-full rounded-xl border-4 border-background bg-card p-1 shadow-2xl ring-1 ring-border/70 sm:p-4">
            <NineGridBoard
              games={games}
              subjectLabel={kindMeta.label}
              readOnly
            />
          </div>
        )}

        <div className="flex w-full flex-col items-center gap-3">
          <SharePlatformActions
            kind={kind}
            shareId={shareId}
            games={games}
            creatorName={creatorName}
            onNotice={handleNotice}
          />
        </div>

        <SelectedGamesList
          games={games}
          subjectLabel={kindMeta.label}
          bangumiSearchCat={kindMeta.search.bangumiSearchCat}
          kind={kind}
          readOnly
          spoilerExpandedSet={spoilerExpandedSet}
          onToggleSpoiler={handleToggleSpoiler}
          onOpenComment={() => undefined}
        />

        <SiteFooter className="w-full" kind={kind} />
      </div>
    </main>
  );
}
