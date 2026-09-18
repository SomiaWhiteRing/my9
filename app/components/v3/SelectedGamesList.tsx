"use client";

import Image from "next/image";
import { AlertTriangle, Globe, MessageCircle } from "lucide-react";
import { ShareGame, type ShareSelectionStats } from "@/lib/share/types";
import type { SubjectKind } from "@/lib/subject-kind";
import { toProxiedBangumiImageUrl } from "@/lib/image-proxy";
import { resolveSubjectLink } from "@/lib/subject-source";
import { ShareSelectionCount, ShareSelectionStatsTimestamp } from "@/app/components/v3/ShareSelectionStats";
import { SelectedGamesSection } from "@/app/components/v3/SelectedGamesSection";
import { SelectedGameCard } from "@/app/components/v3/SelectedGameCard";
import { RelatedSelectionsButton } from "@/components/subject/RelatedSelections";
import type { RelatedSelectionPreviews } from "@/lib/share/related-selections";

interface SelectedGamesListProps {
  shareId?: string | null;
  games: Array<ShareGame | null>;
  selectionStats?: ShareSelectionStats | null;
  relatedSelectionPreviews?: RelatedSelectionPreviews;
  subjectLabel: string;
  bangumiSearchCat?: number;
  kind?: SubjectKind;
  readOnly: boolean;
  spoilerExpandedSet: Set<number>;
  onToggleSpoiler: (index: number) => void;
  onOpenComment: (index: number) => void;
}

function displayName(game: ShareGame): string {
  return game.localizedName?.trim() || game.name;
}

export function SelectedGamesList({
  shareId,
  games,
  selectionStats,
  relatedSelectionPreviews,
  subjectLabel,
  bangumiSearchCat,
  kind,
  readOnly,
  spoilerExpandedSet,
  onToggleSpoiler,
  onOpenComment,
}: SelectedGamesListProps) {
  const selected = games
    .map((game, index) => ({ index, game }))
    .filter((item): item is { index: number; game: ShareGame } => Boolean(item.game));

  return (
    <SelectedGamesSection subjectLabel={subjectLabel} enableResonance={readOnly}>
      <div className="space-y-6">
        {selected.length === 0 ? (
          <p className="py-8 text-center text-sm text-muted-foreground">还没有选择任何{subjectLabel}。</p>
        ) : null}

        {selected.map(({ index, game }) => {
          const spoilerCollapsed = Boolean(game.spoiler) && !spoilerExpandedSet.has(index);
          const subjectLink = resolveSubjectLink({
            kind,
            subject: game,
            bangumiSearchCat,
          });
          return (
            <SelectedGameCard
              key={`${String(game.id)}-${index}`}
              subjectName={displayName(game)}
              subjectId={readOnly ? String(game.id) : undefined}
              kind={kind}
              excludeShareId={readOnly ? shareId ?? undefined : undefined}
              highlightedSubjectIds={readOnly ? selected.filter((item) => item.index !== index).map(({ game }) => String(game.id)) : undefined}
              relatedPreview={readOnly ? relatedSelectionPreviews?.[String(game.id)] : undefined}
            >
              <div className="flex min-w-0 items-start gap-3 sm:gap-4">
                <div className="-ml-1 -mt-1 w-6 flex-shrink-0 text-center font-mono text-xl font-bold text-sky-400 sm:-ml-1.5">
                  {index + 1}
                </div>

                <div className="-ml-0.5 w-14 flex-shrink-0 overflow-hidden rounded-lg border border-border bg-muted shadow-sm sm:-ml-1 sm:w-16">
                  {game.cover ? (
                    <Image
                      src={toProxiedBangumiImageUrl(game.cover) ?? game.cover}
                      alt={game.name}
                      width={64}
                      height={86}
                      unoptimized
                      className="h-auto w-full object-contain"
                    />
                  ) : (
                    <div className="flex aspect-[3/4] items-center justify-center text-[11px] text-muted-foreground">
                      无图
                    </div>
                  )}
                </div>

                <div className="-mt-0.5 min-w-0 flex-1 sm:-mt-1">
                  <h3 className="mb-1 whitespace-normal break-words text-sm font-bold text-card-foreground sm:mb-2 sm:text-lg">
                    {displayName(game)}
                    {game.releaseYear ? ` (${game.releaseYear})` : ""}
                  </h3>
                  {game.localizedName && game.localizedName.trim() !== game.name ? (
                    <p className="-mt-1 mb-2 whitespace-normal break-words text-xs text-muted-foreground sm:text-sm">
                      {game.name}
                    </p>
                  ) : null}

                  {readOnly ? (
                    <ShareSelectionCount subjectLabel={subjectLabel} count={selectionStats?.counts[String(game.id)]} />
                  ) : null}

                  {game.comment ? (
                    <div className="mt-1">
                      {spoilerCollapsed ? (
                        <button
                          type="button"
                          onClick={() => onToggleSpoiler(index)}
                          className="flex w-full items-start gap-2 rounded-xl border border-amber-200 bg-amber-50 px-2.5 py-2 text-left text-xs text-amber-800 transition hover:bg-amber-100"
                        >
                          <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0" />
                          <span>{readOnly ? "包含剧透内容，点击确认后展开" : "剧透评论已折叠，点击展开预览"}</span>
                        </button>
                      ) : (
                        <p className="whitespace-pre-wrap break-words text-xs text-muted-foreground sm:text-sm">
                          {game.comment}
                        </p>
                      )}
                    </div>
                  ) : null}
                </div>

                <div className="-mt-0.5 flex flex-col items-center gap-1 self-start sm:-mt-1">
                  <a
                    href={subjectLink.url}
                    target="_blank"
                    rel="noopener noreferrer"
                    title={`在 ${subjectLink.sourceLabel} 查看`}
                    className="rounded-md border border-border bg-muted p-1.5 text-muted-foreground transition-colors hover:bg-accent hover:text-accent-foreground"
                  >
                    <Globe className="h-4 w-4" />
                  </a>

                  {readOnly && selectionStats?.counts[String(game.id)] !== 0 ? <RelatedSelectionsButton /> : null}
                  {!readOnly ? (
                    <button
                      type="button"
                      onClick={() => onOpenComment(index)}
                      className="rounded-md border border-border bg-muted p-1.5 text-muted-foreground transition-colors hover:bg-accent hover:text-accent-foreground"
                      aria-label={`编辑第 ${index + 1} 格评论`}
                    >
                      <MessageCircle className="h-4 w-4" />
                    </button>
                  ) : null}
                </div>
              </div>
            </SelectedGameCard>
          );
        })}
      </div>
      {readOnly ? <ShareSelectionStatsTimestamp stats={selectionStats} /> : null}
    </SelectedGamesSection>
  );
}
