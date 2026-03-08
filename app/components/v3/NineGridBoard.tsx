"use client";

import Image from "next/image";
import { MessageSquare, Plus, X } from "lucide-react";
import { ShareGame } from "@/lib/share/types";
import { cn } from "@/lib/utils";

interface NineGridBoardProps {
  games: Array<ShareGame | null>;
  subjectLabel: string;
  readOnly: boolean;
  onSelectSlot: (index: number) => void;
  onRemoveSlot: (index: number) => void;
  onOpenComment: (index: number) => void;
}

function displayTitle(game: ShareGame) {
  return game.localizedName?.trim() || game.name;
}

export function NineGridBoard({
  games,
  subjectLabel,
  readOnly,
  onSelectSlot,
  onRemoveSlot,
  onOpenComment,
}: NineGridBoardProps) {
  return (
    <div className="w-full grid grid-cols-3 gap-2 sm:gap-3">
      {games.map((game, index) => (
        <div key={index} className="relative">
          <div
            role={readOnly ? undefined : "button"}
            tabIndex={readOnly ? undefined : 0}
            aria-label={readOnly ? undefined : `选择第 ${index + 1} 格${subjectLabel}`}
            onClick={() => {
              if (readOnly) return;
              onSelectSlot(index);
            }}
            onKeyDown={(event) => {
              if (readOnly) return;
              if (event.key === "Enter" || event.key === " ") {
                event.preventDefault();
                onSelectSlot(index);
              }
            }}
            className={cn(
              "relative flex aspect-[3/4] w-full items-center justify-center overflow-hidden rounded-lg border border-gray-200 bg-gray-50 transition-colors",
              !readOnly && "cursor-pointer hover:border-sky-200"
            )}
          >
            {game?.cover ? (
              <Image
                src={game.cover}
                alt={displayTitle(game)}
                fill
                unoptimized
                className="absolute inset-0 object-cover"
                sizes="(max-width: 640px) 30vw, (max-width: 1024px) 22vw, 180px"
              />
            ) : (
              <div className="absolute inset-0 flex flex-col items-center justify-center gap-1 text-xs font-medium text-gray-400">
                <Plus className="h-4 w-4" />
                <span>选择</span>
              </div>
            )}

            <div className="absolute left-1.5 top-1 text-[10px] font-semibold text-gray-300">
              {index + 1}
            </div>
          </div>

          {game && !readOnly ? (
            <div className="absolute bottom-2 right-2 flex items-center gap-1.5">
              <button
                type="button"
                aria-label={`编辑第 ${index + 1} 格评论`}
                onClick={(event) => {
                  event.stopPropagation();
                  onOpenComment(index);
                }}
                className="inline-flex h-9 w-9 items-center justify-center rounded-full bg-black/70 text-white transition hover:bg-sky-600 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-white/90"
              >
                <MessageSquare className="h-4 w-4" />
              </button>
              <button
                type="button"
                aria-label={`移除第 ${index + 1} 格游戏`}
                onClick={(event) => {
                  event.stopPropagation();
                  onRemoveSlot(index);
                }}
                className="inline-flex h-9 w-9 items-center justify-center rounded-full bg-black/70 text-white transition hover:bg-rose-600 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-white/90"
              >
                <X className="h-4 w-4" />
              </button>
            </div>
          ) : null}
        </div>
      ))}
    </div>
  );
}
