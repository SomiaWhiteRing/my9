"use client";

import Image from "next/image";
import { MessageSquare, Plus, X } from "lucide-react";
import { DragDropProvider } from "@dnd-kit/react";
import { Feedback, AutoScroller, Cursor } from '@dnd-kit/dom';
import { useSortable, isSortable } from "@dnd-kit/react/sortable";
import { arrayMove } from "@dnd-kit/helpers";
import { ShareGame } from "@/lib/share/types";
import { cn } from "@/lib/utils";

interface NineGridBoardProps {
  games: Array<ShareGame | null>;
  subjectLabel: string;
  readOnly?: boolean;
  onSelectSlot?: (index: number) => void;
  onRemoveSlot?: (index: number) => void;
  onOpenComment?: (index: number) => void;
  onReorder?: (games: Array<ShareGame | null>) => void;
}

function displayTitle(game: ShareGame) {
  return game.localizedName?.trim() || game.name;
}

interface SortableSlotProps {
  children: (isDragSource: boolean) => React.ReactNode;
  id: string;
  index: number;
  disabled: boolean;
}

function SortableSlot({ children, id, index, disabled }: SortableSlotProps) {
  const { ref, isDragSource } = useSortable({ id, index, disabled });

  return (
    <div ref={ref} className="relative">
      {children(isDragSource)}
    </div>
  );
}

interface GridCellProps {
  game: ShareGame | null;
  index: number;
  subjectLabel: string;
  readOnly?: boolean;
  isDragSource?: boolean;
  onSelectSlot?: (index: number) => void;
  onRemoveSlot?: (index: number) => void;
  onOpenComment?: (index: number) => void;
}

function GridCell({
  game,
  index,
  subjectLabel,
  readOnly,
  isDragSource,
  onSelectSlot,
  onRemoveSlot,
  onOpenComment,
}: GridCellProps) {
  return (
    <>
      <div
        role={readOnly ? undefined : "button"}
        tabIndex={readOnly ? undefined : 0}
        aria-label={readOnly ? undefined : `选择第 ${index + 1} 格${subjectLabel}`}
        onClick={() => {
          if (readOnly) return;
          onSelectSlot?.(index);
        }}
        onKeyDown={(event) => {
          if (readOnly) return;
          if (event.key === "Enter" || event.key === " ") {
            event.preventDefault();
            onSelectSlot?.(index);
          }
        }}
        className={cn(
          "relative flex aspect-[3/4] w-full items-center justify-center overflow-hidden rounded-lg border border-border bg-muted transition-colors",
          !readOnly && "cursor-pointer hover:border-sky-200",
          isDragSource && "opacity-40 ring-2 ring-sky-400 rounded-lg"
        )}
      >
        {game?.cover ? (
          <Image
            src={game.cover}
            alt={displayTitle(game)}
            fill
            unoptimized
            className="absolute inset-0 object-cover select-none [-webkit-touch-callout:none]"
            sizes="(max-width: 640px) 30vw, (max-width: 1024px) 22vw, 180px"
          />
        ) : (
          <div className="absolute inset-0 flex flex-col items-center justify-center gap-1 text-xs font-medium text-muted-foreground">
            <Plus className="h-4 w-4" />
            <span>选择</span>
          </div>
        )}

        <div className="absolute left-1.5 top-1 text-[10px] font-semibold text-muted-foreground/70">
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
              onOpenComment?.(index);
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
              onRemoveSlot?.(index);
            }}
            className="inline-flex h-9 w-9 items-center justify-center rounded-full bg-black/70 text-white transition hover:bg-rose-600 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-white/90"
          >
            <X className="h-4 w-4" />
          </button>
        </div>
      ) : null}
    </>
  );
}

export function NineGridBoard({
  games,
  subjectLabel,
  readOnly,
  onSelectSlot,
  onRemoveSlot,
  onOpenComment,
  onReorder,
}: NineGridBoardProps) {
  const grid = (
    <div className="w-full grid grid-cols-3 gap-2 sm:gap-3">
      {games.map((game, index) => {
        const id = game ? `subject-${game.id}` : `empty-${index}`;

        if (readOnly) {
          return (
            <div key={id} className="relative">
              <GridCell
                game={game}
                index={index}
                subjectLabel={subjectLabel}
                readOnly
              />
            </div>
          );
        }

        return (
          <SortableSlot
            key={id}
            id={id}
            index={index}
            disabled={!game}
          >
            {(isDragSource) => (
              <GridCell
                game={game}
                index={index}
                subjectLabel={subjectLabel}
                isDragSource={isDragSource}
                onSelectSlot={onSelectSlot}
                onRemoveSlot={onRemoveSlot}
                onOpenComment={onOpenComment}
              />
            )}
          </SortableSlot>
        );
      })}
    </div>
  );

  if (readOnly) return grid;

  return (
    <DragDropProvider
      plugins={[
        Feedback,
        AutoScroller,
        Cursor
      ]}
      onDragEnd={(event) => {
        if (!onReorder) return;
        const { source, canceled } = event.operation;
        if (!source || canceled || !isSortable(source)) return;
        const from = source.initialIndex;
        const to = source.index;
        if (from === to) return;
        onReorder(arrayMove(games, from, to));
      }}
    >
      {grid}
    </DragDropProvider>
  );
}
