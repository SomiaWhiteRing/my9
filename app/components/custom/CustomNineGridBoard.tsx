"use client";

import Image from "next/image";
import type React from "react";
import { MessageSquare, Plus, X } from "lucide-react";
import { DragDropProvider } from "@dnd-kit/react";
import { Feedback, AutoScroller, Cursor } from "@dnd-kit/dom";
import { useSortable, isSortable } from "@dnd-kit/react/sortable";
import { arrayMove } from "@dnd-kit/helpers";
import { CustomEntry, getCustomEntryDisplayTitle } from "@/lib/custom/types";
import { cn } from "@/lib/utils";

interface CustomNineGridBoardProps {
  entries: Array<CustomEntry | null>;
  onSelectSlot: (index: number) => void;
  onRemoveSlot: (index: number) => void;
  onOpenComment: (index: number) => void;
  onReorder: (entries: Array<CustomEntry | null>) => void;
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
  entry: CustomEntry | null;
  index: number;
  isDragSource?: boolean;
  onSelectSlot: (index: number) => void;
  onRemoveSlot: (index: number) => void;
  onOpenComment: (index: number) => void;
}

function GridCell({
  entry,
  index,
  isDragSource,
  onSelectSlot,
  onRemoveSlot,
  onOpenComment,
}: GridCellProps) {
  return (
    <>
      <div
        role="button"
        tabIndex={0}
        aria-label={`选择第 ${index + 1} 格作品`}
        onClick={() => onSelectSlot(index)}
        onKeyDown={(event) => {
          if (event.key === "Enter" || event.key === " ") {
            event.preventDefault();
            onSelectSlot(index);
          }
        }}
        className={cn(
          "relative flex aspect-[3/4] w-full items-center justify-center overflow-hidden rounded-lg border border-border bg-muted transition-colors hover:border-sky-200",
          isDragSource && "rounded-lg opacity-40 ring-2 ring-sky-400"
        )}
      >
        {entry?.cover ? (
          <Image
            src={entry.cover}
            alt={getCustomEntryDisplayTitle(entry)}
            fill
            unoptimized
            className="absolute inset-0 select-none object-cover [-webkit-touch-callout:none]"
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

      {entry ? (
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
            aria-label={`移除第 ${index + 1} 格作品`}
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
    </>
  );
}

export function CustomNineGridBoard({
  entries,
  onSelectSlot,
  onRemoveSlot,
  onOpenComment,
  onReorder,
}: CustomNineGridBoardProps) {
  const grid = (
    <div className="grid w-full grid-cols-3 gap-2 sm:gap-3">
      {entries.map((entry, index) => {
        const id = entry ? `custom-${entry.id}` : `empty-${index}`;
        return (
          <SortableSlot key={id} id={id} index={index} disabled={!entry}>
            {(isDragSource) => (
              <GridCell
                entry={entry}
                index={index}
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

  return (
    <DragDropProvider
      plugins={[Feedback, AutoScroller, Cursor]}
      onDragEnd={(event) => {
        const { source, canceled } = event.operation;
        if (!source || canceled || !isSortable(source)) return;
        const from = source.initialIndex;
        const to = source.index;
        if (from === to) return;
        onReorder(arrayMove(entries, from, to));
      }}
    >
      {grid}
    </DragDropProvider>
  );
}
