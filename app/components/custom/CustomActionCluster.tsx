"use client";

import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

interface CustomActionClusterProps {
  filledCount: number;
  canUndo: boolean;
  canClear: boolean;
  onUndo: () => void;
  onClear: () => void;
  onGenerate: () => void;
}

function generateButtonLabel(params: { filledCount: number; remainingUnit: string }) {
  const { filledCount, remainingUnit } = params;
  if (filledCount < 9) return `还差 ${9 - filledCount} ${remainingUnit}可生成`;
  return "生成图片";
}

export function CustomActionCluster({
  filledCount,
  canUndo,
  canClear,
  onUndo,
  onClear,
  onGenerate,
}: CustomActionClusterProps) {
  const remainingUnit = "部";

  return (
    <section className="flex w-full flex-col items-center gap-3">
      <div className="inline-flex items-center gap-2 rounded-full border border-border bg-card px-4 py-1.5 text-sm font-semibold text-card-foreground">
        <span>{filledCount} / 9 已选择</span>
        {filledCount < 9 ? (
          <span className="text-xs font-bold text-orange-500">还差{9 - filledCount}{remainingUnit}</span>
        ) : null}
      </div>

      <div className="w-full max-w-[42rem] space-y-3">
        <div className="grid grid-cols-2 gap-3">
          <Button
            type="button"
            variant="outline"
            className="inline-flex w-full items-center justify-center gap-2 whitespace-nowrap rounded-full border border-border bg-card px-4 py-3 text-sm font-bold text-card-foreground transition-colors hover:bg-accent hover:text-accent-foreground disabled:cursor-not-allowed disabled:opacity-40"
            disabled={!canUndo}
            onClick={onUndo}
          >
            撤销
          </Button>
          <Button
            type="button"
            variant="outline"
            className="inline-flex w-full items-center justify-center gap-2 whitespace-nowrap rounded-full border border-border bg-card px-4 py-3 text-sm font-bold text-red-500 transition-colors hover:bg-red-50 disabled:cursor-not-allowed disabled:opacity-40 dark:hover:bg-red-950/40"
            disabled={!canClear}
            onClick={onClear}
          >
            清空
          </Button>
        </div>
        <Button
          type="button"
          className={cn(
            "inline-flex w-full items-center justify-center gap-2 whitespace-nowrap rounded-full bg-sky-600 px-4 py-3 text-sm font-bold text-white shadow-sm shadow-sky-200 transition-all hover:bg-sky-700 disabled:cursor-not-allowed disabled:opacity-45",
            filledCount < 9 && "cursor-not-allowed opacity-45 hover:bg-sky-600"
          )}
          onClick={onGenerate}
        >
          {generateButtonLabel({ filledCount, remainingUnit })}
        </Button>
      </div>
    </section>
  );
}
