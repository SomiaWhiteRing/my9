"use client";

import { useState, type ReactNode } from "react";
import { cn } from "@/lib/utils";

export function SelectedGamesSection({
  subjectLabel,
  enableResonance = false,
  children,
}: {
  subjectLabel: string;
  enableResonance?: boolean;
  children: ReactNode;
}) {
  const [showResonance, setShowResonance] = useState(true);

  return (
    <section
      id="selected-games"
      className="group/selection w-full max-w-2xl px-1 sm:px-4"
      data-show-resonance={enableResonance && showResonance}
    >
      <div className="flex items-center justify-between gap-3 border-b border-border pb-3">
        <h2 className="min-w-0 text-lg font-bold text-foreground">选择的{subjectLabel}</h2>
        {enableResonance ? (
          <button
            type="button"
            role="switch"
            aria-checked={showResonance}
            onClick={() => setShowResonance((value) => !value)}
            className="inline-flex min-h-8 shrink-0 items-center gap-2 rounded-md text-xs font-medium text-muted-foreground outline-none focus-visible:ring-2 focus-visible:ring-sky-500 focus-visible:ring-offset-2 focus-visible:ring-offset-background sm:text-sm"
          >
            显示共鸣
            <span
              aria-hidden="true"
              className={cn(
                "inline-flex h-5 w-9 items-center rounded-full transition-colors",
                showResonance ? "bg-sky-600" : "bg-muted-foreground/40"
              )}
            >
              <span
                className={cn(
                  "h-4 w-4 rounded-full bg-white transition-transform",
                  showResonance ? "translate-x-4" : "translate-x-0.5"
                )}
              />
            </span>
          </button>
        ) : null}
      </div>
      {children}
    </section>
  );
}
