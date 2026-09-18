"use client";

import { createContext, useContext, useEffect, useId, useRef, useState, type ReactNode } from "react";
import { Activity, LoaderCircle } from "lucide-react";
import { Dialog, DialogContent, DialogDescription, DialogTitle } from "@/components/ui/dialog";
import { RelatedSelectionsContent } from "@/components/subject/RelatedSelectionsContent";
import type { RelatedSelectionPreview, RelatedSelectionsResult } from "@/lib/share/related-selections";
import type { ShareSubject } from "@/lib/share/types";
import type { SubjectKind } from "@/lib/subject-kind";
import { cn } from "@/lib/utils";

type RelatedSelectionsControls = {
  panelId: string;
  subjectName: string;
  loading: boolean;
  expanded: boolean;
  presentation: "inline" | "dialog";
  toggle: (button?: HTMLButtonElement) => void;
};

const RelatedSelectionsContext = createContext<RelatedSelectionsControls | null>(null);

export function RelatedSelectionsButton() {
  const controls = useContext(RelatedSelectionsContext);
  if (!controls) return null;

  const label = controls.loading
    ? "查询中"
    : `${controls.expanded ? "收起" : "查看"}与《${controls.subjectName}》共同被选择的作品`;

  return (
    <button
      type="button"
      onClick={(event) => controls.toggle(event.currentTarget)}
      disabled={controls.loading}
      aria-label={label}
      title={label}
      aria-controls={controls.panelId}
      aria-expanded={controls.expanded}
      aria-busy={controls.loading}
      aria-haspopup={controls.presentation === "dialog" ? "dialog" : undefined}
      className={cn(
        "inline-flex h-[30px] w-[30px] shrink-0 items-center justify-center rounded-md border p-1.5 transition-colors focus-visible:ring-2 focus-visible:ring-sky-500 disabled:cursor-wait group-data-[show-resonance=false]/selection:hidden",
        controls.expanded || controls.loading
          ? "border-sky-200 bg-sky-50 text-sky-600 dark:border-sky-800 dark:bg-sky-950/60 dark:text-sky-400"
          : "border-border bg-muted text-muted-foreground hover:border-sky-200 hover:bg-sky-50 hover:text-sky-600 dark:hover:border-sky-800 dark:hover:bg-sky-950/60 dark:hover:text-sky-400"
      )}
    >
      {controls.loading ? (
        <LoaderCircle className="h-4 w-4 animate-spin motion-reduce:animate-none" aria-hidden="true" />
      ) : (
        <Activity className="h-4 w-4" aria-hidden="true" />
      )}
    </button>
  );
}

export function RelatedSelectionsCard({
  preview,
  subjectId,
  subjectName,
  subject,
  kind,
  highlightedSubjectIds,
  excludeShareId,
  className,
  children,
  presentation = "inline",
}: {
  preview?: RelatedSelectionPreview;
  subjectId?: string;
  subjectName: string;
  subject?: Pick<ShareSubject, "name" | "localizedName" | "cover" | "releaseYear">;
  kind?: SubjectKind;
  highlightedSubjectIds?: readonly string[];
  excludeShareId?: string;
  className: string;
  children: ReactNode;
  presentation?: "inline" | "dialog";
}) {
  const panelId = useId();
  const headingId = `${panelId}-heading`;
  const descriptionId = `${panelId}-description`;
  const buttonRef = useRef<HTMLButtonElement | null>(null);
  const [status, setStatus] = useState<"idle" | "loading" | "loaded" | "error">("idle");
  const [result, setResult] = useState<RelatedSelectionsResult | null>(null);
  const [error, setError] = useState("");
  const [expanded, setExpanded] = useState(false);
  const pending = useRef<ReturnType<typeof setTimeout> | null>(null);
  const controller = useRef<AbortController | null>(null);

  useEffect(() => () => {
    if (pending.current !== null) clearTimeout(pending.current);
    controller.current?.abort();
    controller.current = null;
  }, []);

  async function toggle(button?: HTMLButtonElement) {
    if (button) buttonRef.current = button;
    if (pending.current !== null || controller.current) return;
    if (status === "loaded") {
      setExpanded((value) => !value);
      return;
    }

    setStatus("loading");
    if (preview) {
      pending.current = setTimeout(() => {
        pending.current = null;
        setResult(preview.result);
        setError("");
        setStatus("loaded");
        setExpanded(true);
      }, preview.delayMs);
      return;
    }
    const abort = new AbortController();
    controller.current = abort;
    const timeout = setTimeout(() => abort.abort(), 15_000);
    try {
      const params = new URLSearchParams({ kind: kind ?? "game", subjectId: subjectId ?? "" });
      if (excludeShareId) params.set("excludeShareId", excludeShareId);
      const response = await fetch(`/api/subjects/related?${params}`, { signal: abort.signal });
      const data = await response.json();
      if (controller.current !== abort) return;
      if (!response.ok) throw new Error(data.error || "查询失败，请稍后重试。");
      setResult(data as RelatedSelectionsResult);
      setError("");
      setStatus("loaded");
      setExpanded(true);
    } catch (cause) {
      if (controller.current !== abort) return;
      setStatus("error");
      setError(cause instanceof Error && cause.name !== "AbortError" ? cause.message : "查询超时，请稍后重试。");
      setExpanded(true);
    } finally {
      clearTimeout(timeout);
      if (controller.current === abort) controller.current = null;
    }
  }

  function closeDialog(open: boolean) {
    setExpanded(open);
    if (!open && status === "loading") {
      if (pending.current !== null) clearTimeout(pending.current);
      pending.current = null;
      controller.current?.abort();
      controller.current = null;
      setStatus(error ? "error" : "idle");
    }
  }

  const errorContent = error ? (
    <div className="text-xs leading-relaxed text-muted-foreground" role="status">
      {error}
      <button type="button" onClick={() => toggle()} disabled={status === "loading"} className="ml-2 inline-flex items-center gap-1 text-sky-600 underline disabled:cursor-wait">
        {status === "loading" ? <LoaderCircle className="h-3 w-3 animate-spin motion-reduce:animate-none" aria-hidden="true" /> : null}
        重试
      </button>
    </div>
  ) : null;

  const resultContent = result ? (
    <RelatedSelectionsContent
      result={result}
      subjectName={subjectName}
      subject={subject}
      kind={kind}
      highlightedSubjectIds={highlightedSubjectIds}
      headingId={headingId}
      descriptionId={descriptionId}
      presentation={presentation}
    />
  ) : null;

  return (
    <RelatedSelectionsContext.Provider value={{ panelId, subjectName, loading: status === "loading", expanded, toggle, presentation }}>
      <article className={className}>
        {children}
        <span className="sr-only group-data-[show-resonance=false]/selection:hidden" role="status">
          {status === "loading" ? "正在查询共同选择" : result ? `已加载 ${result.items.length} 部作品` : ""}
        </span>
        {presentation === "dialog" ? (
          <Dialog open={expanded} onOpenChange={closeDialog}>
            {result || error ? (
              <DialogContent
                id={panelId}
                className="max-h-[85dvh] w-[calc(100%-2rem)] max-w-lg overflow-y-auto rounded-lg px-4 pb-5 pt-12 sm:px-6 sm:pb-6"
                onCloseAutoFocus={(event) => {
                  event.preventDefault();
                  buttonRef.current?.focus();
                }}
              >
                {resultContent}
                {error ? (
                  <div className="space-y-3">
                    <DialogTitle className="text-sm leading-normal tracking-normal">共鸣暂时无法查看</DialogTitle>
                    <DialogDescription className="break-words text-xs">《{subjectName}》</DialogDescription>
                    {errorContent}
                  </div>
                ) : null}
              </DialogContent>
            ) : null}
          </Dialog>
        ) : (
          <div
            id={panelId}
            role="region"
            aria-labelledby={result ? headingId : undefined}
            aria-hidden={!expanded}
            inert={!expanded}
            className={cn(
              "grid transition-[grid-template-rows,opacity] duration-200 ease-out motion-reduce:transition-none group-data-[show-resonance=false]/selection:hidden",
              expanded ? "grid-rows-[1fr] opacity-100" : "grid-rows-[0fr] opacity-0"
            )}
          >
            <div className="min-h-0 overflow-hidden">
              {result || error ? (
                <div className="mt-4 border-t border-border pt-4">
                  {errorContent}
                  {resultContent}
                </div>
              ) : null}
            </div>
          </div>
        )}
      </article>
    </RelatedSelectionsContext.Provider>
  );
}
