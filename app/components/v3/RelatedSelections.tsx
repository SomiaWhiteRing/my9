"use client";

import { createContext, useContext, useEffect, useId, useRef, useState, type ReactNode } from "react";
import { Activity, ArrowUpRight, LoaderCircle } from "lucide-react";
import type { RelatedSelectionPreview, RelatedSelectionsResult } from "@/lib/share/related-selections";
import { getSubjectKindMeta, type SubjectKind } from "@/lib/subject-kind";
import { resolveSubjectLink } from "@/lib/subject-source";
import { cn } from "@/lib/utils";

type RelatedSelectionsControls = {
  panelId: string;
  subjectName: string;
  loading: boolean;
  expanded: boolean;
  toggle: () => void;
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
      onClick={controls.toggle}
      disabled={controls.loading}
      aria-label={label}
      title={label}
      aria-controls={controls.panelId}
      aria-expanded={controls.expanded}
      aria-busy={controls.loading}
      className={cn(
        "inline-flex min-h-[30px] min-w-[30px] items-center justify-center gap-1 rounded-md border p-1.5 transition-colors focus-visible:ring-2 focus-visible:ring-sky-500 disabled:cursor-wait group-data-[show-resonance=false]/selection:hidden",
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
  kind,
  bangumiSearchCat,
  className,
  children,
}: {
  preview?: RelatedSelectionPreview;
  subjectId?: string;
  subjectName: string;
  kind?: SubjectKind;
  bangumiSearchCat?: number;
  className: string;
  children: ReactNode;
}) {
  const panelId = useId();
  const headingId = `${panelId}-heading`;
  const subjectLabel = kind ? getSubjectKindMeta(kind).label : "作品";
  const [status, setStatus] = useState<"idle" | "loading" | "loaded" | "error">("idle");
  const [result, setResult] = useState<RelatedSelectionsResult | null>(null);
  const [error, setError] = useState("");
  const [expanded, setExpanded] = useState(false);
  const pending = useRef<ReturnType<typeof setTimeout> | null>(null);
  const controller = useRef<AbortController | null>(null);

  useEffect(() => () => {
    if (pending.current !== null) clearTimeout(pending.current);
    controller.current?.abort();
  }, []);

  async function toggle() {
    if (pending.current !== null || controller.current) return;
    if (status === "loaded") {
      setExpanded((value) => !value);
      return;
    }

    setStatus("loading");
    setError("");
    if (preview) {
      pending.current = setTimeout(() => {
        pending.current = null;
        setResult(preview.result);
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
      const response = await fetch(`/api/subjects/related?${params}`, { signal: abort.signal });
      const data = await response.json();
      if (!response.ok) throw new Error(data.error || "查询失败，请稍后重试。");
      setResult(data as RelatedSelectionsResult);
      setStatus("loaded");
      setExpanded(true);
    } catch (cause) {
      setStatus("error");
      setError(cause instanceof Error && cause.name !== "AbortError" ? cause.message : "查询超时，请稍后重试。");
      setExpanded(true);
    } finally {
      clearTimeout(timeout);
      controller.current = null;
    }
  }

  return (
    <RelatedSelectionsContext.Provider value={{ panelId, subjectName, loading: status === "loading", expanded, toggle }}>
      <article className={className}>
        {children}
        <span className="sr-only group-data-[show-resonance=false]/selection:hidden" role="status">
          {status === "loading" ? "正在查询共同选择" : result ? `已加载 ${result.items.length} 部作品` : ""}
        </span>
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
            {error ? (
              <div className="mt-4 border-t border-border pt-4 text-xs text-muted-foreground" role="status">
                {error}
                <button type="button" onClick={toggle} disabled={status === "loading"} className="ml-2 text-sky-600 underline">重试</button>
              </div>
            ) : null}
            {result ? (
              <div className="mt-4 border-t border-border pt-4">
                <div className="mb-3 flex items-center justify-between gap-3">
                  <h4 id={headingId} className="text-sm font-semibold text-card-foreground">构成他们的还有……</h4>
                  <span className="shrink-0 text-[11px] text-muted-foreground">共鸣次数</span>
                </div>
                <p className="mb-3 text-xs leading-relaxed text-muted-foreground">
                  被《{subjectName}》所构成的人们，也同时被这些{subjectLabel}所构成。
                </p>

                {result.items.length ? (
                  <ol className="divide-y divide-border/60">
                    {result.items.slice(0, 10).map((item, index) => {
                      const link = resolveSubjectLink({ kind, subject: { id: item.subjectId, name: item.name }, bangumiSearchCat });
                      return (
                        <li key={item.subjectId} className="flex items-start gap-2 py-2.5 text-xs sm:gap-3 sm:text-sm">
                          <span className={cn(
                            "w-5 shrink-0 pt-0.5 text-center font-mono text-xs tabular-nums",
                            index < 3 ? "font-semibold text-sky-600 dark:text-sky-400" : "text-muted-foreground"
                          )} aria-hidden="true">
                            {index + 1}
                          </span>
                          <a
                            href={link.url}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="group min-w-0 flex-1 break-words leading-relaxed text-card-foreground transition-colors hover:text-sky-600 focus-visible:rounded-sm focus-visible:ring-2 focus-visible:ring-sky-500"
                          >
                            {item.name}
                            <ArrowUpRight className="ml-1 inline h-3 w-3 text-muted-foreground/60 group-hover:text-sky-600" aria-hidden="true" />
                            <span className="sr-only">（在 {link.sourceLabel} 新窗口打开）</span>
                          </a>
                          <span className="shrink-0 pt-0.5 font-semibold tabular-nums text-sky-600">
                            {item.count.toLocaleString("zh-CN")}
                          </span>
                        </li>
                      );
                    })}
                  </ol>
                ) : (
                  <p className="rounded-lg bg-muted/60 px-3 py-4 text-xs leading-relaxed text-muted-foreground">
                    暂时还没有足够的共同选择，等更多人留下构成后再来看看。
                  </p>
                )}

                <div className="mt-3 flex flex-wrap items-center justify-between gap-x-3 gap-y-1 text-[11px] leading-relaxed text-muted-foreground">
                  {Number.isSafeInteger(result.kindShares) && result.kindShares >= 0 ? (
                    <span>基于 {result.kindShares.toLocaleString("zh-CN")} 次构成</span>
                  ) : null}
                  <time dateTime={new Date(result.updatedAt).toISOString()}>
                    {new Date(result.updatedAt).toLocaleDateString("zh-CN", { timeZone: "Asia/Shanghai", month: "2-digit", day: "2-digit" })} 更新
                  </time>
                </div>
              </div>
            ) : null}
          </div>
        </div>
      </article>
    </RelatedSelectionsContext.Provider>
  );
}
