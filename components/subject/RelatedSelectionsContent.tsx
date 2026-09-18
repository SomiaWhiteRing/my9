"use client";

import Image from "next/image";
import { DialogDescription, DialogTitle } from "@/components/ui/dialog";
import type { RelatedSelectionsResult } from "@/lib/share/related-selections";
import type { ShareSubject } from "@/lib/share/types";
import { getSubjectKindMeta, type SubjectKind } from "@/lib/subject-kind";
import { toProxiedBangumiImageUrl } from "@/lib/image-proxy";
import { cn } from "@/lib/utils";

export function RelatedSelectionsContent({
  result,
  subjectName,
  subject,
  kind,
  highlightedSubjectIds,
  headingId,
  descriptionId,
  presentation = "inline",
}: {
  result: RelatedSelectionsResult;
  subjectName: string;
  subject?: Pick<ShareSubject, "name" | "localizedName" | "cover" | "releaseYear">;
  kind?: SubjectKind;
  highlightedSubjectIds?: readonly string[];
  headingId: string;
  descriptionId?: string;
  presentation?: "inline" | "dialog";
}) {
  const subjectLabel = kind ? getSubjectKindMeta(kind).label : "作品";
  const Heading = presentation === "dialog" ? DialogTitle : "h4";
  const Description = presentation === "dialog" ? DialogDescription : "p";
  const firstCount = result.items[0]?.count ?? 0;

  return (
    <div className="min-w-0">
      {presentation === "dialog" && subject ? (
        <div className="mb-4 flex min-w-0 items-start gap-3 border-b border-border pb-4 sm:gap-4">
          <div className="aspect-[3/4] w-14 shrink-0 overflow-hidden rounded-lg border border-border bg-muted shadow-sm sm:w-16">
            {subject.cover ? (
              <Image
                src={toProxiedBangumiImageUrl(subject.cover) ?? subject.cover}
                alt={subject.name}
                width={64}
                height={86}
                unoptimized
                className="h-full w-full object-contain"
              />
            ) : (
              <div className="flex h-full items-center justify-center text-[11px] text-muted-foreground">无图</div>
            )}
          </div>
          <div className="min-w-0 flex-1">
            <h3 className="mb-1 break-words text-sm font-bold text-card-foreground sm:mb-2 sm:text-lg">
              {subjectName}
              {subject.releaseYear ? ` (${subject.releaseYear})` : ""}
            </h3>
            {subject.localizedName?.trim() && subject.localizedName.trim() !== subject.name ? (
              <p className="mb-2 break-words text-xs text-muted-foreground sm:text-sm">{subject.name}</p>
            ) : null}
            {Number.isSafeInteger(result.matchedShares) && result.matchedShares > 0 ? (
              <p className="text-xs text-muted-foreground sm:text-sm">
                本{subjectLabel}也成为了
                <span className="font-semibold tabular-nums text-sky-600">{result.matchedShares.toLocaleString("zh-CN")}</span>
                人的构成
              </p>
            ) : null}
          </div>
        </div>
      ) : null}
      <div className="mb-3 flex items-center justify-between gap-3">
        <Heading {...(presentation === "inline" ? { id: headingId } : {})} className="text-sm font-semibold leading-normal tracking-normal text-card-foreground">
          构成他们的还有……
        </Heading>
        <span className="shrink-0 text-[11px] text-muted-foreground">共鸣次数</span>
      </div>
      <Description {...(presentation === "inline" ? { id: descriptionId } : {})} className="mb-3 break-words text-xs leading-relaxed text-muted-foreground">
        被《{subjectName}》所构成的人们，也同时被这些{subjectLabel}所构成。
      </Description>

      {result.items.length ? (
        <ol className="space-y-1">
          {result.items.slice(0, 10).map((item, index) => {
            const percentage = firstCount > 0 ? Math.max(0, Math.min(100, item.count / firstCount * 100)) : 0;
            return (
              <li key={item.subjectId} className="relative isolate flex min-h-10 items-start gap-2 overflow-hidden rounded px-2 py-2.5 text-xs sm:gap-3 sm:px-3 sm:text-sm">
                <span
                  aria-hidden="true"
                  className="pointer-events-none absolute inset-y-0 left-0 -z-10 bg-sky-100/70 dark:bg-sky-900/25"
                  style={{ width: `${percentage}%` }}
                />
                <span className={cn(
                  "w-5 shrink-0 pt-0.5 text-center font-mono text-xs tabular-nums",
                  index < 3 ? "font-semibold text-sky-600 dark:text-sky-400" : "text-muted-foreground"
                )} aria-hidden="true">
                  {index + 1}
                </span>
                <span className="min-w-0 flex-1 break-words leading-relaxed text-card-foreground">
                  {highlightedSubjectIds?.includes(item.subjectId) ? (
                    <span
                      title="也在这份构成中"
                      className="text-sky-600 dark:text-sky-400"
                    >
                      {item.name}
                    </span>
                  ) : item.name}
                </span>
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
  );
}
