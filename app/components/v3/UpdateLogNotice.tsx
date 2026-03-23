"use client";

import type { ReactNode } from "react";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { cn } from "@/lib/utils";

type UpdateLogLine = {
  className: string;
  content: ReactNode;
};

type UpdateLogEntry = {
  id: string;
  publishedAt: string;
  headline: string;
  lines: UpdateLogLine[];
};

const amberPrimaryClassName = "text-amber-600 dark:text-amber-400";
const amberSecondaryClassName = "text-amber-300 dark:text-amber-200";
const yellowPrimaryClassName = "text-yellow-500";

const UPDATE_LOG_ENTRIES: UpdateLogEntry[] = [
  {
    id: "2026-03-21-2359-export-options",
    publishedAt: "2026-03-21 23:59",
    headline: "在导出图片时增加了更多自定义选项！",
    lines: [{ className: amberPrimaryClassName, content: <>在导出图片时增加了更多自定义选项！</> }],
  },
  {
    id: "2026-03-21-0124-outage-fixed",
    publishedAt: "2026-03-21 01:24",
    headline: "3月20日23:55开始的服务器问题已于3月21日01:07修复！",
    lines: [
      {
        className: amberPrimaryClassName,
        content: (
          <>
            3月20日23:55开始的服务器问题已于3月21日01:07修复！
            <span style={{ textDecoration: "line-through" }}>cloudflare的账单系统有毛病……</span>
          </>
        ),
      },
    ],
  },
  {
    id: "2026-03-17-0540-custom-mode",
    publishedAt: "2026-03-17 05:40",
    headline: "自定义模式现已追加！",
    lines: [{ className: amberPrimaryClassName, content: <>自定义模式现已追加！</> }],
  },
  {
    id: "2026-03-13-1913-film-tv",
    publishedAt: "2026-03-13 19:13",
    headline: "作品分类现已支持添加影视剧！",
    lines: [{ className: amberPrimaryClassName, content: <>作品分类现已支持添加影视剧！</> }],
  },
  {
    id: "2026-03-13-1105-role-person",
    publishedAt: "2026-03-13 11:05",
    headline: "现已追加电影/电视剧/单曲/专辑/人物/角色的支持！",
    lines: [{ className: amberPrimaryClassName, content: <>现已追加电影/电视剧/单曲/专辑/人物/角色的支持！</> }],
  },
  {
    id: "2026-03-13-0044-song-album",
    publishedAt: "2026-03-13 00:44",
    headline: "现已追加电影/电视剧/单曲/专辑的支持！",
    lines: [{ className: amberPrimaryClassName, content: <>现已追加电影/电视剧/单曲/专辑的支持！</> }],
  },
  {
    id: "2026-03-12-1530-thanks-miqier",
    publishedAt: "2026-03-12 15:30",
    headline: "感谢 MiQieR 的贡献，现已追加电影/电视剧的支持！",
    lines: [
      {
        className: amberPrimaryClassName,
        content: (
          <>
            感谢{" "}
            <a
              href="https://github.com/MiQieR"
              target="_blank"
              rel="noreferrer"
              className="font-semibold text-sky-600 underline decoration-sky-300 underline-offset-2 hover:text-sky-700 dark:text-sky-400 dark:decoration-sky-500 dark:hover:text-sky-300"
            >
              MiQieR
            </a>{" "}
            的贡献，现已追加电影/电视剧的支持！
          </>
        ),
      },
    ],
  },
  {
    id: "2026-03-11-1744-outage-fixed-exact",
    publishedAt: "2026-03-11 17:44",
    headline: "3月11日16时56分开始的服务器崩溃已于17时38分修复！如果途中遭遇炸服可重新尝试生成。",
    lines: [
      {
        className: yellowPrimaryClassName,
        content: <>3月11日16时56分开始的服务器崩溃已于17时38分修复！如果途中遭遇炸服可重新尝试生成。</>,
      },
    ],
  },
];

const latestUpdateLogEntry = UPDATE_LOG_ENTRIES[0];

export function UpdateLogNotice() {
  if (!latestUpdateLogEntry) return null;

  return (
    <Dialog>
      <DialogTrigger asChild>
        <button
          type="button"
          aria-label={`${latestUpdateLogEntry.headline} 点击查看完整更新日志`}
          className={cn(
            "block w-full rounded-sm bg-transparent p-0 text-center text-sm focus:outline-none focus-visible:ring-2 focus-visible:ring-amber-300 dark:focus-visible:ring-amber-700",
            amberPrimaryClassName
          )}
        >
          {latestUpdateLogEntry.headline}
        </button>
      </DialogTrigger>
      <DialogContent className="w-[calc(100vw-1rem)] max-h-[88dvh] overflow-y-auto rounded-2xl p-4 md:w-[92vw] md:max-h-[85vh] md:max-w-2xl md:p-5">
        <DialogHeader className="text-left">
          <DialogTitle>更新日志</DialogTitle>
        </DialogHeader>
        <div className="space-y-3">
          {UPDATE_LOG_ENTRIES.map((entry) => (
            <article key={entry.id} className="rounded-xl border border-border bg-card p-4 text-left">
              <p className="text-xs font-semibold tracking-wide text-muted-foreground">{entry.publishedAt}</p>
              <div className="mt-2 space-y-2 text-sm leading-6">
                {entry.lines.map((line, index) => (
                  <p key={`${entry.id}-${index}`} className={cn("break-words", line.className)}>
                    {line.content}
                  </p>
                ))}
              </div>
            </article>
          ))}
        </div>
      </DialogContent>
    </Dialog>
  );
}
