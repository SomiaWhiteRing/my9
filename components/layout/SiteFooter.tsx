"use client";

import { cn } from "@/lib/utils";

interface SiteFooterProps {
  className?: string;
}

export function SiteFooter({ className }: SiteFooterProps) {
  return (
    <footer
      className={cn(
        "mx-auto w-full max-w-2xl border-t border-slate-500 pt-8 text-center text-xs text-slate-500",
        className
      )}
    >
      <p>
        Powered by{" "}
        <a
          href="https://bangumi.tv/"
          target="_blank"
          rel="noreferrer"
          className="font-semibold text-sky-600 hover:underline"
        >
          Bangumi
        </a>
      </p>
      <p className="mt-2">
        <a
          href="https://weibo.com/6571509464/Phs2X0DIy"
          target="_blank"
          rel="noreferrer"
          className="font-semibold text-sky-600 hover:underline"
        >
          苍旻白轮
        </a>{" "}
        made with Codex
      </p>
      <div className="mt-2 flex flex-wrap items-center justify-center gap-2">
        <span>如果觉得对你有用请点Star →</span>
        <a
          href="https://github.com/SomiaWhiteRing/my9"
          target="_blank"
          rel="noreferrer"
          aria-label="GitHub Stars"
        >
          {/* eslint-disable-next-line @next/next/no-img-element */}
          <img
            src="https://img.shields.io/github/stars/SomiaWhiteRing/my9?style=social&label=GitHub%20Stars"
            alt="GitHub Stars badge"
          />
        </a>
      </div>
      <div className="mt-2 flex items-center justify-center">
        <a href="https://hits.sh/my9.shatranj.space/" target="_blank" rel="noreferrer" aria-label="hitsh">
          {/* eslint-disable-next-line @next/next/no-img-element */}
          <img
            src="https://hits.sh/my9.shatranj.space.svg?style=flat-square&label=visitors"
            alt="hitsh badge"
          />
        </a>
      </div>
    </footer>
  );
}
