import type { ReactNode } from "react";
import { renderToString } from "react-dom/server";
import assets from "#client-assets";
import settings from "#public-settings";
import { SYSTEM_THEME_INIT_SCRIPT } from "@/app/layout";
import type { createPageMetadata } from "@/lib/page-metadata";

const escape = (text: string) => text.replaceAll("&", "&amp;").replaceAll('"', "&quot;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");
const scriptData = (value: unknown) => JSON.stringify(value).replaceAll("<", "\\u003c");

export function renderDocument(meta: ReturnType<typeof createPageMetadata>, view: ReactNode, state: unknown) {
  const title = escape(meta.title);
  const desc = escape(meta.description);
  const canonical = escape(new URL(meta.path, settings.siteUrl).href);
  const ga = settings.gaId;
  const analytics = ga ? `<script async src="https://www.googletagmanager.com/gtag/js?id=${encodeURIComponent(ga)}"></script><script>window.dataLayer=window.dataLayer||[];function gtag(){dataLayer.push(arguments)}gtag('js',new Date());gtag('config',${scriptData(ga)})</script>` : "";
  return `<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>${title}</title><meta name="description" content="${desc}"><link rel="canonical" href="${canonical}"><meta property="og:type" content="website"><meta property="og:locale" content="zh_CN"><meta property="og:site_name" content="构成我的九部作品"><meta property="og:title" content="${title}"><meta property="og:description" content="${desc}"><meta property="og:url" content="${canonical}"><meta name="twitter:card" content="summary_large_image"><meta name="twitter:title" content="${title}"><meta name="twitter:description" content="${desc}"><meta name="google-site-verification" content="swtOMxSQC6Dfn-w4YtMQ3OFH4SZz00Blcd6FI0qMgJc"><script>${SYSTEM_THEME_INIT_SCRIPT}</script>${analytics}${assets.css.map(href => `<link rel="stylesheet" href="${escape(href)}">`).join("")}</head><body><div id="app">${view ? renderToString(view) : ""}</div>${state ? `<script type="application/json" id="my9-data">${scriptData(state)}</script><script type="module" src="${escape(assets.entry)}"></script>` : ""}</body></html>`;
}
