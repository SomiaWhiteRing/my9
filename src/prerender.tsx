import { writeFile } from "node:fs/promises";
import { staticPage, staticPaths } from "./static-pages";
import { renderDocument } from "./document";
import robots from "@/app/robots";
import sitemap from "@/app/sitemap";

export async function prerender(dir: string) {
  for (const path of staticPaths) {
    const { meta, view, page, ...params } = staticPage(path)!;
    const html = renderDocument(meta, view, page ? { page, ...params } : null);
    await writeFile(`${dir}/${path === "/" ? "index" : path.slice(1)}.html`, html);
  }
  const rules = robots();
  const lines = rules.rules.flatMap(rule => [
    "User-agent: " + rule.userAgent, ...rule.allow.map(p => "Allow: " + p), ...rule.disallow.map(p => "Disallow: " + p)
  ]);
  await writeFile(dir + "/robots.txt", [...lines, "Host: " + rules.host, "Sitemap: " + rules.sitemap, ""].join("\n"));
  const entries = sitemap().map(i => `<url><loc>${i.url}</loc><changefreq>${i.changeFrequency}</changefreq><priority>${i.priority}</priority></url>`);
  await writeFile(dir + "/sitemap.xml", `<?xml version="1.0" encoding="UTF-8"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">${entries.join("")}</urlset>`);
}
