import type { MetadataRoute } from "next";
import { getServerSiteUrl } from "@/lib/site-url";
import { SUBJECT_KIND_ORDER } from "@/lib/subject-kind";

const STATIC_ROUTES = [
  { path: "", changeFrequency: "daily", priority: 1 },
  { path: "/trends", changeFrequency: "weekly", priority: 0.8 },
  { path: "/agreement", changeFrequency: "monthly", priority: 0.3 },
  { path: "/privacy-policy", changeFrequency: "monthly", priority: 0.3 },
  { path: "/commercial-disclosure", changeFrequency: "monthly", priority: 0.3 },
] as const;

export default function sitemap(): MetadataRoute.Sitemap {
  const siteUrl = getServerSiteUrl();
  const lastModified = new Date();

  const staticEntries: MetadataRoute.Sitemap = STATIC_ROUTES.map((route) => ({
    url: `${siteUrl}${route.path}`,
    lastModified,
    changeFrequency: route.changeFrequency,
    priority: route.priority,
  }));

  const kindEntries: MetadataRoute.Sitemap = SUBJECT_KIND_ORDER.map((kind) => ({
    url: `${siteUrl}/${kind}`,
    lastModified,
    changeFrequency: "weekly",
    priority: 0.8,
  }));

  return [...staticEntries, ...kindEntries];
}
