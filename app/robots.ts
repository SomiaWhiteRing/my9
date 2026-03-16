import type { MetadataRoute } from "next";
import { getServerSiteUrl, getSiteHost } from "@/lib/site-url";

export default function robots(): MetadataRoute.Robots {
  const siteUrl = getServerSiteUrl();
  return {
    rules: [
      {
        userAgent: "*",
        allow: ["/", "/game", "/anime", "/tv", "/movie", "/manga", "/lightnovel", "/work", "/custom"],
        disallow: ["/api/", "/trends", "/*/s/*"],
      },
    ],
    host: getSiteHost(siteUrl),
    sitemap: `${siteUrl}/sitemap.xml`,
  };
}
