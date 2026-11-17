import path from "node:path";
import { mkdirSync, writeFileSync } from "node:fs";
import { loadEnv } from "vite";

export function prepareSite(mode, dev = false) {
  const dir = path.resolve(dev ? ".my9/dev" : ".my9");
  mkdirSync(dir, { recursive: true });
  const values = loadEnv(mode, process.cwd(), "NEXT_PUBLIC_");
  const settings = {
    siteUrl: process.env.SITE_URL || process.env.NEXT_PUBLIC_SITE_URL || (mode === "test" ? "https://my9test.shatranj.space" : (dev && values.NEXT_PUBLIC_SITE_URL) || "https://my9.shatranj.space"),
    gaId: dev ? "" : values.NEXT_PUBLIC_GA_ID || "", tally: values.NEXT_PUBLIC_TALLY_FORM_URL || "",
    wechat: values.NEXT_PUBLIC_WECHAT_PAY_QR_URL || "", count: values.NEXT_PUBLIC_SHARE_COUNT || "",
  };
  writeFileSync(path.join(dir, "public-settings.json"), JSON.stringify(settings));
  if (dev) writeFileSync(path.join(dir, "client-assets.json"), JSON.stringify({ entry: "/src/client.tsx", css: ["/app/globals.css"], version: "" }));
  return { "@": process.cwd(), "#public-settings": path.join(dir, "public-settings.json"), "#client-assets": path.join(dir, "client-assets.json") };
}
