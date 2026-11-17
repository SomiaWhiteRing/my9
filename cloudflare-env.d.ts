interface CloudflareEnv {
  MY9_DB: import("./lib/share/storage-d1-runtime").D1DatabaseLike;
  MY9_SHARE_VIEW_ANALYTICS: NonNullable<Parameters<typeof import("./lib/share/view-stats").trackShareViewRequest>[1]>;
}
declare module "cloudflare:workers" { export const env: CloudflareEnv; }
declare const __MY9_DEV__: boolean;
declare module "#client-assets" {
  const assets: { entry: string; css: string[]; version: string };
  export default assets;
}
declare module "#public-settings" {
  const settings: { siteUrl: string; gaId: string; tally: string; wechat: string; count: string };
  export default settings;
}
