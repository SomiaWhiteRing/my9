import { defineCloudflareConfig } from "@opennextjs/cloudflare";
import staticAssetsIncrementalCache from "@opennextjs/cloudflare/overrides/incremental-cache/static-assets-incremental-cache";

const config = defineCloudflareConfig({
  incrementalCache: staticAssetsIncrementalCache,
});

// OpenNext's Cloudflare runtime does not currently support Turbopack server builds.
config.buildCommand = "npm run build:cf";

export default config;
