import { defineCloudflareConfig } from "@opennextjs/cloudflare";

const config = defineCloudflareConfig();

// OpenNext's Cloudflare runtime does not currently support Turbopack server builds.
config.buildCommand = "npm run build:cf";

export default config;
