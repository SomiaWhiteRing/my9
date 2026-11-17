import { defineConfig } from "vite";
import { cloudflare } from "@cloudflare/vite-plugin";
import { prepareSite } from "./scripts/site-settings.mjs";

export default defineConfig(({ mode }) => ({
  appType: "custom",
  resolve: { alias: prepareSite(mode, true) },
  define: { __MY9_DEV__: "true" },
  plugins: [cloudflare({
    remoteBindings: false, persistState: { path: ".wrangler/state" },
    config: { define: { __MY9_DEV__: "true" } }
  })],
}));
