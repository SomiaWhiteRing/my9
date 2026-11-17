import path from "node:path";
import { pathToFileURL } from "node:url";
import { writeFile } from "node:fs/promises";
import { spawnSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { build } from "vite";
import { prepareSite } from "./site-settings.mjs";

const test = process.argv.includes("--test");
const mode = test ? "test" : "production";
const snapshot = spawnSync(process.execPath, ["scripts/write-share-count-snapshot.mjs",
  ...(process.argv.includes("--remote") ? ["--remote"] : []), ...(test ? ["--test"] : [])], { stdio: "inherit" });
if (snapshot.status !== 0) process.exit(snapshot.status ?? 1);

const alias = prepareSite(mode);
const base = { configFile: false, mode, resolve: { alias }, define: { __MY9_DEV__: "false" } };
const client = await build({ ...base, build: { outDir: "dist/client", emptyOutDir: true,
  rollupOptions: { input: "src/client.tsx", onwarn(warning, warn) {
    if (warning.code !== "MODULE_LEVEL_DIRECTIVE") warn(warning);
  } } } });
const entry = client.output.find(file => file.type === "chunk" && file.isEntry);
await writeFile(alias["#client-assets"], JSON.stringify({
  entry: "/" + entry.fileName, css: [...entry.viteMetadata.importedCss].map(file => "/" + file), version: randomUUID()
}));
await build({ ...base, publicDir: false, build: {
  ssr: "src/prerender.tsx", outDir: ".my9/render", emptyOutDir: true,
  rollupOptions: { output: { entryFileNames: "prerender.mjs" } }
} });
const { prerender } = await import(pathToFileURL(path.resolve(".my9/render/prerender.mjs")).href);
await prerender("dist/client");
