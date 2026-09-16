import fs from "node:fs";
import path from "node:path";
import { createRequire } from "node:module";

// Backport https://github.com/opennextjs/opennextjs-aws/pull/1189 to the
// pinned core. Keep route URLs as `/`, but read the generated `/index` asset.
// Run from the cf:build wrapper, so clean installs receive the same fix without
// importing filesystem code into the runtime config. Fail closed on upgrades.
export function patchOpenNextIndexCache() {
  const require = createRequire(import.meta.url);
  const cloudflareRequire = createRequire(require.resolve("@opennextjs/cloudflare"));
  const packagePath = path.resolve(path.dirname(cloudflareRequire.resolve("@opennextjs/aws/index.js")), "../package.json");
  const { version } = JSON.parse(fs.readFileSync(packagePath, "utf8"));
  if (version !== "3.9.16") {
    throw new Error(`Review/remove the OpenNext index cache backport for core ${version}`);
  }
  const file = path.join(path.dirname(packagePath), "dist/core/routing/cacheInterceptor.js");
  const replacements = [
    [
      "localizedPath = decodePathParams(localizedPath);",
      'localizedPath = decodePathParams(localizedPath) || "/";\n    const cacheKey = localizedPath === "/" ? "/index" : localizedPath;',
    ],
    ['.includes(localizedPath ?? "/")', '.includes(localizedPath)'],
    ['globalThis.incrementalCache.get(localizedPath ?? "/index")', 'globalThis.incrementalCache.get(cacheKey)'],
    ['hasBeenRevalidated(localizedPath, tags, cachedData)', 'hasBeenRevalidated(cacheKey, tags, cachedData)'],
  ];
  const original = fs.readFileSync(file, "utf8");
  let patched = original;
  for (const [before, after] of replacements) {
    if (patched.split(after).length === 2 && !patched.includes(before)) continue;
    if (patched.split(before).length !== 2) {
      throw new Error(`Unexpected OpenNext cache interceptor source: ${before}`);
    }
    patched = patched.replace(before, after);
  }
  if (patched !== original) fs.writeFileSync(file, patched);
}
