#!/usr/bin/env node

import { readFile } from "node:fs/promises";
import { resolve } from "node:path";

const DEFAULT_ZONE_NAME = "shatranj.space";
const DEFAULT_TEST_DOMAIN = "my9test.shatranj.space";
const DEFAULT_PRODUCTION_DOMAIN = "my9.shatranj.space";

function loadLocalEnvFiles() {
  const candidates = [".env.local", ".env"];
  for (const file of candidates) {
    try {
      process.loadEnvFile(resolve(process.cwd(), file));
    } catch {
      // ignore missing env file
    }
  }
}

loadLocalEnvFiles();

function readEnv(...names) {
  for (const name of names) {
    const value = process.env[name];
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }
  return null;
}

async function readJsonConfig(path) {
  const raw = await readFile(resolve(process.cwd(), path), "utf8");
  return JSON.parse(raw);
}

async function cfFetch(pathname, token) {
  const response = await fetch(`https://api.cloudflare.com/client/v4${pathname}`, {
    headers: {
      Authorization: `Bearer ${token}`,
    },
  });

  const json = await response.json();
  if (!response.ok || !json.success) {
    const message =
      json?.errors?.map((error) => error.message).filter(Boolean).join("; ") ||
      `${response.status} ${response.statusText}`;
    throw new Error(`${pathname}: ${message}`);
  }

  return json.result;
}

function printCheck(label, status, detail) {
  const prefix = status ? "OK" : "FAIL";
  console.log(`[${prefix}] ${label}: ${detail}`);
}

async function main() {
  const token = readEnv("CLOUDFLARE_API_TOKEN");
  const accountId = readEnv("CLOUDFLARE_ACCOUNT_ID");
  const zoneName = readEnv("CLOUDFLARE_ZONE_NAME") ?? DEFAULT_ZONE_NAME;
  const testDomain = readEnv("CLOUDFLARE_TEST_DOMAIN") ?? DEFAULT_TEST_DOMAIN;
  const productionDomain =
    readEnv("CLOUDFLARE_PRODUCTION_DOMAIN") ?? DEFAULT_PRODUCTION_DOMAIN;

  if (!token || !accountId) {
    throw new Error("CLOUDFLARE_API_TOKEN and CLOUDFLARE_ACCOUNT_ID are required");
  }

  const wranglerConfig = await readJsonConfig("wrangler.jsonc");
  const defaultBucketName = wranglerConfig.r2_buckets?.[0]?.bucket_name ?? null;
  const testBucketName = wranglerConfig.env?.test?.r2_buckets?.[0]?.bucket_name ?? null;
  const testDomainPattern = wranglerConfig.env?.test?.routes?.[0]?.pattern ?? null;
  const productionDomainPattern = wranglerConfig.routes?.[0]?.pattern ?? null;
  const envBucketName = readEnv("R2_BUCKET");
  const testWorkerName = `${wranglerConfig.name}-test`;

  const tokenResult = await cfFetch(`/accounts/${accountId}/tokens/verify`, token);
  printCheck("account token", true, `status=${tokenResult.status}`);

  const subdomainResult = await cfFetch(`/accounts/${accountId}/workers/subdomain`, token);
  printCheck("workers subdomain", true, subdomainResult.subdomain);

  const workersResult = await cfFetch(`/accounts/${accountId}/workers/scripts`, token);
  printCheck("workers read", true, `${workersResult.length} scripts visible`);

  const zonesResult = await cfFetch(`/zones?name=${encodeURIComponent(zoneName)}`, token);
  const zone = zonesResult[0] ?? null;
  if (!zone) {
    throw new Error(`zone not found: ${zoneName}`);
  }
  printCheck("zone access", true, `${zone.name} (${zone.id})`);
  printCheck("zone permissions", true, (zone.permissions ?? []).join(", "));

  const domainsResult = await cfFetch(`/accounts/${accountId}/workers/domains?zone_id=${zone.id}`, token);
  const testDomainBinding = domainsResult.find((domain) => domain.hostname === testDomain) ?? null;
  const productionDomainBinding =
    domainsResult.find((domain) => domain.hostname === productionDomain) ?? null;
  printCheck("workers custom domains read", true, `${domainsResult.length} custom domains visible`);
  printCheck(
    "test domain config",
    Boolean(testDomainPattern),
    testDomainPattern ? `configured in wrangler as ${testDomainPattern}` : "missing from wrangler env.test.routes"
  );
  printCheck(
    "test domain binding",
    Boolean(testDomainBinding),
    testDomainBinding ? `${testDomainBinding.hostname} -> ${testDomainBinding.service}` : `${testDomain} is not attached yet`
  );
  printCheck(
    "production domain config",
    Boolean(productionDomainPattern),
    productionDomainPattern
      ? `configured in wrangler as ${productionDomainPattern}`
      : "missing from wrangler routes"
  );
  printCheck(
    "production domain binding",
    Boolean(productionDomainBinding),
    productionDomainBinding
      ? `${productionDomainBinding.hostname} -> ${productionDomainBinding.service}`
      : `${productionDomain} is not attached yet`
  );
  printCheck(
    "test domain worker match",
    Boolean(testDomainBinding?.service === testWorkerName),
    testDomainBinding ? `${testDomainBinding.service} vs ${testWorkerName}` : `${testWorkerName} not bound`
  );

  const bucketsResult = await cfFetch(`/accounts/${accountId}/r2/buckets`, token);
  const bucketNames = new Set((bucketsResult.buckets ?? []).map((bucket) => bucket.name));
  printCheck("r2 read", true, `${bucketNames.size} buckets visible`);
  printCheck(
    "default wrangler bucket",
    Boolean(defaultBucketName && bucketNames.has(defaultBucketName)),
    defaultBucketName ? defaultBucketName : "missing"
  );
  printCheck(
    "test wrangler bucket",
    Boolean(testBucketName && bucketNames.has(testBucketName)),
    testBucketName ? testBucketName : "missing"
  );
  printCheck(
    "env R2 bucket match",
    Boolean(envBucketName && envBucketName === defaultBucketName),
    envBucketName ? `${envBucketName} vs ${defaultBucketName}` : "R2_BUCKET missing"
  );

  console.log("");
  console.log("Manual follow-up still required:");
  console.log("- Confirm the token policy in the Cloudflare dashboard includes Worker deploy, route/custom domain edit, and secret write.");
  console.log("- Provision Worker secrets separately; this script only verifies read access and custom-domain alignment.");
  if (productionDomainPattern && !productionDomainBinding) {
    console.log(
      "- Production cutover on an existing hostname also requires deleting the current DNS record first, or expanding the token to include Zone DNS Edit so Cloudflare can replace it."
    );
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
