import { getCloudflareContext } from "@opennextjs/cloudflare";
import { gzipSync, gunzipSync } from "node:zlib";
import { CompactSharePayload, normalizeCompactPayload } from "@/lib/share/compact";

type S3Module = typeof import("@aws-sdk/client-s3");

type ColdStorageReadResultLike = {
  arrayBuffer(): Promise<ArrayBuffer>;
};

export type ColdStorageBucketLike = {
  get(key: string): Promise<ColdStorageReadResultLike | null>;
  put(
    key: string,
    value: Uint8Array | ArrayBuffer | string,
    options?: {
      httpMetadata?: {
        contentType?: string;
        contentEncoding?: string;
      };
    }
  ): Promise<unknown>;
};

function readEnv(...names: string[]): string | null {
  for (const name of names) {
    const value = process.env[name];
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }
  return null;
}

const R2_ENDPOINT = readEnv("R2_ENDPOINT");
const R2_BUCKET = readEnv("R2_BUCKET");
const R2_ACCESS_KEY_ID = readEnv("R2_ACCESS_KEY_ID");
const R2_SECRET_ACCESS_KEY = readEnv("R2_SECRET_ACCESS_KEY");
const R2_REGION = readEnv("R2_REGION") ?? "auto";

const S3_FALLBACK_ENABLED = Boolean(
  R2_ENDPOINT && R2_BUCKET && R2_ACCESS_KEY_ID && R2_SECRET_ACCESS_KEY
);

let s3ModulePromise: Promise<S3Module> | null = null;
let s3ClientPromise: Promise<InstanceType<S3Module["S3Client"]> | null> | null = null;

async function getS3Module(): Promise<S3Module> {
  if (!s3ModulePromise) {
    s3ModulePromise = import("@aws-sdk/client-s3");
  }
  return s3ModulePromise;
}

async function getS3Client(): Promise<InstanceType<S3Module["S3Client"]> | null> {
  if (!S3_FALLBACK_ENABLED) {
    return null;
  }

  if (!s3ClientPromise) {
    s3ClientPromise = (async () => {
      const { S3Client } = await getS3Module();
      return new S3Client({
        endpoint: R2_ENDPOINT!,
        region: R2_REGION,
        forcePathStyle: true,
        credentials: {
          accessKeyId: R2_ACCESS_KEY_ID!,
          secretAccessKey: R2_SECRET_ACCESS_KEY!,
        },
      });
    })();
  }

  return s3ClientPromise;
}

async function getCloudflareColdStorageBucket(): Promise<ColdStorageBucketLike | null> {
  try {
    const { env } = await getCloudflareContext({ async: true });
    return env.MY9_COLD_STORAGE ?? null;
  } catch {
    return null;
  }
}

async function bodyToBuffer(body: unknown): Promise<Buffer> {
  if (!body) return Buffer.alloc(0);

  const typedBody = body as {
    transformToByteArray?: () => Promise<Uint8Array>;
    arrayBuffer?: () => Promise<ArrayBuffer>;
    [Symbol.asyncIterator]?: () => AsyncIterator<Uint8Array>;
  };

  if (typeof typedBody.transformToByteArray === "function") {
    const bytes = await typedBody.transformToByteArray();
    return Buffer.from(bytes);
  }

  if (typeof typedBody.arrayBuffer === "function") {
    const buffer = await typedBody.arrayBuffer();
    return Buffer.from(buffer);
  }

  if (typeof typedBody[Symbol.asyncIterator] === "function") {
    const chunks: Buffer[] = [];
    for await (const chunk of typedBody as AsyncIterable<Uint8Array>) {
      chunks.push(Buffer.from(chunk));
    }
    return Buffer.concat(chunks);
  }

  throw new Error("unsupported object body type");
}

async function resolveColdStorageBucket(
  bucket?: ColdStorageBucketLike | null
): Promise<ColdStorageBucketLike | null> {
  if (bucket) {
    return bucket;
  }
  return getCloudflareColdStorageBucket();
}

export async function isColdStorageEnabled(bucket?: ColdStorageBucketLike | null): Promise<boolean> {
  if (await resolveColdStorageBucket(bucket)) {
    return true;
  }
  return Boolean(await getS3Client());
}

export function buildColdObjectKey(shareId: string): string {
  return `shares/v1/${shareId}.json.gz`;
}

export async function putColdSharePayload(
  objectKey: string,
  payload: CompactSharePayload,
  options?: {
    bucket?: ColdStorageBucketLike | null;
  }
): Promise<boolean> {
  const body = gzipSync(Buffer.from(JSON.stringify(payload), "utf8"));
  const bucket = await resolveColdStorageBucket(options?.bucket);

  if (bucket) {
    try {
      await bucket.put(objectKey, body, {
        httpMetadata: {
          contentType: "application/json",
          contentEncoding: "gzip",
        },
      });
      return true;
    } catch {
      return false;
    }
  }

  const client = await getS3Client();
  if (!client || !R2_BUCKET) {
    return false;
  }

  try {
    const { PutObjectCommand } = await getS3Module();
    await client.send(
      new PutObjectCommand({
        Bucket: R2_BUCKET,
        Key: objectKey,
        Body: body,
        ContentType: "application/json",
        ContentEncoding: "gzip",
      })
    );
    return true;
  } catch {
    return false;
  }
}

export async function getColdSharePayload(
  objectKey: string,
  options?: {
    bucket?: ColdStorageBucketLike | null;
  }
): Promise<CompactSharePayload | null> {
  const bucket = await resolveColdStorageBucket(options?.bucket);

  if (bucket) {
    try {
      const response = await bucket.get(objectKey);
      if (!response) {
        return null;
      }
      const body = Buffer.from(await response.arrayBuffer());
      const raw = gunzipSync(body).toString("utf8");
      return normalizeCompactPayload(JSON.parse(raw));
    } catch {
      return null;
    }
  }

  const client = await getS3Client();
  if (!client || !R2_BUCKET) {
    return null;
  }

  try {
    const { GetObjectCommand } = await getS3Module();
    const response = await client.send(
      new GetObjectCommand({
        Bucket: R2_BUCKET,
        Key: objectKey,
      })
    );
    const body = await bodyToBuffer(response.Body);
    const raw = gunzipSync(body).toString("utf8");
    return normalizeCompactPayload(JSON.parse(raw));
  } catch {
    return null;
  }
}
