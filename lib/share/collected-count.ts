import { SHARE_COUNT_SNAPSHOT } from "@/lib/generated/share-count-snapshot";

const collectedCountFromEnv = process.env.NEXT_PUBLIC_SHARE_COUNT
  ? parseInt(process.env.NEXT_PUBLIC_SHARE_COUNT, 10)
  : null;
const collectedCount = SHARE_COUNT_SNAPSHOT > 0 ? SHARE_COUNT_SNAPSHOT : collectedCountFromEnv;
let collectedCountText: string | undefined;

export function getCollectedCountText() {
  return collectedCountText ??= collectedCount === null ? "..." : collectedCount.toLocaleString("zh-CN");
}
