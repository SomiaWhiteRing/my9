import { handleBangumiSearchRequest } from "@/lib/bangumi/route";

// 兼容旧接口：统一转发到 Bangumi 搜索
export async function GET(request: Request) {
  return handleBangumiSearchRequest(request, { forcedKind: "game" });
}
