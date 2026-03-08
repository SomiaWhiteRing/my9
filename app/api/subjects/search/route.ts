import { handleBangumiSearchRequest } from "@/lib/bangumi/route";

export async function GET(request: Request) {
  return handleBangumiSearchRequest(request);
}
