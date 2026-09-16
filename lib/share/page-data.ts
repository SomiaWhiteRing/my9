import { isCanonicalShareId, normalizeShareId } from "@/lib/share/id";
import { getShare } from "@/lib/share/storage";
import { parseSubjectKind } from "@/lib/subject-kind";

// Shared by the Next page and the Worker HEAD fast path. Keep Next's thrown
// notFound/redirect signals outside the storage error boundary.
export async function resolveSharePage(rawKind: string, rawShareId: string) {
  const kind = parseSubjectKind(rawKind);
  const shareId = normalizeShareId(rawShareId);
  if (!kind || !shareId) return { type: "not-found" } as const;

  if (!isCanonicalShareId(rawShareId) || rawShareId.trim().toLowerCase() !== shareId) {
    return { type: "redirect", status: 308, location: `/${kind}/s/${shareId}` } as const;
  }

  let share: Awaited<ReturnType<typeof getShare>> = null;
  try {
    share = await getShare(shareId);
  } catch {
    // Preserve the existing client-side retry fallback on storage failure.
  }
  if (!share) return { type: "fallback", kind, shareId } as const;

  const shareKind = parseSubjectKind(share.kind) ?? kind;
  if (shareKind !== kind) {
    return { type: "redirect", status: 307, location: `/${shareKind}/s/${share.shareId}` } as const;
  }
  return { type: "share", kind, shareId, share } as const;
}
