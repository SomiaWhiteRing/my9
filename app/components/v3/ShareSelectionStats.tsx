import type { ShareSelectionStats } from "@/lib/share/types";

export function ShareSelectionCount({
  subjectLabel,
  count,
}: {
  subjectLabel: string;
  count?: number;
}) {
  if (typeof count !== "number" || !Number.isSafeInteger(count) || count <= 0) return null;

  return (
    <p className="mb-2 text-xs text-muted-foreground group-data-[show-resonance=false]/selection:hidden sm:text-sm">
      本{subjectLabel}也成为了
      <span className="font-semibold tabular-nums text-sky-600">{count.toLocaleString("zh-CN")}</span>
      人的构成
    </p>
  );
}

export function ShareSelectionStatsTimestamp({ stats }: { stats?: ShareSelectionStats | null }) {
  if (!stats?.updatedAt || !Object.values(stats.counts).some((count) => count > 0)) return null;
  const updatedAt = new Date(stats.updatedAt);
  if (!Number.isFinite(updatedAt.getTime())) return null;

  return (
    <p className="mt-3 text-xs text-muted-foreground group-data-[show-resonance=false]/selection:hidden">
      统计更新于{" "}
      <time dateTime={updatedAt.toISOString()}>
        {updatedAt.toLocaleString("zh-CN", {
          timeZone: "Asia/Shanghai",
          year: "numeric",
          month: "2-digit",
          day: "2-digit",
          hour: "2-digit",
          minute: "2-digit",
          hourCycle: "h23",
        })}
      </time>
      （北京时间）
    </p>
  );
}
