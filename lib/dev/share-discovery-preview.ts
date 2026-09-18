import { SUBJECT_KIND_ORDER, getSubjectKindMeta, type SubjectKind } from "@/lib/subject-kind";
import { SHARE_KIND_COLORS, type ShareDiscoveryItem } from "@/lib/share/discovery";
import type { ShareGame } from "@/lib/share/types";
import type { RelatedSelectionPreviews } from "@/lib/share/related-selections";

export type DiscoveryPreviewState = "normal" | "empty" | "error";

const CREATORS = [
  "阿菜", "今天星期八", "夏天的最后一集", "负电荷", "幻化成风", "枫桥云梦海",
  null, "屿月", "某叶", "想把所有喜欢的故事都好好收藏起来的人", "森森明",
  "阿菜", "破隐出云", "养生从入门到入土", null, "豹", "果子大王WWW", "山间来信", "阿菜", "小满",
];
const VIEW_COUNTS = [48980, 10116, 4649, 2245, 2078, 1643, 1383, 1342, 1317, 1252, 1193, 1133, 967, 920, 902, 880, 822, 756, 708, 671];
const BASE_DATE = Date.parse("2026-09-17T04:00:00Z");

export const DISCOVERY_PREVIEW_ITEMS: ShareDiscoveryItem[] = [
  ...CREATORS.map((creatorName, index) => ({
    shareId: `find-${String(index + 1).padStart(2, "0")}`,
    kind: SUBJECT_KIND_ORDER[index % SUBJECT_KIND_ORDER.length],
    creatorName,
    createdAt: BASE_DATE - index * 3 * 86400000,
    viewCount: VIEW_COUNTS[index],
  })),
  ...Array.from({ length: 23 }, (_, index) => ({
    shareId: `find-${index + 21}`,
    kind: SUBJECT_KIND_ORDER[index % SUBJECT_KIND_ORDER.length],
    creatorName: "阿菜",
    createdAt: BASE_DATE - (index + 1) * 86400000,
    viewCount: Math.max(0, 240 - index * 12),
  })),
];

const PREVIEW_TITLES: Record<SubjectKind, string[]> = {
  game: ["我的世界", "星露谷物语", "塞尔达传说 旷野之息"],
  anime: ["葬送的芙莉莲", "乒乓", "紫罗兰永恒花园"],
  tv: ["漫长的季节", "请回答1988", "重启人生"],
  movie: ["千与千寻", "海上钢琴师", "星际穿越"],
  manga: ["灌篮高手", "钢之炼金术师", "蓝色巨星"],
  lightnovel: ["狼与香辛料", "奇诺之旅", "龙与虎"],
  song: ["晴天", "夜空中最亮的星", "Lemon"],
  album: ["范特西", "叶惠美", "寓言"],
  work: ["小王子", "千与千寻", "我的世界"],
  character: ["芙莉莲", "牧濑红莉栖", "御坂美琴"],
  person: ["宫崎骏", "坂本龙一", "王家卫"],
};

export function createDiscoveryPreviewShare(item: ShareDiscoveryItem) {
  const meta = getSubjectKindMeta(item.kind);
  const games: ShareGame[] = Array.from({ length: 9 }, (_, index) => {
    const name = PREVIEW_TITLES[item.kind][index] ?? `${meta.label}示例 ${index + 1}`;
    const lines = [name.slice(0, 7), name.slice(7)];
    const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="240" height="320" viewBox="0 0 240 320"><rect width="240" height="320" fill="${SHARE_KIND_COLORS[item.kind]}"/><text x="20" y="40" fill="white" font-family="sans-serif" font-size="14">MY9 / ${index + 1}</text><text x="20" y="224" fill="white" font-family="sans-serif" font-size="23" font-weight="600">${lines[0]}</text><text x="20" y="258" fill="white" font-family="sans-serif" font-size="23" font-weight="600">${lines[1]}</text></svg>`;
    return {
      id: `discovery-${item.kind}-${index + 1}`,
      name,
      cover: `data:image/svg+xml;charset=utf-8,${encodeURIComponent(svg)}`,
      comment: index < 3 ? "有些喜欢，会在很久以后依然记得。" : undefined,
    };
  });
  const relatedSelectionPreviews: RelatedSelectionPreviews = Object.fromEntries(
    games.map((game) => [String(game.id), {
      delayMs: 500,
      result: { items: [], matchedShares: 0, kindShares: 1000, updatedAt: BASE_DATE },
    }]),
  );
  return { games, relatedSelectionPreviews };
}
