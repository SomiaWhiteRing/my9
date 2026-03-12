export type SubjectKind = "game" | "anime" | "tv" | "movie" | "manga" | "lightnovel" | "work" | "song" | "album";

export const DEFAULT_SUBJECT_KIND: SubjectKind = "game";

export const SUBJECT_KIND_ORDER: SubjectKind[] = [
  "game",
  "anime",
  "tv",
  "movie",
  "manga",
  "lightnovel",
  "song",
  "album",
  "work",
];

type KindSearchConfig = {
  typeFilter?: number[];
  strictPlatform?: string;
  bangumiSearchCat?: number;
};

export type SubjectKindMeta = {
  kind: SubjectKind;
  label: string;
  longLabel: string;
  selectionUnit: string;
  subtitle: string;
  searchPlaceholder: string;
  searchDialogTitle: string;
  searchIdleHint: string;
  draftStorageKey: string;
  trendLabel: string;
  search: KindSearchConfig;
};

const KIND_META_MAP: Record<SubjectKind, SubjectKindMeta> = {
  game: {
    kind: "game",
    label: "游戏",
    longLabel: "九部游戏",
    selectionUnit: "部",
    subtitle: "向世界传达你所爱的游戏。",
    searchPlaceholder: "输入游戏名称",
    searchDialogTitle: "搜索游戏",
    searchIdleHint: "输入游戏名称开始搜索",
    draftStorageKey: "my-nine-game:v1",
    trendLabel: "游戏",
    search: {
      typeFilter: [4],
      bangumiSearchCat: 4,
    },
  },
  anime: {
    kind: "anime",
    label: "动画",
    longLabel: "九部动画",
    selectionUnit: "部",
    subtitle: "向世界传达你所爱的动画。",
    searchPlaceholder: "输入动画名称",
    searchDialogTitle: "搜索动画",
    searchIdleHint: "输入动画名称开始搜索",
    draftStorageKey: "my-nine-anime:v1",
    trendLabel: "动画",
    search: {
      typeFilter: [2],
      bangumiSearchCat: 2,
    },
  },
  manga: {
    kind: "manga",
    label: "漫画",
    longLabel: "九部漫画",
    selectionUnit: "部",
    subtitle: "向世界传达你所爱的漫画。",
    searchPlaceholder: "输入漫画名称",
    searchDialogTitle: "搜索漫画",
    searchIdleHint: "输入漫画名称开始搜索",
    draftStorageKey: "my-nine-manga:v1",
    trendLabel: "漫画",
    search: {
      typeFilter: [1],
      strictPlatform: "漫画",
      bangumiSearchCat: 1,
    },
  },
  lightnovel: {
    kind: "lightnovel",
    label: "轻小说",
    longLabel: "九部轻小说",
    selectionUnit: "部",
    subtitle: "向世界传达你所爱的轻小说。",
    searchPlaceholder: "输入轻小说名称",
    searchDialogTitle: "搜索轻小说",
    searchIdleHint: "输入轻小说名称开始搜索",
    draftStorageKey: "my-nine-lightnovel:v1",
    trendLabel: "轻小说",
    search: {
      typeFilter: [1],
      strictPlatform: "小说",
      bangumiSearchCat: 1,
    },
  },
  tv: {
    kind: "tv",
    label: "电视剧",
    longLabel: "九部电视剧",
    selectionUnit: "部",
    subtitle: "向世界传达你所爱的电视剧。",
    searchPlaceholder: "输入电视剧名称",
    searchDialogTitle: "搜索电视剧",
    searchIdleHint: "输入电视剧名称开始搜索",
    draftStorageKey: "my-nine-tv:v1",
    trendLabel: "电视剧",
    search: {},
  },
  movie: {
    kind: "movie",
    label: "电影",
    longLabel: "九部电影",
    selectionUnit: "部",
    subtitle: "向世界传达你所爱的电影。",
    searchPlaceholder: "输入电影名称",
    searchDialogTitle: "搜索电影",
    searchIdleHint: "输入电影名称开始搜索",
    draftStorageKey: "my-nine-movie:v1",
    trendLabel: "电影",
    search: {},
  },
  work: {
    kind: "work",
    label: "作品",
    longLabel: "九部作品",
    selectionUnit: "部",
    subtitle: "向世界传达你所爱的作品。",
    searchPlaceholder: "输入作品名称",
    searchDialogTitle: "搜索作品",
    searchIdleHint: "输入作品名称开始搜索",
    draftStorageKey: "my-nine-work:v1",
    trendLabel: "作品",
    search: {},
  },
  song: {
    kind: "song",
    label: "单曲",
    longLabel: "九首单曲",
    selectionUnit: "首",
    subtitle: "向世界传达你所爱的单曲。",
    searchPlaceholder: "输入单曲/歌曲名称",
    searchDialogTitle: "搜索单曲",
    searchIdleHint: "输入单曲名称开始搜索",
    draftStorageKey: "my-nine-song:v1",
    trendLabel: "单曲",
    search: {},
  },
  album: {
    kind: "album",
    label: "专辑",
    longLabel: "九张专辑",
    selectionUnit: "张",
    subtitle: "向世界传达你所爱的专辑。",
    searchPlaceholder: "输入专辑名称",
    searchDialogTitle: "搜索专辑",
    searchIdleHint: "输入专辑名称开始搜索",
    draftStorageKey: "my-nine-album:v1",
    trendLabel: "专辑",
    search: {},
  },
};

export function getSubjectKindMeta(kind: SubjectKind): SubjectKindMeta {
  return KIND_META_MAP[kind];
}

export function getSubjectKindShareTitle(kind: SubjectKind): string {
  const meta = getSubjectKindMeta(kind);
  return `构成我的九${meta.selectionUnit}${meta.label}`;
}

export function parseSubjectKind(value: string | null | undefined): SubjectKind | null {
  if (!value) return null;
  const normalized = value.trim().toLowerCase();
  if (!normalized) return null;
  if (normalized in KIND_META_MAP) {
    return normalized as SubjectKind;
  }
  return null;
}

export function toSubjectKindOrDefault(value: string | null | undefined): SubjectKind {
  return parseSubjectKind(value) ?? DEFAULT_SUBJECT_KIND;
}
