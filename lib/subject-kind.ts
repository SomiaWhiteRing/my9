export type SubjectKind = "game" | "anime" | "manga" | "lightnovel" | "work";

export const DEFAULT_SUBJECT_KIND: SubjectKind = "game";

export const SUBJECT_KIND_ORDER: SubjectKind[] = [
  "game",
  "anime",
  "manga",
  "lightnovel",
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
  subtitle: string;
  searchPlaceholder: string;
  searchDialogTitle: string;
  searchIdleHint: string;
  shareTitle: string;
  draftStorageKey: string;
  trendLabel: string;
  search: KindSearchConfig;
};

const KIND_META_MAP: Record<SubjectKind, SubjectKindMeta> = {
  game: {
    kind: "game",
    label: "游戏",
    longLabel: "九部游戏",
    subtitle: "向世界传达你所爱的游戏。",
    searchPlaceholder: "输入游戏名称",
    searchDialogTitle: "搜索游戏",
    searchIdleHint: "输入游戏名称开始搜索",
    shareTitle: "构成我的九部游戏",
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
    subtitle: "向世界传达你所爱的动画。",
    searchPlaceholder: "输入动画名称",
    searchDialogTitle: "搜索动画",
    searchIdleHint: "输入动画名称开始搜索",
    shareTitle: "构成我的九部动画",
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
    subtitle: "向世界传达你所爱的漫画。",
    searchPlaceholder: "输入漫画名称",
    searchDialogTitle: "搜索漫画",
    searchIdleHint: "输入漫画名称开始搜索",
    shareTitle: "构成我的九部漫画",
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
    subtitle: "向世界传达你所爱的轻小说。",
    searchPlaceholder: "输入轻小说名称",
    searchDialogTitle: "搜索轻小说",
    searchIdleHint: "输入轻小说名称开始搜索",
    shareTitle: "构成我的九部轻小说",
    draftStorageKey: "my-nine-lightnovel:v1",
    trendLabel: "轻小说",
    search: {
      typeFilter: [1],
      strictPlatform: "小说",
      bangumiSearchCat: 1,
    },
  },
  work: {
    kind: "work",
    label: "作品",
    longLabel: "九部作品",
    subtitle: "向世界传达你所爱的作品。",
    searchPlaceholder: "输入作品名称",
    searchDialogTitle: "搜索作品",
    searchIdleHint: "输入作品名称开始搜索",
    shareTitle: "构成我的九部作品",
    draftStorageKey: "my-nine-work:v1",
    trendLabel: "作品",
    search: {},
  },
};

export function getSubjectKindMeta(kind: SubjectKind): SubjectKindMeta {
  return KIND_META_MAP[kind];
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
