import {
  SUBJECT_KIND_ORDER,
  SubjectKind,
  getSubjectKindMeta,
  parseSubjectKind,
} from "@/lib/subject-kind";

export type FillMode = SubjectKind | "custom";

export type FillModeMeta = {
  mode: FillMode;
  label: string;
  route: string;
  selectionUnit: string;
  pageTitle: string;
  subtitle: string;
};

export const FILL_MODE_ORDER: FillMode[] = [...SUBJECT_KIND_ORDER, "custom"];

const CUSTOM_FILL_MODE_META: FillModeMeta = {
  mode: "custom",
  label: "自定义",
  route: "/custom",
  selectionUnit: "部",
  pageTitle: "自定义模式",
  subtitle: "创建属于你自己的构成。",
};

export function parseFillMode(value: string | null | undefined): FillMode | null {
  if (!value) return null;
  const normalized = value.trim().toLowerCase();
  if (!normalized) return null;
  if (normalized === "custom") return "custom";
  return parseSubjectKind(normalized);
}

export function getFillModeMeta(mode: FillMode): FillModeMeta {
  if (mode === "custom") {
    return CUSTOM_FILL_MODE_META;
  }

  const meta = getSubjectKindMeta(mode);
  return {
    mode,
    label: meta.label,
    route: `/${mode}`,
    selectionUnit: meta.selectionUnit,
    pageTitle: `构成我的${meta.longLabel}`,
    subtitle: meta.subtitle,
  };
}

export function isSubjectKindFillMode(mode: FillMode): mode is SubjectKind {
  return mode !== "custom";
}
