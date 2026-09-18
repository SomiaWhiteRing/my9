export type RelatedSelectionItem = {
  subjectId: string;
  name: string;
  count: number;
};

export type RelatedSelectionsResult = {
  items: RelatedSelectionItem[];
  matchedShares: number;
  kindShares: number;
  updatedAt: number;
};

// Explicit preview data keeps the local prototype separate from live statistics.
export type RelatedSelectionPreview = {
  result: RelatedSelectionsResult;
  delayMs: number;
};

export type RelatedSelectionPreviews = Record<string, RelatedSelectionPreview>;
