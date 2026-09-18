import type { ReactNode } from "react";
import { RelatedSelectionsCard } from "@/components/subject/RelatedSelections";
import type { RelatedSelectionPreview } from "@/lib/share/related-selections";
import type { SubjectKind } from "@/lib/subject-kind";

export function SelectedGameCard({
  relatedPreview,
  children,
  ...props
}: {
  relatedPreview?: RelatedSelectionPreview;
  subjectId?: string;
  subjectName: string;
  kind?: SubjectKind;
  children: ReactNode;
}) {
  const className = "rounded-2xl border border-border bg-card p-5 transition-shadow hover:shadow-md";
  if (!relatedPreview && !props.subjectId) return <article className={className}>{children}</article>;

  return (
    <RelatedSelectionsCard {...props} preview={relatedPreview} className={className}>
      {children}
    </RelatedSelectionsCard>
  );
}
