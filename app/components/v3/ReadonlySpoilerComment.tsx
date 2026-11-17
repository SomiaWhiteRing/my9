import { useState } from "react";
import { AlertTriangle } from "lucide-react";

interface ReadonlySpoilerCommentProps {
  comment: string;
}

export function ReadonlySpoilerComment({ comment }: ReadonlySpoilerCommentProps) {
  const [expanded, setExpanded] = useState(false);

  if (expanded) {
    return <p className="whitespace-pre-wrap break-words text-xs text-muted-foreground sm:text-sm">{comment}</p>;
  }

  return (
    <button
      type="button"
      onClick={() => {
        const confirmed = window.confirm("包含剧透内容，确认展开吗？");
        if (!confirmed) return;
        setExpanded(true);
      }}
      className="flex w-full items-start gap-2 rounded-xl border border-amber-200 bg-amber-50 px-2.5 py-2 text-left text-xs text-amber-800 transition hover:bg-amber-100"
    >
      <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0" />
      <span>包含剧透内容，点击确认后展开</span>
    </button>
  );
}
