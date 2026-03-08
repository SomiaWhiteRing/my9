"use client";

import { BookOpen, BookText, Clapperboard, Gamepad2, Shapes } from "lucide-react";
import { SubjectKind } from "@/lib/subject-kind";

interface SubjectKindIconProps {
  kind: SubjectKind;
  className?: string;
}

export function SubjectKindIcon({ kind, className }: SubjectKindIconProps) {
  switch (kind) {
    case "game":
      return <Gamepad2 className={className} />;
    case "anime":
      return <Clapperboard className={className} />;
    case "manga":
      return <BookOpen className={className} />;
    case "lightnovel":
      return <BookText className={className} />;
    case "work":
    default:
      return <Shapes className={className} />;
  }
}
