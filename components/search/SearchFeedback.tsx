"use client";

import { AlertCircle, Loader2, RefreshCw } from "lucide-react";
import { Button } from "@/components/ui/button";

type SearchFeedbackProps = {
  loading: boolean;
  error: string;
  onRetry: () => void;
};

export function SearchFeedback({ loading, error, onRetry }: SearchFeedbackProps) {
  if (loading) {
    return (
      <div className="flex flex-col items-center justify-center py-10 text-muted-foreground" role="status">
        <Loader2 className="mb-2 h-8 w-8 animate-spin" aria-hidden="true" />
        <p>正在搜索...</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex flex-col items-center justify-center py-10 text-center text-red-500" role="alert">
        <AlertCircle className="mb-2 h-8 w-8" aria-hidden="true" />
        <p className="max-w-full break-words">{error}</p>
        <Button variant="outline" className="mt-4" onClick={onRetry}>
          <RefreshCw className="h-4 w-4" aria-hidden="true" />
          重试
        </Button>
      </div>
    );
  }

  return null;
}
