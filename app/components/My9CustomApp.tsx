"use client";

import { useEffect, useRef, useState } from "react";
import { ChevronRight, ChevronsUpDown } from "lucide-react";
import { Input } from "@/components/ui/input";
import { SiteFooter } from "@/components/layout/SiteFooter";
import { SubjectKindIcon } from "@/components/subject/SubjectKindIcon";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { UpdateLogNotice } from "@/app/components/v3/UpdateLogNotice";
import { SupportButton } from "@/components/SupportButton";
import { InlineToast, type ToastKind } from "@/app/components/v3/InlineToast";
import { CustomActionCluster } from "@/app/components/custom/CustomActionCluster";
import { CustomImageCropDialog } from "@/app/components/custom/CustomImageCropDialog";
import { CustomImagePreviewDialog } from "@/app/components/custom/CustomImagePreviewDialog";
import { CustomLocalNoticeDialog } from "@/app/components/custom/CustomLocalNoticeDialog";
import { CustomNineGridBoard } from "@/app/components/custom/CustomNineGridBoard";
import { CustomSearchDialog, type CustomSearchTab } from "@/app/components/custom/CustomSearchDialog";
import { CustomSelectedEntriesList } from "@/app/components/custom/CustomSelectedEntriesList";
import {
  type CustomDraftSnapshot,
  type CustomEntry,
  type CustomPendingSelection,
  type CustomSearchItem,
  cloneCustomEntries,
  createEmptyCustomEntries,
  getCustomSearchItemDefaultTitle,
  normalizeCustomEntries,
} from "@/lib/custom/types";
import { readCustomDraftSnapshot, writeCustomDraftSnapshot } from "@/lib/custom/storage";
import { FILL_MODE_ORDER, type FillMode, getFillModeMeta } from "@/lib/fill-mode";
import { cn } from "@/lib/utils";
import { downloadBlob, generateLocalTestImageBlob } from "@/utils/image/exportShareImage";
import { useRouter } from "next/navigation";

type ToastState = {
  kind: ToastKind;
  message: string;
} | null;

type DraftSnapshot = {
  entries: Array<CustomEntry | null>;
  creatorName: string;
};

const CREATOR_STORAGE_KEY = "my-nine-creator:v1";
const CUSTOM_LOCAL_NOTICE_SEEN_KEY = "my9-custom-local-notice:v1";

async function copyText(value: string) {
  if (navigator.clipboard?.writeText) {
    await navigator.clipboard.writeText(value);
    return;
  }

  const textarea = document.createElement("textarea");
  textarea.value = value;
  textarea.style.position = "fixed";
  textarea.style.left = "-9999px";
  document.body.appendChild(textarea);
  textarea.focus();
  textarea.select();
  document.execCommand("copy");
  document.body.removeChild(textarea);
}

export default function My9CustomApp() {
  const router = useRouter();

  const [entries, setEntries] = useState<Array<CustomEntry | null>>(createEmptyCustomEntries);
  const [creatorName, setCreatorName] = useState("");
  const [draftHydrated, setDraftHydrated] = useState(false);
  const [kindPickerOpen, setKindPickerOpen] = useState(false);

  const [toast, setToast] = useState<ToastState>(null);
  const [singleUndoSnapshot, setSingleUndoSnapshot] = useState<DraftSnapshot | null>(null);
  const [spoilerExpandedSet, setSpoilerExpandedSet] = useState<Set<number>>(new Set());

  const [selectedSlot, setSelectedSlot] = useState<number | null>(null);
  const [searchOpen, setSearchOpen] = useState(false);
  const [searchTab, setSearchTab] = useState<CustomSearchTab>("bangumi");
  const [searchQuery, setSearchQuery] = useState("");
  const [searchCommittedQuery, setSearchCommittedQuery] = useState("");
  const [searchLoading, setSearchLoading] = useState(false);
  const [searchError, setSearchError] = useState("");
  const [searchResults, setSearchResults] = useState<CustomSearchItem[]>([]);
  const [searchNoResultQuery, setSearchNoResultQuery] = useState<string | null>(null);
  const [pendingSelection, setPendingSelection] = useState<CustomPendingSelection | null>(null);
  const [pendingTitle, setPendingTitle] = useState("");

  const [cropOpen, setCropOpen] = useState(false);
  const [cropImageSrc, setCropImageSrc] = useState<string | null>(null);
  const [cropFileName, setCropFileName] = useState("");
  const cropObjectUrlRef = useRef<string | null>(null);

  const [previewOpen, setPreviewOpen] = useState(false);

  const [localNoticeOpen, setLocalNoticeOpen] = useState(false);
  const [localNoticeStatus, setLocalNoticeStatus] = useState<ToastState>(null);
  const [testingDownload, setTestingDownload] = useState(false);

  const filledCount = entries.filter((item) => item !== null).length;

  useEffect(() => {
    if (!toast) return;
    const timer = window.setTimeout(() => setToast(null), 2800);
    return () => window.clearTimeout(timer);
  }, [toast]);

  useEffect(() => {
    let active = true;

    async function hydrateDraft() {
      const [draftSnapshot] = await Promise.all([
        readCustomDraftSnapshot(),
      ]);

      if (!active) return;

      try {
        const creatorRaw = localStorage.getItem(CREATOR_STORAGE_KEY);
        if (typeof creatorRaw === "string") {
          setCreatorName(creatorRaw);
        }

        if (!localStorage.getItem(CUSTOM_LOCAL_NOTICE_SEEN_KEY)) {
          setLocalNoticeOpen(true);
        }
      } catch {
        // ignore localStorage failures
      }

      if (draftSnapshot?.entries) {
        setEntries(normalizeCustomEntries(draftSnapshot.entries));
      }

      setDraftHydrated(true);
    }

    hydrateDraft();
    return () => {
      active = false;
      if (cropObjectUrlRef.current) {
        URL.revokeObjectURL(cropObjectUrlRef.current);
        cropObjectUrlRef.current = null;
      }
    };
  }, []);

  useEffect(() => {
    if (!draftHydrated) return;
    const snapshot: CustomDraftSnapshot = {
      entries,
    };
    void writeCustomDraftSnapshot(snapshot);
    try {
      localStorage.setItem(CREATOR_STORAGE_KEY, creatorName);
    } catch {
      // ignore write failures
    }
  }, [creatorName, draftHydrated, entries]);

  function pushToast(kind: ToastKind, message: string) {
    setToast({ kind, message });
  }

  function makeUndoSnapshot() {
    setSingleUndoSnapshot({
      entries: cloneCustomEntries(entries),
      creatorName,
    });
  }

  function handleReorder(nextEntries: Array<CustomEntry | null>) {
    makeUndoSnapshot();
    setEntries(nextEntries);
    setSpoilerExpandedSet(new Set());
  }

  function updateSlot(index: number, entry: CustomEntry | null) {
    makeUndoSnapshot();
    setEntries((prev) => {
      const next = [...prev];
      next[index] = entry;
      return next;
    });
    setSpoilerExpandedSet((prev) => {
      if (!prev.has(index)) return prev;
      const next = new Set(prev);
      next.delete(index);
      return next;
    });
  }

  function handleUndo() {
    if (!singleUndoSnapshot) return;
    setEntries(singleUndoSnapshot.entries);
    setCreatorName(singleUndoSnapshot.creatorName);
    setSingleUndoSnapshot(null);
    setSpoilerExpandedSet(new Set());
    pushToast("success", "已撤销上一步操作");
  }

  function handleClear() {
    if (filledCount === 0) return;
    makeUndoSnapshot();
    setEntries(createEmptyCustomEntries());
    setSpoilerExpandedSet(new Set());
    pushToast("info", "已清空当前九宫格");
  }

  function resetSearchState() {
    setSearchQuery("");
    setSearchCommittedQuery("");
    setSearchError("");
    setSearchResults([]);
    setSearchNoResultQuery(null);
    setPendingSelection(null);
    setPendingTitle("");
  }

  function buildPendingSelectionFromEntry(entry: CustomEntry): CustomPendingSelection {
    return {
      id: entry.id,
      fallbackName: entry.fallbackName,
      cover: entry.cover,
      coverMode: entry.coverMode,
      source: entry.source,
      sourceLabel: entry.sourceLabel,
      externalUrl: entry.externalUrl,
      releaseYear: entry.releaseYear,
    };
  }

  function openSearch(index: number) {
    const entry = entries[index];
    setSelectedSlot(index);
    setSearchQuery("");
    setSearchCommittedQuery("");
    setSearchError("");
    setSearchResults([]);
    setSearchNoResultQuery(null);
    if (entry) {
      setSearchTab(entry.source === "upload" ? "upload" : entry.source);
      setPendingSelection(buildPendingSelectionFromEntry(entry));
      setPendingTitle(entry.title);
    } else {
      setSearchTab("bangumi");
      setPendingSelection(null);
      setPendingTitle("");
    }
    setSearchOpen(true);
  }

  function handleSearchOpenChange(open: boolean) {
    setSearchOpen(open);
    if (!open) {
      setSelectedSlot(null);
      resetSearchState();
      setSearchTab("bangumi");
    }
  }

  async function handleSearchSubmit() {
    const trimmedQuery = searchQuery.trim();
    if (!trimmedQuery || searchTab === "upload") {
      return;
    }

    setSearchLoading(true);
    setSearchError("");
    setSearchCommittedQuery(trimmedQuery);

    try {
      const response = await fetch(
        `/api/custom/search?q=${encodeURIComponent(trimmedQuery)}&source=${encodeURIComponent(searchTab)}`
      );
      const json = (await response.json()) as {
        ok?: boolean;
        error?: string;
        items?: CustomSearchItem[];
        noResultQuery?: string | null;
      };

      if (!response.ok || !json.ok) {
        setSearchError(json.error || "搜索失败，请稍后重试");
        setSearchResults([]);
        setSearchNoResultQuery(typeof json.noResultQuery === "string" ? json.noResultQuery : trimmedQuery);
        return;
      }

      setSearchResults(Array.isArray(json.items) ? json.items : []);
      setSearchNoResultQuery(typeof json.noResultQuery === "string" ? json.noResultQuery : null);
    } catch {
      setSearchError("搜索失败，请稍后重试");
      setSearchResults([]);
      setSearchNoResultQuery(trimmedQuery);
    } finally {
      setSearchLoading(false);
    }
  }

  function handleTabChange(nextTab: CustomSearchTab) {
    setSearchTab(nextTab);
    setSearchQuery("");
    setSearchCommittedQuery("");
    setSearchError("");
    setSearchResults([]);
    setSearchNoResultQuery(null);
  }

  function handlePickSearchResult(item: CustomSearchItem) {
    setPendingSelection({
      id: `${item.source}:${item.id}`,
      fallbackName: getCustomSearchItemDefaultTitle(item),
      cover: item.cover,
      coverMode: item.coverMode,
      source: item.source,
      sourceLabel: item.sourceLabel,
      externalUrl: item.externalUrl,
      releaseYear: item.releaseYear,
    });
    setPendingTitle(getCustomSearchItemDefaultTitle(item));
  }

  function handleSelectUploadFile(file: File) {
    if (cropObjectUrlRef.current) {
      URL.revokeObjectURL(cropObjectUrlRef.current);
      cropObjectUrlRef.current = null;
    }

    const objectUrl = URL.createObjectURL(file);
    cropObjectUrlRef.current = objectUrl;
    setCropImageSrc(objectUrl);
    setCropFileName(file.name);
    setCropOpen(true);
  }

  function handleCropOpenChange(open: boolean) {
    setCropOpen(open);
    if (!open && cropObjectUrlRef.current) {
      URL.revokeObjectURL(cropObjectUrlRef.current);
      cropObjectUrlRef.current = null;
      setCropImageSrc(null);
      setCropFileName("");
    }
  }

  function handleCropConfirm(coverData: string, titleSuggestion: string) {
    setPendingSelection({
      id: `upload:${Date.now()}`,
      fallbackName: titleSuggestion,
      cover: coverData,
      coverMode: "inline",
      source: "upload",
      sourceLabel: "本地上传",
    });
    setPendingTitle(titleSuggestion);
    handleCropOpenChange(false);
  }

  function handleConfirmSelection() {
    if (selectedSlot === null || !pendingSelection) return;
    const targetSlot = selectedSlot;
    const duplicateIndex = entries.findIndex(
      (item, index) => index !== targetSlot && item?.id === pendingSelection.id
    );

    if (duplicateIndex >= 0) {
      makeUndoSnapshot();
      setEntries((prev) => {
        const next = [...prev];
        const current = next[targetSlot];
        const duplicate = next[duplicateIndex];
        next[targetSlot] = duplicate ? { ...duplicate } : null;
        next[duplicateIndex] = current ? { ...current } : null;
        return next;
      });
      setSearchOpen(false);
      setSelectedSlot(null);
      resetSearchState();
      pushToast("success", `已与第 ${duplicateIndex + 1} 格互换`);
      return;
    }

    updateSlot(targetSlot, {
      ...pendingSelection,
      title: pendingTitle,
      comment: entries[targetSlot]?.comment,
      spoiler: entries[targetSlot]?.spoiler,
    });
    setSearchOpen(false);
    setSelectedSlot(null);
    resetSearchState();
    pushToast("success", `已填入第 ${targetSlot + 1} 格`);
  }

  function handleToggleSpoiler(index: number) {
    const entry = entries[index];
    if (!entry || !entry.spoiler) return;

    setSpoilerExpandedSet((prev) => {
      const next = new Set(prev);
      if (next.has(index)) {
        next.delete(index);
      } else {
        next.add(index);
      }
      return next;
    });
  }

  function handleGenerateImage() {
    if (filledCount < 9) {
      const confirmed = window.confirm(`当前仅选择了 ${filledCount}/9 部作品，确认继续生成吗？`);
      if (!confirmed) return;
    }

    setPreviewOpen(true);
  }

  function switchKind(nextKind: FillMode) {
    if (nextKind === "custom") {
      setKindPickerOpen(false);
      return;
    }
    setKindPickerOpen(false);
    router.push(getFillModeMeta(nextKind).route);
  }

  function handleLocalNoticeOpenChange(open: boolean) {
    setLocalNoticeOpen(open);
    if (!open) {
      try {
        localStorage.setItem(CUSTOM_LOCAL_NOTICE_SEEN_KEY, "1");
      } catch {
        // ignore write failures
      }
    }
  }

  function handleOpenUsageNotice() {
    setLocalNoticeStatus(null);
    setLocalNoticeOpen(true);
  }

  async function handleTestDownload() {
    setTestingDownload(true);
    try {
      const blob = await generateLocalTestImageBlob();
      downloadBlob(blob, "my9-custom-test.png");
      setLocalNoticeStatus({ kind: "success", message: "测试图片已触发下载。如果你能正常保存它，当前环境通常可用。" });
    } catch {
      setLocalNoticeStatus({ kind: "error", message: "测试下载失败。建议复制当前页面链接并改用系统浏览器。" });
    } finally {
      setTestingDownload(false);
    }
  }

  async function handleCopyCustomLink() {
    const customUrl = typeof window === "undefined" ? "/custom" : `${window.location.origin}/custom`;
    try {
      await copyText(customUrl);
      setLocalNoticeStatus({ kind: "success", message: "已复制当前页面链接，可以粘贴到系统浏览器。" });
    } catch {
      setLocalNoticeStatus({ kind: "error", message: "复制失败，请手动复制当前页面地址。"});
    }
  }

  return (
    <main className="min-h-screen bg-background px-4 py-16 text-foreground">
      <div className="mx-auto flex w-full max-w-2xl flex-col items-center gap-4">
        <header className="space-y-3 text-center">
          <div className="inline-flex items-center gap-2 sm:gap-3">
            <h1 className="whitespace-nowrap text-3xl font-bold leading-tight tracking-tight text-foreground sm:text-4xl">
              自定义模式
            </h1>
            <button
              type="button"
              onClick={() => setKindPickerOpen(true)}
              className="inline-flex items-center gap-1 rounded-full border border-border bg-card px-2.5 py-1 text-xs font-semibold text-card-foreground transition-colors hover:bg-accent hover:text-accent-foreground sm:px-3 sm:py-1.5 sm:text-sm"
              aria-label="切换填写类型"
            >
              <ChevronsUpDown className="h-3.5 w-3.5 text-muted-foreground" />
              切换
            </button>
          </div>
          <p className="text-sm text-muted-foreground">创建属于你自己的构成。</p>
          <button
            type="button"
            className="inline-flex items-center justify-center gap-1 rounded-full border border-amber-200 bg-amber-50 px-4 py-1.5 text-base font-semibold text-amber-700 transition-colors hover:bg-amber-100 dark:border-amber-900 dark:bg-amber-950/50 dark:text-amber-300 dark:hover:bg-amber-900/60"
            onClick={handleOpenUsageNotice}
          >
            使用须知
            <ChevronRight className="h-4 w-4 text-amber-500 dark:text-amber-300" aria-hidden="true" />
          </button>
          <UpdateLogNotice />
          <SupportButton />
        </header>

        {toast ? (
          <div className="fixed inset-x-0 top-4 z-50 flex justify-center px-4">
            <InlineToast kind={toast.kind} message={toast.message} />
          </div>
        ) : null}

        <div className="w-full max-w-xl">
          <label className="mb-2 block text-sm font-semibold text-foreground">创作者（推荐填写）</label>
          <Input
            value={creatorName}
            onChange={(event) => setCreatorName(event.target.value.slice(0, 40))}
            placeholder="输入你的昵称"
            className="w-full rounded-xl border-border bg-card px-4 py-2.5 text-sm text-foreground placeholder:text-muted-foreground focus-visible:ring-sky-200 dark:focus-visible:ring-sky-900"
          />
          <p className="mt-1 text-right text-xs text-muted-foreground">{creatorName.length}/40</p>
        </div>

        <div className="mx-auto w-full rounded-xl border-4 border-background bg-card p-1 shadow-2xl ring-1 ring-border/70 sm:p-4">
          <CustomNineGridBoard
            entries={entries}
            onSelectSlot={openSearch}
            onRemoveSlot={(index) => updateSlot(index, null)}
            onReorder={handleReorder}
          />
        </div>

        <CustomActionCluster
          filledCount={filledCount}
          canUndo={Boolean(singleUndoSnapshot)}
          canClear={filledCount > 0}
          onUndo={handleUndo}
          onClear={handleClear}
          onGenerate={handleGenerateImage}
        />

        <CustomSelectedEntriesList
          entries={entries}
          spoilerExpandedSet={spoilerExpandedSet}
          onToggleSpoiler={handleToggleSpoiler}
        />

        <SiteFooter className="w-full" kind="custom" />
      </div>

      <CustomSearchDialog
        open={searchOpen}
        onOpenChange={handleSearchOpenChange}
        activeTab={searchTab}
        onTabChange={handleTabChange}
        query={searchQuery}
        committedQuery={searchCommittedQuery}
        onQueryChange={(value) => {
          setSearchQuery(value);
          setSearchError("");
          if (value.trim().length === 0) {
            setSearchCommittedQuery("");
            setSearchResults([]);
            setSearchNoResultQuery(null);
          }
        }}
        loading={searchLoading}
        error={searchError}
        results={searchResults}
        noResultQuery={searchNoResultQuery}
        pendingSelection={pendingSelection}
        pendingTitle={pendingTitle}
        selectedSlotIndex={selectedSlot}
        onPendingTitleChange={setPendingTitle}
        onSubmitSearch={handleSearchSubmit}
        onPickResult={handlePickSearchResult}
        onConfirmSelection={handleConfirmSelection}
        onSelectUploadFile={handleSelectUploadFile}
      />

      <CustomImageCropDialog
        open={cropOpen}
        imageSrc={cropImageSrc}
        fileName={cropFileName}
        onOpenChange={handleCropOpenChange}
        onConfirm={handleCropConfirm}
      />

      <CustomImagePreviewDialog
        open={previewOpen}
        onOpenChange={setPreviewOpen}
        entries={entries}
        creatorName={creatorName}
        onNotice={pushToast}
      />

      <CustomLocalNoticeDialog
        open={localNoticeOpen}
        onOpenChange={handleLocalNoticeOpenChange}
        testing={testingDownload}
        status={localNoticeStatus}
        onTestDownload={handleTestDownload}
        onCopyLink={handleCopyCustomLink}
      />

      <Dialog open={kindPickerOpen} onOpenChange={setKindPickerOpen}>
        <DialogContent className="w-[86vw] max-w-[21rem] rounded-2xl p-4 sm:max-w-md sm:p-6">
          <DialogHeader>
            <DialogTitle>切换填写类型</DialogTitle>
          </DialogHeader>
          <div className="grid grid-cols-2 gap-2 sm:grid-cols-2">
            {FILL_MODE_ORDER.map((item) => {
              const meta = getFillModeMeta(item);
              const active = item === "custom";
              return (
                <Button
                  key={item}
                  type="button"
                  variant="outline"
                  onClick={() => switchKind(item)}
                  className={cn(
                    "h-auto justify-start gap-3 rounded-xl px-4 py-3 text-left",
                    active && "border-sky-300 bg-sky-50 text-sky-700 dark:border-sky-800 dark:bg-sky-950/50 dark:text-sky-200"
                  )}
                >
                  <SubjectKindIcon kind={item} className="h-4 w-4" />
                  <span className="font-semibold">{meta.label}</span>
                </Button>
              );
            })}
          </div>
        </DialogContent>
      </Dialog>
    </main>
  );
}
