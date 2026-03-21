"use client";

import QRCode from "qrcode";
import { getCustomEntryExportTitle, type CustomEntry } from "@/lib/custom/types";
import { SubjectKind, getSubjectKindMeta } from "@/lib/subject-kind";
import { ShareGame } from "@/lib/share/types";

const CANVAS_WIDTH = 1080;
const CANVAS_HEIGHT = 1440;
const ENHANCED_EXTRA_HEIGHT = 220;

const PANEL_MARGIN_X = 42;
const PANEL_MARGIN_Y = 44;
const PANEL_PADDING = 18;
const PANEL_RADIUS = 22;
const SLOT_GAP = 14;

const PANEL_X = PANEL_MARGIN_X;
const PANEL_Y = PANEL_MARGIN_Y;
const PANEL_WIDTH = CANVAS_WIDTH - PANEL_MARGIN_X * 2;
const BASE_PANEL_HEIGHT = CANVAS_HEIGHT - PANEL_MARGIN_Y * 2;
const SECTION_DIVIDER_INSET = 22;
const SECTION_CONTENT_X = PANEL_X + 26;

const REVIEW_SECTION_TOP_PADDING = 28;
const REVIEW_SECTION_BOTTOM_PADDING = 32;
const REVIEW_SECTION_TITLE_LINE_HEIGHT = 38;
const REVIEW_SECTION_HEADER_GAP = 18;
const REVIEW_CARD_GAP = 18;
const REVIEW_CARD_RADIUS = 18;
const REVIEW_CARD_PADDING = 20;
const REVIEW_CARD_COVER_WIDTH = 92;
const REVIEW_CARD_COVER_HEIGHT = 124;
const REVIEW_CARD_TEXT_GAP = 18;
const REVIEW_CARD_TITLE_LINE_HEIGHT = 30;
const REVIEW_CARD_META_GAP = 14;
const REVIEW_COMMENT_LINE_HEIGHT = 34;
const REVIEW_SPOILER_BADGE_HEIGHT = 28;
const REVIEW_SPOILER_BADGE_HORIZONTAL_PADDING = 12;

type GridExportItem = {
  cover: string | null;
  title: string;
  alignTop?: boolean;
};

type ReviewExportItem = {
  slotIndex: number;
  cover: string | null;
  title: string;
  comment: string;
  spoiler: boolean;
  alignTop?: boolean;
};

type ReviewCardLayout = {
  item: ReviewExportItem;
  title: string;
  commentLines: string[];
  spoilerBadgeWidth: number;
  cardHeight: number;
};

type ReviewSectionLayout = {
  height: number;
  cards: ReviewCardLayout[];
};

function displayName(game: ShareGame | null): string {
  if (!game) return "未选择";
  return game.localizedName?.trim() || game.name;
}

function displayUserName(creatorName?: string | null): string {
  const value = creatorName?.trim();
  return value || "我";
}

export function buildDefaultShareImageHeaderTitle(kind: SubjectKind, creatorName?: string | null): string {
  const kindMeta = getSubjectKindMeta(kind);
  const userName = displayUserName(creatorName);
  return `构成${userName}的九${kindMeta.selectionUnit}${kindMeta.label}`;
}

export function buildDefaultShareImageHeaderSubtitle(
  kind: SubjectKind,
  creatorName?: string | null,
  reviewCount = 0
): string {
  const kindMeta = getSubjectKindMeta(kind);
  const userName = displayUserName(creatorName);
  return reviewCount > 0
    ? `扫码查看${userName}的${reviewCount}条评价`
    : `扫码查看${kindMeta.label}详情`;
}

async function srcToImage(src: string): Promise<HTMLImageElement> {
  const image = new Image();
  image.src = src;
  await new Promise<void>((resolve, reject) => {
    image.onload = () => resolve();
    image.onerror = () => reject(new Error("图片加载失败"));
  });
  return image;
}

async function blobToImage(blob: Blob): Promise<HTMLImageElement> {
  const objectUrl = URL.createObjectURL(blob);
  try {
    return await srcToImage(objectUrl);
  } finally {
    URL.revokeObjectURL(objectUrl);
  }
}

async function dataUrlToImage(dataUrl: string): Promise<HTMLImageElement> {
  return srcToImage(dataUrl);
}

function canvasToBlob(canvas: HTMLCanvasElement): Promise<Blob> {
  return new Promise<Blob>((resolve, reject) => {
    canvas.toBlob((blob) => {
      if (!blob) {
        reject(new Error("无法生成图片数据"));
        return;
      }
      resolve(blob);
    }, "image/png");
  });
}

function roundedRectPath(
  ctx: CanvasRenderingContext2D,
  x: number,
  y: number,
  width: number,
  height: number,
  radius: number
) {
  const r = Math.min(radius, width / 2, height / 2);
  ctx.beginPath();
  ctx.moveTo(x + r, y);
  ctx.arcTo(x + width, y, x + width, y + height, r);
  ctx.arcTo(x + width, y + height, x, y + height, r);
  ctx.arcTo(x, y + height, x, y, r);
  ctx.arcTo(x, y, x + width, y, r);
  ctx.closePath();
}

function drawCoverFit(
  ctx: CanvasRenderingContext2D,
  image: HTMLImageElement,
  x: number,
  y: number,
  width: number,
  height: number,
  options?: { alignTop?: boolean }
) {
  const scale = Math.max(width / image.width, height / image.height);
  const drawWidth = image.width * scale;
  const drawHeight = image.height * scale;
  const offsetX = x + (width - drawWidth) / 2;
  const offsetY = options?.alignTop ? y : y + (height - drawHeight) / 2;
  ctx.drawImage(image, offsetX, offsetY, drawWidth, drawHeight);
}

function shouldTopCropCover(kind?: SubjectKind) {
  return kind === "character" || kind === "person";
}

function drawEmptySlot(
  ctx: CanvasRenderingContext2D,
  x: number,
  y: number,
  width: number,
  height: number
) {
  const centerX = x + width / 2;
  const centerY = y + height / 2 - 12;

  ctx.strokeStyle = "#9ca3af";
  ctx.lineWidth = 4;
  ctx.lineCap = "round";
  ctx.beginPath();
  ctx.moveTo(centerX - 13, centerY);
  ctx.lineTo(centerX + 13, centerY);
  ctx.moveTo(centerX, centerY - 13);
  ctx.lineTo(centerX, centerY + 13);
  ctx.stroke();

  ctx.fillStyle = "#9ca3af";
  ctx.font = "600 24px sans-serif";
  ctx.textAlign = "center";
  ctx.fillText("选择", centerX, centerY + 45);
}

function trimTextToWidth(
  ctx: CanvasRenderingContext2D,
  text: string,
  maxWidth: number
): string {
  if (ctx.measureText(text).width <= maxWidth) {
    return text;
  }

  let output = text;
  while (output && ctx.measureText(`${output}...`).width > maxWidth) {
    output = output.slice(0, -1);
  }
  return `${output}...`;
}

function wrapTextToWidth(
  ctx: CanvasRenderingContext2D,
  text: string,
  maxWidth: number
): string[] {
  const normalized = text.replace(/\r\n/g, "\n");
  const paragraphs = normalized.split("\n");
  const lines: string[] = [];

  for (const paragraph of paragraphs) {
    if (!paragraph) {
      lines.push("");
      continue;
    }

    let currentLine = "";
    for (const char of Array.from(paragraph)) {
      const nextLine = `${currentLine}${char}`;
      if (!currentLine || ctx.measureText(nextLine).width <= maxWidth) {
        currentLine = nextLine;
        continue;
      }

      lines.push(currentLine);
      currentLine = char;
    }

    if (currentLine) {
      lines.push(currentLine);
    }
  }

  return lines.length > 0 ? lines : [""];
}

function normalizeCoverUrl(value: string): string | null {
  const raw = value.trim();
  if (!raw) return null;

  if (raw.startsWith("data:") || raw.startsWith("blob:")) {
    return raw;
  }

  if (raw.startsWith("//")) {
    return `https:${raw}`;
  }

  try {
    return new URL(raw).toString();
  } catch {
    try {
      return new URL(raw, "https://bgm.tv").toString();
    } catch {
      return null;
    }
  }
}

function toWsrvUrl(value: string): string | null {
  const normalized = normalizeCoverUrl(value);
  if (!normalized) return null;
  if (normalized.startsWith("data:") || normalized.startsWith("blob:")) {
    return normalized;
  }
  return `https://wsrv.nl/?url=${encodeURIComponent(normalized)}&w=640&output=webp`;
}

async function loadCoverImage(cover: string): Promise<HTMLImageElement | null> {
  const normalized = normalizeCoverUrl(cover);
  if (!normalized) return null;

  if (normalized.startsWith("data:") || normalized.startsWith("blob:")) {
    try {
      return await srcToImage(normalized);
    } catch {
      return null;
    }
  }

  const wsrvUrl = toWsrvUrl(normalized);
  if (!wsrvUrl) return null;

  try {
    const response = await fetch(wsrvUrl, { cache: "force-cache" });
    if (!response.ok) return null;
    return await blobToImage(await response.blob());
  } catch {
    return null;
  }
}

async function loadCovers(items: GridExportItem[]) {
  return Promise.all(items.map(async (item) => {
    const cover = item.cover?.trim();
    if (!cover) return null;
    return loadCoverImage(cover);
  }));
}

function drawPageBackground(ctx: CanvasRenderingContext2D, height: number) {
  ctx.fillStyle = "#f3f6fb";
  ctx.fillRect(0, 0, CANVAS_WIDTH, height);
}

function drawSectionDivider(ctx: CanvasRenderingContext2D, y: number) {
  ctx.strokeStyle = "#e2e8f0";
  ctx.lineWidth = 2;
  ctx.beginPath();
  ctx.moveTo(PANEL_X + SECTION_DIVIDER_INSET, y + 2);
  ctx.lineTo(PANEL_X + PANEL_WIDTH - SECTION_DIVIDER_INSET, y + 2);
  ctx.stroke();
}

function drawBoardPanel(ctx: CanvasRenderingContext2D, panelHeight: number) {
  ctx.save();
  ctx.shadowColor = "rgba(15, 23, 42, 0.16)";
  ctx.shadowBlur = 30;
  ctx.shadowOffsetY = 12;
  roundedRectPath(ctx, PANEL_X, PANEL_Y, PANEL_WIDTH, panelHeight, PANEL_RADIUS);
  ctx.fillStyle = "#ffffff";
  ctx.fill();
  ctx.restore();

  roundedRectPath(ctx, PANEL_X, PANEL_Y, PANEL_WIDTH, panelHeight, PANEL_RADIUS);
  ctx.strokeStyle = "#f1f5f9";
  ctx.lineWidth = 2;
  ctx.stroke();
}

function drawGrid(
  ctx: CanvasRenderingContext2D,
  items: GridExportItem[],
  covers: Array<HTMLImageElement | null>,
  showNames: boolean
) {
  const innerWidth = PANEL_WIDTH - PANEL_PADDING * 2;
  const gridHeight = BASE_PANEL_HEIGHT - PANEL_PADDING * 2;

  const slotWidth = Math.floor((innerWidth - SLOT_GAP * 2) / 3);
  const slotHeight = Math.floor((gridHeight - SLOT_GAP * 2) / 3);

  for (let index = 0; index < 9; index += 1) {
    const col = index % 3;
    const row = Math.floor(index / 3);
    const item = items[index] || { cover: null, title: "" };

    const x = PANEL_X + PANEL_PADDING + col * (slotWidth + SLOT_GAP);
    const y = PANEL_Y + PANEL_PADDING + row * (slotHeight + SLOT_GAP);

    ctx.save();
    roundedRectPath(ctx, x, y, slotWidth, slotHeight, 14);
    ctx.fillStyle = "#f9fafb";
    ctx.fill();
    ctx.strokeStyle = "#e5e7eb";
    ctx.lineWidth = 1;
    ctx.stroke();
    ctx.restore();

    ctx.save();
    roundedRectPath(ctx, x, y, slotWidth, slotHeight, 14);
    ctx.clip();
    const cover = covers[index];
    if (cover) {
      drawCoverFit(ctx, cover, x, y, slotWidth, slotHeight, {
        alignTop: item.alignTop,
      });
    } else {
      drawEmptySlot(ctx, x, y, slotWidth, slotHeight);
    }
    ctx.restore();

    ctx.fillStyle = "#d1d5db";
    ctx.font = "700 19px sans-serif";
    ctx.textAlign = "left";
    ctx.fillText(String(index + 1), x + 12, y + 24);

    if (showNames) {
      const stripHeight = 52;
      const stripY = y + slotHeight - stripHeight;
      const name = trimTextToWidth(ctx, item.title || "", slotWidth - 20);

      ctx.fillStyle = "rgba(255,255,255,0.95)";
      ctx.fillRect(x, stripY, slotWidth, stripHeight);
      ctx.fillStyle = "#111827";
      ctx.font = "700 21px sans-serif";
      ctx.textAlign = "center";
      ctx.textBaseline = "middle";
      ctx.fillText(name, x + slotWidth / 2, stripY + stripHeight / 2 + 1);
      ctx.textBaseline = "alphabetic";
    }
  }
}

async function createBoardCanvas(options: {
  items: GridExportItem[];
  totalHeight: number;
  panelHeight: number;
  showNames: boolean;
}) {
  const { items, totalHeight, panelHeight, showNames } = options;
  const covers = await loadCovers(items);

  const canvas = document.createElement("canvas");
  canvas.width = CANVAS_WIDTH;
  canvas.height = totalHeight;

  const ctx = canvas.getContext("2d");
  if (!ctx) {
    throw new Error("无法创建导出画布");
  }

  drawPageBackground(ctx, totalHeight);
  drawBoardPanel(ctx, panelHeight);
  drawGrid(ctx, items, covers, showNames);

  return { canvas, covers };
}

function toShareGridItems(kind: SubjectKind | undefined, games: Array<ShareGame | null>): GridExportItem[] {
  return games.map((game) => ({
    cover: game?.cover || null,
    title: displayName(game),
    alignTop: shouldTopCropCover(kind),
  }));
}

function toCustomGridItems(entries: Array<CustomEntry | null>): GridExportItem[] {
  return entries.map((entry) => ({
    cover: entry?.cover || null,
    title: getCustomEntryExportTitle(entry),
    alignTop: false,
  }));
}

function collectReviewItems(
  kind: SubjectKind | undefined,
  games: Array<ShareGame | null>
): ReviewExportItem[] {
  return games.flatMap((game, slotIndex) => {
    const comment = game?.comment?.trim();
    if (!game || !comment) return [];

    return [
      {
        slotIndex,
        cover: game.cover || null,
        title: displayName(game),
        comment,
        spoiler: Boolean(game.spoiler),
        alignTop: shouldTopCropCover(kind),
      },
    ];
  });
}

function buildReviewSectionLayout(reviewItems: ReviewExportItem[]): ReviewSectionLayout | null {
  if (reviewItems.length === 0) {
    return null;
  }

  const measureCanvas = document.createElement("canvas");
  const ctx = measureCanvas.getContext("2d");
  if (!ctx) {
    throw new Error("无法创建导出画布");
  }

  const cards: ReviewCardLayout[] = [];
  const cardWidth = PANEL_WIDTH - (SECTION_CONTENT_X - PANEL_X) * 2;
  const bodyWidth =
    cardWidth - REVIEW_CARD_PADDING * 2 - REVIEW_CARD_COVER_WIDTH - REVIEW_CARD_TEXT_GAP;

  for (const item of reviewItems) {
    ctx.font = "700 28px sans-serif";
    const spoilerBadgeWidth = item.spoiler
      ? Math.ceil(ctx.measureText("剧透").width + REVIEW_SPOILER_BADGE_HORIZONTAL_PADDING * 2)
      : 0;
    const titleGap = item.spoiler ? 12 : 0;
    const titleMaxWidth = Math.max(120, bodyWidth - spoilerBadgeWidth - titleGap);
    const title = trimTextToWidth(ctx, item.title, titleMaxWidth);

    ctx.font = "500 24px sans-serif";
    const commentLines = wrapTextToWidth(ctx, item.comment, bodyWidth);
    const commentHeight = Math.max(1, commentLines.length) * REVIEW_COMMENT_LINE_HEIGHT;

    const bodyHeight =
      REVIEW_CARD_PADDING +
      REVIEW_CARD_TITLE_LINE_HEIGHT +
      REVIEW_CARD_META_GAP +
      commentHeight +
      REVIEW_CARD_PADDING;

    const cardHeight = Math.max(
      REVIEW_CARD_PADDING * 2 + REVIEW_CARD_COVER_HEIGHT,
      bodyHeight
    );

    cards.push({
      item,
      title,
      commentLines,
      spoilerBadgeWidth,
      cardHeight,
    });
  }

  const cardsHeight =
    cards.reduce((sum, card) => sum + card.cardHeight, 0) + REVIEW_CARD_GAP * (cards.length - 1);

  return {
    cards,
    height:
      REVIEW_SECTION_TOP_PADDING +
      REVIEW_SECTION_TITLE_LINE_HEIGHT +
      REVIEW_SECTION_HEADER_GAP +
      cardsHeight +
      REVIEW_SECTION_BOTTOM_PADDING,
  };
}

function drawMissingReviewCover(
  ctx: CanvasRenderingContext2D,
  x: number,
  y: number,
  width: number,
  height: number
) {
  ctx.save();
  roundedRectPath(ctx, x, y, width, height, 14);
  ctx.fillStyle = "#f3f4f6";
  ctx.fill();
  ctx.restore();

  ctx.fillStyle = "#9ca3af";
  ctx.font = "600 20px sans-serif";
  ctx.textAlign = "center";
  ctx.textBaseline = "middle";
  ctx.fillText("无图", x + width / 2, y + height / 2);
  ctx.textBaseline = "alphabetic";
}

async function drawHeaderSection(
  ctx: CanvasRenderingContext2D,
  startY: number,
  options: {
    title: string;
    subtitle: string;
    qrUrl?: string | null;
    showQr?: boolean;
  }
) {
  const textX = SECTION_CONTENT_X;
  const showQr = options.showQr !== false && Boolean(options.qrUrl);
  let textMaxWidth = PANEL_X + PANEL_WIDTH - 26 - textX;

  if (showQr && options.qrUrl) {
    const qrDataUrl = await QRCode.toDataURL(options.qrUrl, {
      width: 180,
      margin: 1,
    });
    const qrImage = await dataUrlToImage(qrDataUrl);

    const qrSize = 150;
    const qrX = PANEL_X + PANEL_WIDTH - qrSize - 26;
    const qrY = startY + Math.round((ENHANCED_EXTRA_HEIGHT - qrSize) / 2);
    ctx.drawImage(qrImage, qrX, qrY, qrSize, qrSize);
    textMaxWidth = qrX - textX - 20;
  }

  const trimmedTitle = options.title.trim();

  ctx.textAlign = "left";
  if (trimmedTitle) {
    ctx.fillStyle = "#0f172a";
    ctx.font = "700 38px sans-serif";
    ctx.fillText(trimTextToWidth(ctx, trimmedTitle, textMaxWidth), textX, startY + 86);
  }

  ctx.fillStyle = "#334155";
  ctx.font = "600 30px sans-serif";
  ctx.fillText(trimTextToWidth(ctx, options.subtitle, textMaxWidth), textX, startY + 142);
}

function drawReviewSection(
  ctx: CanvasRenderingContext2D,
  startY: number,
  layout: ReviewSectionLayout,
  covers: Array<HTMLImageElement | null>
) {
  const cardWidth = PANEL_WIDTH - (SECTION_CONTENT_X - PANEL_X) * 2;
  const bodyX = SECTION_CONTENT_X + REVIEW_CARD_PADDING + REVIEW_CARD_COVER_WIDTH + REVIEW_CARD_TEXT_GAP;

  let currentY = startY + REVIEW_SECTION_TOP_PADDING;

  ctx.textAlign = "left";
  ctx.fillStyle = "#0f172a";
  ctx.font = "700 32px sans-serif";
  ctx.fillText("评价", SECTION_CONTENT_X, currentY + REVIEW_SECTION_TITLE_LINE_HEIGHT - 8);

  currentY += REVIEW_SECTION_TITLE_LINE_HEIGHT + REVIEW_SECTION_HEADER_GAP;

  for (const card of layout.cards) {
    const cardX = SECTION_CONTENT_X;
    const cardY = currentY;

    ctx.save();
    roundedRectPath(ctx, cardX, cardY, cardWidth, card.cardHeight, REVIEW_CARD_RADIUS);
    ctx.fillStyle = "#f8fafc";
    ctx.fill();
    ctx.strokeStyle = "#e2e8f0";
    ctx.lineWidth = 1.5;
    ctx.stroke();
    ctx.restore();

    const coverX = cardX + REVIEW_CARD_PADDING;
    const coverY = cardY + REVIEW_CARD_PADDING;
    const coverImage = covers[card.item.slotIndex] || null;

    ctx.save();
    roundedRectPath(
      ctx,
      coverX,
      coverY,
      REVIEW_CARD_COVER_WIDTH,
      REVIEW_CARD_COVER_HEIGHT,
      14
    );
    ctx.clip();
    if (coverImage) {
      drawCoverFit(
        ctx,
        coverImage,
        coverX,
        coverY,
        REVIEW_CARD_COVER_WIDTH,
        REVIEW_CARD_COVER_HEIGHT,
        {
          alignTop: card.item.alignTop,
        }
      );
    } else {
      drawMissingReviewCover(
        ctx,
        coverX,
        coverY,
        REVIEW_CARD_COVER_WIDTH,
        REVIEW_CARD_COVER_HEIGHT
      );
    }
    ctx.restore();

    const titleY = cardY + REVIEW_CARD_PADDING + 26;
    ctx.fillStyle = "#94a3b8";
    ctx.font = "700 22px sans-serif";
    ctx.fillText(`${card.item.slotIndex + 1}.`, bodyX, titleY);

    ctx.fillStyle = "#0f172a";
    ctx.font = "700 28px sans-serif";
    const slotWidth = ctx.measureText(`${card.item.slotIndex + 1}.`).width + 12;
    const titleX = bodyX + slotWidth;
    ctx.fillText(card.title, titleX, titleY);

    if (card.item.spoiler) {
      const badgeX = titleX + ctx.measureText(card.title).width + 12;
      const badgeY = titleY - REVIEW_SPOILER_BADGE_HEIGHT + 5;
      ctx.save();
      roundedRectPath(
        ctx,
        badgeX,
        badgeY,
        card.spoilerBadgeWidth,
        REVIEW_SPOILER_BADGE_HEIGHT,
        REVIEW_SPOILER_BADGE_HEIGHT / 2
      );
      ctx.fillStyle = "#fef3c7";
      ctx.fill();
      ctx.restore();

      ctx.fillStyle = "#b45309";
      ctx.font = "700 18px sans-serif";
      ctx.fillText(
        "剧透",
        badgeX + REVIEW_SPOILER_BADGE_HORIZONTAL_PADDING,
        titleY - 2
      );
    }

    ctx.fillStyle = "#334155";
    ctx.font = "500 24px sans-serif";
    let lineY = cardY + REVIEW_CARD_PADDING + REVIEW_CARD_TITLE_LINE_HEIGHT + REVIEW_CARD_META_GAP + 18;
    for (const line of card.commentLines) {
      ctx.fillText(line, bodyX, lineY);
      lineY += REVIEW_COMMENT_LINE_HEIGHT;
    }

    currentY += card.cardHeight + REVIEW_CARD_GAP;
  }
}

export async function generateShareImageBlob(options: {
  kind?: SubjectKind;
  shareId?: string;
  title?: string;
  games: Array<ShareGame | null>;
  creatorName?: string | null;
  origin?: string;
  showNames?: boolean;
  showHeaderBlock?: boolean;
  showHeaderQr?: boolean;
  headerSubtitle?: string;
  showComments?: boolean;
}) {
  const showHeaderBlock = options.showHeaderBlock !== false;
  const showHeaderQr = options.showHeaderQr !== false;
  const requestedReviewItems = options.showComments ? collectReviewItems(options.kind, options.games) : [];
  const reviewSectionLayout = buildReviewSectionLayout(requestedReviewItems);
  const showComments = Boolean(reviewSectionLayout);

  const headerExtraHeight = showHeaderBlock ? ENHANCED_EXTRA_HEIGHT : 0;
  const reviewExtraHeight = reviewSectionLayout?.height ?? 0;

  const { canvas, covers } = await createBoardCanvas({
    items: toShareGridItems(options.kind, options.games),
    totalHeight: CANVAS_HEIGHT + headerExtraHeight + reviewExtraHeight,
    panelHeight: BASE_PANEL_HEIGHT + headerExtraHeight + reviewExtraHeight,
    showNames: options.showNames !== false,
  });

  const ctx = canvas.getContext("2d");
  if (!ctx) {
    throw new Error("无法创建导出画布");
  }

  const kindMeta = options.kind ? getSubjectKindMeta(options.kind) : null;
  const reviewCount = requestedReviewItems.length;
  let currentY = PANEL_Y + BASE_PANEL_HEIGHT;
  let qrUrl: string | null = null;

  if (showHeaderBlock) {
    if (!options.kind) {
      throw new Error("缺少分享链接所需参数");
    }

    if (showHeaderQr && options.shareId) {
      const origin = options.origin ?? window.location.origin;
      qrUrl = `${origin}/${options.kind}/s/${options.shareId}`;
    }

    const line1 = kindMeta
      ? buildDefaultShareImageHeaderTitle(options.kind, options.creatorName)
      : options.title?.trim() || "我的九宫格";
    const line2 =
      options.headerSubtitle ??
      (kindMeta
        ? buildDefaultShareImageHeaderSubtitle(options.kind, options.creatorName, reviewCount)
        : "扫码查看分享页");

    drawSectionDivider(ctx, currentY);
    await drawHeaderSection(ctx, currentY, {
      title: line1,
      subtitle: line2,
      qrUrl,
      showQr: showHeaderQr,
    });
    currentY += ENHANCED_EXTRA_HEIGHT;
  }

  if (reviewSectionLayout) {
    drawSectionDivider(ctx, currentY);
    drawReviewSection(ctx, currentY, reviewSectionLayout, covers);
  }

  (window as typeof window & Record<string, unknown>).__MY9_LAST_SHARE_EXPORT__ = {
    width: canvas.width,
    height: canvas.height,
    showNames: options.showNames !== false,
    showHeaderBlock,
    showHeaderQr,
    headerSubtitle: options.headerSubtitle ?? null,
    showComments,
    reviewCount,
    qrUrl,
  };

  return canvasToBlob(canvas);
}

async function generateQrGridImageBlob(options: {
  title: string;
  subtitle: string;
  qrUrl: string;
  items: GridExportItem[];
  showNames?: boolean;
  debugInfoKey?: string;
  debugInfo?: Record<string, unknown>;
}) {
  const { canvas } = await createBoardCanvas({
    items: options.items,
    totalHeight: CANVAS_HEIGHT + ENHANCED_EXTRA_HEIGHT,
    panelHeight: BASE_PANEL_HEIGHT + ENHANCED_EXTRA_HEIGHT,
    showNames: options.showNames !== false,
  });

  const ctx = canvas.getContext("2d");
  if (!ctx) {
    throw new Error("无法创建导出画布");
  }

  const extY = PANEL_Y + BASE_PANEL_HEIGHT;
  drawSectionDivider(ctx, extY);
  await drawHeaderSection(ctx, extY, {
    title: options.title,
    subtitle: options.subtitle,
    qrUrl: options.qrUrl,
  });

  if (options.debugInfoKey) {
    (window as typeof window & Record<string, unknown>)[options.debugInfoKey] = {
      width: canvas.width,
      height: canvas.height,
      showNames: options.showNames !== false,
      qrUrl: options.qrUrl,
      ...options.debugInfo,
    };
  }

  return canvasToBlob(canvas);
}

export async function generateStandardShareImageBlob(options: {
  kind?: SubjectKind;
  games: Array<ShareGame | null>;
  creatorName?: string | null;
  showNames?: boolean;
}) {
  return generateShareImageBlob({
    kind: options.kind,
    games: options.games,
    creatorName: options.creatorName,
    showNames: options.showNames,
    showHeaderBlock: false,
    showComments: false,
  });
}

export async function generateStandardCustomShareImageBlob(options: {
  entries: Array<CustomEntry | null>;
  showNames?: boolean;
}) {
  const { canvas } = await createBoardCanvas({
    items: toCustomGridItems(options.entries),
    totalHeight: CANVAS_HEIGHT,
    panelHeight: BASE_PANEL_HEIGHT,
    showNames: options.showNames !== false,
  });

  (window as typeof window & Record<string, unknown>).__MY9_LAST_CUSTOM_EXPORT__ = {
    width: canvas.width,
    height: canvas.height,
    showNames: options.showNames !== false,
    qrUrl: null,
  };

  return canvasToBlob(canvas);
}

export async function generateEnhancedShareImageBlob(options: {
  kind: SubjectKind;
  shareId: string;
  title: string;
  games: Array<ShareGame | null>;
  creatorName?: string | null;
  origin?: string;
  showNames?: boolean;
  showHeaderQr?: boolean;
  headerSubtitle?: string;
  showComments?: boolean;
}) {
  return generateShareImageBlob({
    kind: options.kind,
    shareId: options.shareId,
    title: options.title,
    games: options.games,
    creatorName: options.creatorName,
    origin: options.origin,
    showNames: options.showNames,
    showHeaderBlock: true,
    showHeaderQr: options.showHeaderQr,
    headerSubtitle: options.headerSubtitle,
    showComments: options.showComments,
  });
}

export async function generateCustomShareImageBlob(options: {
  title: string;
  qrUrl: string;
  entries: Array<CustomEntry | null>;
  showNames?: boolean;
}) {
  return generateQrGridImageBlob({
    title: options.title,
    subtitle: "扫码填写你的构成",
    qrUrl: options.qrUrl,
    items: toCustomGridItems(options.entries),
    showNames: options.showNames,
    debugInfoKey: "__MY9_LAST_CUSTOM_EXPORT__",
  });
}

export async function generateLocalTestImageBlob() {
  const canvas = document.createElement("canvas");
  canvas.width = 960;
  canvas.height = 640;

  const ctx = canvas.getContext("2d");
  if (!ctx) {
    throw new Error("无法创建测试画布");
  }

  ctx.fillStyle = "#f3f6fb";
  ctx.fillRect(0, 0, canvas.width, canvas.height);

  ctx.fillStyle = "#ffffff";
  ctx.fillRect(60, 60, canvas.width - 120, canvas.height - 120);
  ctx.strokeStyle = "#dbe4f0";
  ctx.lineWidth = 4;
  ctx.strokeRect(60, 60, canvas.width - 120, canvas.height - 120);

  ctx.fillStyle = "#0f172a";
  ctx.font = "700 46px sans-serif";
  ctx.textAlign = "center";
  ctx.fillText("保存图片测试", canvas.width / 2, 240);

  ctx.fillStyle = "#475569";
  ctx.font = "600 28px sans-serif";
  ctx.fillText("如果这张图可以正常下载，当前浏览器环境通常可用。", canvas.width / 2, 310);
  ctx.fillText("若失败，请复制 /custom 到系统浏览器继续。", canvas.width / 2, 360);

  return canvasToBlob(canvas);
}

export function downloadBlob(blob: Blob, filename: string) {
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.setAttribute("download", filename);
  link.setAttribute("href", url);
  link.style.display = "none";
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  window.setTimeout(() => URL.revokeObjectURL(url), 1000);
}

export async function exportEnhancedShareImage(options: {
  kind: SubjectKind;
  shareId: string;
  title: string;
  games: Array<ShareGame | null>;
  creatorName?: string | null;
  origin?: string;
  showNames?: boolean;
  showHeaderQr?: boolean;
  headerSubtitle?: string;
  showComments?: boolean;
}) {
  const blob = await generateEnhancedShareImageBlob(options);
  const fileName = `${options.title}.png`;
  downloadBlob(blob, fileName);
}
