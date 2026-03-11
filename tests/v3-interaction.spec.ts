import { expect, test, type Page } from "@playwright/test";
import { mkdirSync } from "node:fs";

const SHARE_ID = "60fe04cbe7874fa2";
const DEFAULT_KIND = "game";
const ONE_PIXEL_PNG_BASE64 =
  "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9YxX5iQAAAAASUVORK5CYII=";

type MockShareState = {
  kind: string;
  creatorName: string | null;
  games: Array<Record<string, unknown> | null>;
};

function createFilledGames() {
  return Array.from({ length: 9 }, (_, index) => ({
    id: 2000 + index,
    name: `Game ${index + 1}`,
    localizedName: `游戏 ${index + 1}`,
    cover: `https://lain.bgm.tv/r/400/pic/cover/l/mock-${index + 1}.jpg`,
    releaseYear: 2000 + index,
    gameTypeId: 0,
    platforms: ["PC"],
    comment: "",
    spoiler: false,
  }));
}

function buildSearchResponse(query: string, kind = DEFAULT_KIND) {
  if (query.toLowerCase() === "zelda") {
    return {
      ok: true,
      source: "bangumi",
      kind,
      items: [
        {
          id: 101,
          name: "The Legend of Zelda",
          localizedName: "塞尔达传说",
          cover: "https://lain.bgm.tv/r/400/pic/cover/l/zelda.jpg",
          releaseYear: 2017,
          gameTypeId: 0,
          platforms: ["Nintendo Switch"],
        },
        {
          id: 102,
          name: "Stardew Valley",
          localizedName: "星露谷物语",
          cover: "https://lain.bgm.tv/r/400/pic/cover/l/stardew.jpg",
          releaseYear: 2016,
          gameTypeId: 0,
          platforms: ["PC"],
        },
      ],
      topPickIds: [101],
      suggestions: ["可尝试游戏正式名或别名"],
      noResultQuery: null,
    };
  }

  const hash = Array.from(query).reduce((acc, char) => acc + char.charCodeAt(0), 0);
  const id = Math.max(1000, hash + 900);
  return {
    ok: true,
    source: "bangumi",
    kind,
    items: [
      {
        id,
        name: `Result ${query}`,
        localizedName: `结果 ${query}`,
        cover: `https://lain.bgm.tv/r/400/pic/cover/l/result-${id}.jpg`,
        releaseYear: 2020,
        gameTypeId: 0,
        platforms: ["PC"],
      },
    ],
    topPickIds: [id],
    suggestions: ["减少关键词，仅保留核心词"],
    noResultQuery: null,
  };
}

async function mockV3Apis(page: Page) {
  const state: MockShareState = {
    kind: DEFAULT_KIND,
    creatorName: "测试玩家",
    games: createFilledGames(),
  };

  await page.route(/\/api\/share\/touch\?/, async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ ok: true }),
    });
  });

  await page.route(/https:\/\/wsrv\.nl\/\?url=/, async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "image/png",
      body: Buffer.from(ONE_PIXEL_PNG_BASE64, "base64"),
    });
  });

  await page.route(/\/api\/subjects\/search\?/, async (route) => {
    const url = new URL(route.request().url());
    const q = (url.searchParams.get("q") || "").trim();
    const kind = (url.searchParams.get("kind") || DEFAULT_KIND).trim();
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(buildSearchResponse(q, kind)),
    });
  });

  await page.route(/\/api\/share(\?.*)?$/, async (route) => {
    const request = route.request();
    if (request.method() === "POST") {
      await new Promise((resolve) => setTimeout(resolve, 220));
      const body = request.postDataJSON() as {
        kind?: string;
        creatorName?: string | null;
        games?: Array<Record<string, unknown> | null>;
      };
      state.kind = body.kind || DEFAULT_KIND;
      state.creatorName = body.creatorName || null;
      state.games = Array.isArray(body.games) ? body.games : state.games;
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          ok: true,
          kind: state.kind,
          shareId: SHARE_ID,
          shareUrl: `http://localhost:3000/${state.kind}/s/${SHARE_ID}`,
          deduped: false,
        }),
      });
      return;
    }

    const url = new URL(request.url());
    const id = url.searchParams.get("id");
    if (id !== SHARE_ID) {
      await route.fulfill({
        status: 404,
        contentType: "application/json",
        body: JSON.stringify({ ok: false, error: "分享不存在" }),
      });
      return;
    }

    await route.fulfill({
      status: 200,
      contentType: "application/json",
        body: JSON.stringify({
          ok: true,
          kind: state.kind,
          shareId: SHARE_ID,
          creatorName: state.creatorName,
          games: state.games,
        createdAt: Date.now(),
        updatedAt: Date.now(),
        lastViewedAt: Date.now(),
      }),
    });
  });
}

async function installClientSpies(page: Page) {
  await page.addInitScript(() => {
    const g = window as typeof window & {
      __clipboardWrites?: string[];
      __clipboardFail?: boolean;
    };

    g.__clipboardWrites = [];
    g.__clipboardFail = false;

    Object.defineProperty(navigator, "clipboard", {
      configurable: true,
      value: {
        writeText: async (text: string) => {
          if (g.__clipboardFail) {
            throw new Error("clipboard_failed");
          }
          g.__clipboardWrites!.push(text);
        },
      },
    });
  });
}

async function fillSlot(page: Page, slot: number, query: string) {
  await page.getByLabel(`选择第 ${slot} 格游戏`).click();
  const searchInput = page.getByPlaceholder("输入游戏名");
  await searchInput.fill(query);
  await searchInput.press("Enter");
  await expect(page.locator("#search-results-list button").first()).toBeVisible();
  await searchInput.press("Enter");
  await expect(page.getByText(`已填入第 ${slot} 格`)).toBeVisible();
}

test.describe("v3 interaction", () => {
  test.describe.configure({ mode: "serial" });

  test.beforeEach(async ({ page }) => {
    await installClientSpies(page);
    await mockV3Apis(page);
  });

  test("首页显示类型选择并可进入填写页", async ({ page }) => {
    test.setTimeout(120_000);
    await page.goto("/");
    await expect(page.getByRole("heading", { name: "构成我的九部" })).toBeVisible();
    await expect(page.getByRole("button", { name: "游戏" })).toBeVisible();
    await expect(page.getByRole("button", { name: "动画" })).toBeVisible();
    await expect(page.getByRole("button", { name: "电视剧" })).toBeVisible();
    await expect(page.getByRole("button", { name: "电影" })).toBeVisible();
    await expect(page.getByRole("link", { name: "开始填写！" })).toBeVisible();
    await page.getByRole("link", { name: "开始填写！" }).click();
    await expect(page).toHaveURL("/game", { timeout: 30_000 });
    await expect(page.getByText("0 / 9 已选择")).toBeVisible();
    await expect(page.getByRole("button", { name: "撤销" })).toBeDisabled();
    await expect(page.getByRole("button", { name: "清空" })).toBeDisabled();
    await expect(page.getByRole("button", { name: "还差 9 个可保存" })).toBeEnabled();
    await expect(page.getByRole("button", { name: "保存图片" })).toHaveCount(0);
    await expect(page.getByRole("button", { name: "生成分享链接" })).toHaveCount(0);
    await expect(page.getByRole("button", { name: "生成分享图片" })).toHaveCount(0);
  });

  test("非法 kind 路径会回落首页", async ({ page }) => {
    await page.goto("/76c33a16-ef44-47ed-b239-d38b24206d95");
    await expect(page).toHaveURL("/", { timeout: 30_000 });
    await expect(page.getByRole("heading", { name: "构成我的九部" })).toBeVisible();
  });

  test("搜索键盘选择、重复项互换与评论剧透折叠生效", async ({ page }) => {
    await page.goto("/game");

    await page.getByLabel("选择第 1 格游戏").click();
    const firstSearchInput = page.getByPlaceholder("输入游戏名");
    await firstSearchInput.fill("zelda");
    await firstSearchInput.press("Enter");
    await expect(page.locator("#search-results-list button").first()).toBeVisible();
    await firstSearchInput.press("Enter");
    await expect(page.getByText("已填入第 1 格")).toBeVisible();

    await page.getByLabel("选择第 2 格游戏").click();
    const secondSearchInput = page.getByPlaceholder("输入游戏名");
    await secondSearchInput.fill("q2");
    await secondSearchInput.press("Enter");
    await expect(page.locator("#search-results-list button").first()).toBeVisible();
    await secondSearchInput.press("Enter");
    await expect(page.getByText("已填入第 2 格")).toBeVisible();

    await page.getByLabel("选择第 2 格游戏").click();
    const swapSearchInput = page.getByPlaceholder("输入游戏名");
    await swapSearchInput.fill("zelda");
    await swapSearchInput.press("Enter");
    await expect(page.locator("#search-results-list button").first()).toBeVisible();
    await swapSearchInput.press("Enter");
    await expect(page.getByText("已与第 1 格互换")).toBeVisible();

    const draftIds = await page.evaluate(() => {
      const raw = localStorage.getItem("my-nine-game:v1");
      if (!raw) return [];
      const parsed = JSON.parse(raw) as { games?: Array<{ id?: number | string } | null> };
      if (!Array.isArray(parsed.games)) return [];
      return parsed.games.map((item) => item?.id ?? null);
    });
    expect(draftIds[0]).toBe(1063);
    expect(draftIds[1]).toBe(101);

    await page.getByRole("button", { name: "编辑第 1 格评论" }).first().click();
    await page.getByPlaceholder("写下你想说的评论...").fill("终局剧情神作");
    await page.getByLabel("剧透折叠").check();
    await page.getByRole("button", { name: "保存", exact: true }).click();
    await expect(page.getByText("剧透评论已折叠，点击展开预览")).toBeVisible();
    await page.getByRole("button", { name: "剧透评论已折叠，点击展开预览" }).click();
    await expect(page.getByText("终局剧情神作")).toBeVisible();
  });

  test("回车提交搜索后不会自动选中首项", async ({ page }) => {
    await page.goto("/game");

    await page.getByLabel("选择第 1 格游戏").click();
    const searchInput = page.getByPlaceholder("输入游戏名");
    await searchInput.fill("zelda");
    await searchInput.press("Enter");

    await expect(page.locator("#search-results-list button").first()).toBeVisible();
    await expect(page.getByText("已填入第 1 格")).toHaveCount(0);
    await expect(page.getByText("0 / 9 已选择")).toBeVisible();
  });

  test("重新打开搜索窗口时保留上次搜索结果", async ({ page }) => {
    await page.goto("/game");

    await page.getByLabel("选择第 1 格游戏").click();
    const firstSearchInput = page.getByPlaceholder("输入游戏名");
    await firstSearchInput.fill("zelda");
    await firstSearchInput.press("Enter");
    await expect(page.locator("#search-results-list button").first()).toBeVisible();
    await expect(page.locator("#search-results-list").getByText("塞尔达传说")).toBeVisible();
    await page.getByRole("button", { name: "Close" }).click();

    await page.getByLabel("选择第 2 格游戏").click();
    const reopenedSearchInput = page.getByPlaceholder("输入游戏名");
    await expect(reopenedSearchInput).toHaveValue("zelda");
    await expect(page.locator("#search-results-list").getByText("塞尔达传说")).toBeVisible();
  });

  test("搜索缓存可跨刷新命中 sessionStorage", async ({ page }) => {
    let searchRequestCount = 0;
    page.on("request", (request) => {
      if (request.url().includes("/api/subjects/search?")) {
        searchRequestCount += 1;
      }
    });

    await page.goto("/game");

    await page.getByLabel("选择第 1 格游戏").click();
    const firstSearchInput = page.getByPlaceholder("输入游戏名");
    await firstSearchInput.fill("zelda");
    await firstSearchInput.press("Enter");
    await expect(page.locator("#search-results-list").getByText("塞尔达传说")).toBeVisible();
    await page.getByRole("button", { name: "Close" }).click();

    await page.reload();

    await page.getByLabel("选择第 2 格游戏").click();
    const secondSearchInput = page.getByPlaceholder("输入游戏名");
    await secondSearchInput.fill("  Zelda   ");
    await secondSearchInput.press("Enter");
    await expect(page.locator("#search-results-list").getByText("塞尔达传说")).toBeVisible();

    expect(searchRequestCount).toBe(1);
  });

  test("填写页刷新后保留本地缓存草稿", async ({ page }) => {
    await page.goto("/game");
    await page.getByPlaceholder("输入你的昵称").fill("缓存玩家");
    await page.getByLabel("选择第 1 格游戏").click();
    const searchInput = page.getByPlaceholder("输入游戏名");
    await searchInput.fill("zelda");
    await searchInput.press("Enter");
    await expect(page.locator("#search-results-list button").first()).toBeVisible();
    await searchInput.press("Enter");
    await expect(page.getByText("已填入第 1 格")).toBeVisible();

    await page.reload();

    await expect(page.getByPlaceholder("输入你的昵称")).toHaveValue("缓存玩家");
    await expect(page.getByText("1 / 9 已选择")).toBeVisible();
    await expect(page.getByText("塞尔达传说")).toBeVisible();
  });

  test("未填满可点击保存，需单次确认", async ({ page }) => {
    await page.goto("/game");
    await fillSlot(page, 1, "zelda");

    let dialogIndex = 0;
    page.on("dialog", async (dialog) => {
      dialogIndex += 1;
      await dialog.accept();
    });

    await page.getByRole("button", { name: "还差 8 个可保存" }).click();
    await expect(page).toHaveURL(`/${DEFAULT_KIND}/s/${SHARE_ID}`, { timeout: 30_000 });
    expect(dialogIndex).toBe(1);
  });

  test("9/9 保存后跳只读页，且只读操作锁定", async ({ page }) => {
    test.setTimeout(120_000);
    await page.goto("/game");
    for (let slot = 1; slot <= 9; slot += 1) {
      await fillSlot(page, slot, `q${slot}`);
    }
    await expect(page.getByRole("button", { name: "保存页面" })).toBeEnabled();
    await page.getByRole("button", { name: "保存页面" }).click();
    await expect(page.getByRole("button", { name: "保存中..." })).toBeVisible();
    await expect(page).toHaveURL(`/${DEFAULT_KIND}/s/${SHARE_ID}`, { timeout: 30_000 });

    await expect(page.getByText("这是共享页面（只读）")).toBeVisible();
    await expect(page.getByRole("button", { name: "撤销" })).toHaveCount(0);
    await expect(page.getByRole("button", { name: "清空" })).toHaveCount(0);
    await expect(page.getByRole("button", { name: "共享页面" })).toHaveCount(0);
    await expect(page.getByRole("button", { name: "保存图片" })).toHaveCount(0);
    await expect(page.getByText("9 / 9 已选择")).toHaveCount(0);
    await expect(page.getByRole("button", { name: "前往填写页面" })).toBeVisible();
    await page.getByRole("button", { name: "前往填写页面" }).click();
    await expect(page).toHaveURL(`/${DEFAULT_KIND}`, { timeout: 30_000 });
  });

  test("生成分享图片预览时会通过 wsrv 加载封面", async ({ page }) => {
    await page.goto("/game");
    await fillSlot(page, 1, "zelda");

    page.on("dialog", async (dialog) => {
      await dialog.accept();
    });
    await page.getByRole("button", { name: "还差 8 个可保存" }).click();
    await expect(page).toHaveURL(`/${DEFAULT_KIND}/s/${SHARE_ID}`, { timeout: 30_000 });

    const wsrvRequest = page.waitForRequest((request) =>
      request.url().includes("https://wsrv.nl/?url=")
    );

    await page.getByRole("button", { name: "生成分享图片" }).click();
    await expect(page.getByRole("heading", { name: "生成分享图片" })).toBeVisible();
    await wsrvRequest;
    await expect(page.getByAltText("分享图片预览")).toBeVisible({ timeout: 15_000 });

    mkdirSync("screenshot", { recursive: true });
    await page.screenshot({ path: "screenshot/share-image-preview-wsrv.png", fullPage: true });
  });

  test("只读页仅保留分享链接/分享图片，复制与导图可用", async ({ page }) => {
    await page.goto(`/${DEFAULT_KIND}/s/${SHARE_ID}`);
    await expect(page.getByText("正在加载共享页面...")).not.toBeVisible({ timeout: 15_000 });

    await expect(page.getByRole("button", { name: "生成分享链接" })).toBeVisible();
    await expect(page.getByRole("button", { name: "生成分享图片" })).toBeVisible();
    await expect(page.getByRole("button", { name: "X 分享" })).toHaveCount(0);
    await expect(page.getByRole("button", { name: "微博" })).toHaveCount(0);
    await expect(page.getByRole("button", { name: "QQ好友" })).toHaveCount(0);
    await expect(page.getByRole("button", { name: "QQ空间" })).toHaveCount(0);
    await expect(page.getByRole("button", { name: "B站文案" })).toHaveCount(0);

    await page.getByRole("button", { name: "生成分享链接" }).click();
    await expect(page.getByRole("heading", { name: "生成分享链接" })).toBeVisible();
    const linkInput = page.getByRole("textbox", { name: "当前分享链接" });
    await expect(linkInput).toHaveValue(new RegExp(`/${DEFAULT_KIND}/s/${SHARE_ID}$`));
    await page.getByRole("button", { name: "复制链接" }).click();
    await expect(page.getByText("复制成功", { exact: true })).toBeVisible();
    const copied = await page.evaluate(() => {
      const g = window as typeof window & { __clipboardWrites?: string[] };
      return g.__clipboardWrites || [];
    });
    expect(copied.some((item) => item.endsWith(`/${DEFAULT_KIND}/s/${SHARE_ID}`))).toBeTruthy();

    await page.evaluate(() => {
      const g = window as typeof window & { __clipboardFail?: boolean };
      g.__clipboardFail = true;
    });
    await page.getByRole("button", { name: "复制链接" }).click();
    await expect(page.getByText("复制失败，请手动复制上方链接。", { exact: true })).toBeVisible();
    await page.getByRole("button", { name: "关闭" }).click();

    await page.getByRole("button", { name: "生成分享图片" }).click();
    await expect(page.getByRole("heading", { name: "生成分享图片" })).toBeVisible();
    const qrSwitch = page.getByRole("switch", { name: "附带分享链接" });
    const showNameSwitch = page.getByRole("switch", { name: "显示名称" });
    await expect(qrSwitch).toHaveAttribute("aria-checked", "true");
    await expect(showNameSwitch).toHaveAttribute("aria-checked", "true");
    await expect(page.getByAltText("分享图片预览")).toBeVisible({ timeout: 15_000 });

    await page.evaluate(() => {
      const g = window as typeof window & {
        __MY9_LAST_DOWNLOAD_NAME__?: string;
        __ORIGIN_ANCHOR_SET_ATTRIBUTE__?: typeof HTMLAnchorElement.prototype.setAttribute;
      };
      if (!g.__ORIGIN_ANCHOR_SET_ATTRIBUTE__) {
        g.__ORIGIN_ANCHOR_SET_ATTRIBUTE__ = HTMLAnchorElement.prototype.setAttribute;
        HTMLAnchorElement.prototype.setAttribute = function (name: string, value: string) {
          if (name === "download") {
            g.__MY9_LAST_DOWNLOAD_NAME__ = value;
          }
          return g.__ORIGIN_ANCHOR_SET_ATTRIBUTE__!.call(this, name, value);
        };
      }
    });

    await page.getByRole("button", { name: "保存图片" }).click();

    const exportInfo = await page.evaluate(() => {
      const g = window as typeof window & {
        __MY9_LAST_SHARE_EXPORT__?: { width: number; height: number; showNames?: boolean };
      };
      return g.__MY9_LAST_SHARE_EXPORT__ || null;
    });
    const downloadName = await page.evaluate(() => {
      const g = window as typeof window & { __MY9_LAST_DOWNLOAD_NAME__?: string };
      return g.__MY9_LAST_DOWNLOAD_NAME__ || "";
    });
    expect(exportInfo).not.toBeNull();
    expect(exportInfo?.width).toBe(1080);
    expect(exportInfo?.height).toBe(1660);
    expect(exportInfo?.showNames).toBeTruthy();
    expect(downloadName.endsWith(".png")).toBeTruthy();
    expect(downloadName.includes("分享图")).toBeFalsy();
    expect(downloadName).toContain("测试玩家");

    await showNameSwitch.click();
    await expect(showNameSwitch).toHaveAttribute("aria-checked", "false");
    await page.getByRole("button", { name: "保存图片" }).click();
    const exportInfoWithoutNames = await page.evaluate(() => {
      const g = window as typeof window & {
        __MY9_LAST_SHARE_EXPORT__?: { showNames?: boolean };
      };
      return g.__MY9_LAST_SHARE_EXPORT__ || null;
    });
    expect(exportInfoWithoutNames?.showNames).toBeFalsy();

    await qrSwitch.click();
    await expect(qrSwitch).toHaveAttribute("aria-checked", "false");

    await page.evaluate(() => {
      const g = window as typeof window & {
        __ORIGIN_CREATE_OBJECT_URL__?: typeof URL.createObjectURL;
        __ORIGIN_ANCHOR_SET_ATTRIBUTE__?: typeof HTMLAnchorElement.prototype.setAttribute;
      };
      g.__ORIGIN_CREATE_OBJECT_URL__ = URL.createObjectURL;
      URL.createObjectURL = (() => {
        throw new Error("create_object_url_failed");
      }) as typeof URL.createObjectURL;
    });
    await page.getByRole("button", { name: "保存图片" }).click();
    await expect(page.getByText("下载失败，请长按预览图保存")).toBeVisible();
    await page.evaluate(() => {
      const g = window as typeof window & {
        __ORIGIN_CREATE_OBJECT_URL__?: typeof URL.createObjectURL;
        __ORIGIN_ANCHOR_SET_ATTRIBUTE__?: typeof HTMLAnchorElement.prototype.setAttribute;
      };
      if (g.__ORIGIN_CREATE_OBJECT_URL__) {
        URL.createObjectURL = g.__ORIGIN_CREATE_OBJECT_URL__;
      }
      if (g.__ORIGIN_ANCHOR_SET_ATTRIBUTE__) {
        HTMLAnchorElement.prototype.setAttribute = g.__ORIGIN_ANCHOR_SET_ATTRIBUTE__;
      }
    });
  });

  test("被污染 shareId 会重定向到规范链接", async ({ page }) => {
    await page.goto(`/${DEFAULT_KIND}/s/${SHARE_ID}${encodeURIComponent("申请恢复访问")}`);
    await expect(page).toHaveURL(`/${DEFAULT_KIND}/s/${SHARE_ID}`, { timeout: 30_000 });
    await expect(page.getByText("这是共享页面（只读）")).toBeVisible();
  });

  test("不同类型表格草稿隔离，创作者全局共享", async ({ page }) => {
    await page.goto("/anime");
    await page.getByPlaceholder("输入你的昵称").fill("全局玩家A");
    await page.getByLabel("选择第 1 格动画").click();
    const animeSearchInput = page.getByPlaceholder("输入动画名称");
    await animeSearchInput.fill("q1");
    await animeSearchInput.press("Enter");
    await expect(page.locator("#search-results-list button").first()).toBeVisible();
    await animeSearchInput.press("Enter");
    await expect(page.getByText("已填入第 1 格")).toBeVisible();

    await page.goto("/game");
    await expect(page.getByPlaceholder("输入你的昵称")).toHaveValue("全局玩家A");
    await expect(page.getByText("0 / 9 已选择")).toBeVisible();
    await page.getByPlaceholder("输入你的昵称").fill("全局玩家B");

    await page.goto("/anime");
    await expect(page.getByPlaceholder("输入你的昵称")).toHaveValue("全局玩家B");
    await expect(page.getByText("1 / 9 已选择")).toBeVisible();

  });

  test("移动端分享按钮顺序为图片在上链接在下", async ({ page }) => {
    await page.setViewportSize({ width: 390, height: 844 });
    await page.goto(`/${DEFAULT_KIND}/s/${SHARE_ID}`);
    await expect(page.getByText("正在加载共享页面...")).not.toBeVisible({ timeout: 15_000 });

    const imageButton = page.getByRole("button", { name: "生成分享图片" });
    const linkButton = page.getByRole("button", { name: "生成分享链接" });
    const [imageBox, linkBox] = await Promise.all([imageButton.boundingBox(), linkButton.boundingBox()]);
    expect(imageBox).not.toBeNull();
    expect(linkBox).not.toBeNull();
    expect((imageBox?.y || 0) < (linkBox?.y || 0)).toBeTruthy();
  });
});
