import { expect, test, type Page } from "@playwright/test";
import { mkdirSync } from "node:fs";

const SHARE_ID = "60fe04cbe7874fa2";
const DEFAULT_KIND = "game";
const ONE_PIXEL_PNG_BASE64 =
  "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9YxX5iQAAAAASUVORK5CYII=";
const LANDSCAPE_SVG = `
<svg xmlns="http://www.w3.org/2000/svg" width="960" height="640" viewBox="0 0 960 640">
  <rect width="960" height="640" fill="#f3f6fb"/>
  <rect x="60" y="60" width="840" height="520" fill="#ffffff" stroke="#dbe4f0" stroke-width="4"/>
  <text x="480" y="260" text-anchor="middle" font-size="46" font-weight="700" fill="#0f172a">保存图片测试</text>
  <text x="480" y="330" text-anchor="middle" font-size="28" font-weight="600" fill="#475569">如果这张图可以正常下载，当前浏览器环境通常可用。</text>
  <text x="480" y="380" text-anchor="middle" font-size="28" font-weight="600" fill="#475569">若失败，请复制 /custom 到系统浏览器继续。</text>
</svg>
`.trim();
const PORTRAIT_SVG = `
<svg xmlns="http://www.w3.org/2000/svg" width="720" height="960" viewBox="0 0 720 960">
  <rect width="720" height="960" fill="#f3f6fb"/>
  <rect x="40" y="40" width="640" height="880" fill="#ffffff" stroke="#dbe4f0" stroke-width="4"/>
  <text x="360" y="360" text-anchor="middle" font-size="46" font-weight="700" fill="#0f172a">3:4 测试图</text>
  <text x="360" y="450" text-anchor="middle" font-size="28" font-weight="600" fill="#475569">最小缩放时不应被额外裁边</text>
</svg>
`.trim();

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
  if (kind === "work") {
    if (query.toLowerCase() === "work-tmdb-movie") {
      return {
        ok: true,
        source: "mixed",
        kind,
        items: [
          {
            id: "tmdb:movie:550",
            name: "Fight Club",
            localizedName: "搏击俱乐部",
            cover: "https://image.tmdb.org/t/p/w500/fight-club.jpg",
            releaseYear: 1999,
            genres: ["剧情"],
          },
        ],
        noResultQuery: null,
      };
    }

    if (query.toLowerCase() === "work-itunes-song") {
      return {
        ok: true,
        source: "mixed",
        kind,
        items: [
          {
            id: "itunes:song:909253",
            name: "Taylor Swift",
            localizedName: "Love Story",
            cover: "https://is1-ssl.mzstatic.com/image/thumb/Music123/v4/mock/cover/100x100bb.jpg",
            releaseYear: 2008,
            genres: ["Pop"],
            storeUrls: {
              apple: "https://music.apple.com/cn/album/love-story/123456?i=909253",
            },
          },
        ],
        noResultQuery: null,
      };
    }

    const hash = Array.from(query).reduce((acc, char) => acc + char.charCodeAt(0), 0);
    const id = Math.max(1000, hash + 900);
    return {
      ok: true,
      source: "mixed",
      kind,
      items: [
        {
          id: `tmdb:movie:${id}`,
          name: `Work ${query}`,
          localizedName: `作品 ${query}`,
          cover: `https://image.tmdb.org/t/p/w500/work-${id}.jpg`,
          releaseYear: 2020,
          genres: ["剧情"],
        },
      ],
      noResultQuery: null,
    };
  }

  if (kind === "character") {
    return {
      ok: true,
      source: "bangumi",
      kind,
      items: [
        {
          id: 88001,
          name: "アルトリア・ペンドラゴン",
          localizedName: "阿尔托莉雅·潘德拉贡",
          cover: "https://lain.bgm.tv/r/400/pic/crt/l/mock-character.jpg",
          releaseYear: 2004,
          gameTypeId: 0,
          platforms: [],
        },
      ],
      noResultQuery: null,
    };
  }

  if (kind === "person") {
    return {
      ok: true,
      source: "bangumi",
      kind,
      items: [
        {
          id: 99002,
          name: "宮崎駿",
          localizedName: "宫崎骏",
          cover: "https://lain.bgm.tv/r/400/pic/crt/l/mock-person.jpg",
          releaseYear: 1941,
          gameTypeId: 0,
          platforms: [],
        },
      ],
      noResultQuery: null,
    };
  }

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
    noResultQuery: null,
  };
}

function buildCustomSearchResponse(query: string, source: "bangumi" | "tmdb" | "apple") {
  if (source === "bangumi") {
    return {
      ok: true,
      source,
      items: [
        {
          id: "8801",
          name: "Bangumi Source",
          localizedName: "Bangumi 作品",
          cover: "https://lain.bgm.tv/r/400/pic/cover/l/custom-bangumi.jpg",
          coverMode: "remote",
          source: "bangumi",
          sourceLabel: "Bangumi",
          externalUrl: "https://bgm.tv/subject/8801",
          releaseYear: 2018,
        },
      ],
      noResultQuery: null,
    };
  }

  if (source === "tmdb") {
    return {
      ok: true,
      source,
      items: [
        {
          id: "movie:550",
          name: "Fight Club",
          localizedName: "搏击俱乐部",
          cover: "https://image.tmdb.org/t/p/w500/custom-tmdb.jpg",
          coverMode: "remote",
          source: "tmdb",
          sourceLabel: "TMDB",
          externalUrl: "https://www.themoviedb.org/movie/550",
          releaseYear: 1999,
        },
      ],
      noResultQuery: null,
    };
  }

  return {
    ok: true,
    source,
    items: [
      {
        id: `apple-${query || "1"}`,
        name: "Taylor Swift",
        localizedName: "Love Story",
        cover: "https://is1-ssl.mzstatic.com/image/thumb/Music123/v4/mock/cover/100x100bb.jpg",
        coverMode: "remote",
        source: "apple",
        sourceLabel: "Apple Music",
        externalUrl: "https://music.apple.com/cn/album/love-story/123456?i=909253",
        releaseYear: 2008,
      },
    ],
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

async function mockCustomSearchApi(page: Page) {
  await page.route(/\/api\/custom\/search\?/, async (route) => {
    const url = new URL(route.request().url());
    const source = (url.searchParams.get("source") || "bangumi").trim() as "bangumi" | "tmdb" | "apple";
    const q = (url.searchParams.get("q") || "").trim();

    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(buildCustomSearchResponse(q, source)),
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

async function fillSlotByKind(page: Page, options: {
  slot: number;
  subjectLabel: string;
  searchPlaceholder: string;
  query: string;
}) {
  const { slot, subjectLabel, searchPlaceholder, query } = options;
  await page.getByLabel(`选择第 ${slot} 格${subjectLabel}`).click();
  const searchInput = page.getByPlaceholder(searchPlaceholder);
  await searchInput.fill(query);
  await searchInput.press("Enter");
  await expect(page.locator("#search-results-list button").first()).toBeVisible();
  await searchInput.press("Enter");
  await expect(page.getByText(`已填入第 ${slot} 格`)).toBeVisible();
}

async function mockTrendsApi(page: Page) {
  await page.route(/\/api\/trends\?/, async (route) => {
    const url = new URL(route.request().url());
    const kind = (url.searchParams.get("kind") || DEFAULT_KIND).trim();
    const period = (url.searchParams.get("period") || "24h").trim();
    const view = (url.searchParams.get("view") || "overall").trim();

    const trendId = kind === "character"
      ? "88001"
      : kind === "person"
        ? "99002"
        : kind === "work"
          ? "tmdb:movie:77001"
          : "77001";
    const trendName = kind === "character"
      ? "测试角色"
      : kind === "person"
        ? "测试人物"
        : kind === "work"
          ? "测试作品"
          : "测试条目";

    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        ok: true,
        period,
        view,
        sampleCount: 31,
        range: {
          from: Date.now() - 24 * 60 * 60 * 1000,
          to: Date.now(),
        },
        lastUpdatedAt: Date.now(),
        items: [
          {
            key: `${kind}:${trendId}`,
            label: trendName,
            count: 12,
            games: [
              {
                id: trendId,
                name: trendName,
                localizedName: trendName,
                cover: "https://lain.bgm.tv/r/400/pic/cover/l/trend.jpg",
                releaseYear: 2020,
                count: 12,
              },
            ],
          },
        ],
      }),
    });
  });
}

test.describe("v3 interaction", () => {
  test.describe.configure({ mode: "serial" });

  test.beforeEach(async ({ page }) => {
    await installClientSpies(page);
    await mockV3Apis(page);
    await mockCustomSearchApi(page);
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
    await expect(page.getByRole("button", { name: /^还差 9 .可保存$/ })).toBeEnabled();
    await expect(page.getByRole("button", { name: "保存图片" })).toHaveCount(0);
    await expect(page.getByRole("button", { name: "生成分享链接" })).toHaveCount(0);
    await expect(page.getByRole("button", { name: "生成分享图片" })).toHaveCount(0);
  });

  test("非法 kind 路径会回落首页", async ({ page }) => {
    await page.goto("/76c33a16-ef44-47ed-b239-d38b24206d95");
    await expect(page).toHaveURL("/", { timeout: 30_000 });
    await expect(page.getByRole("heading", { name: "构成我的九部" })).toBeVisible();
  });

  test("非法分享路径会回落首页", async ({ page }) => {
    await page.goto("/game/s/%20");
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

    await page.getByRole("button", { name: /^还差 8 .可保存$/ }).click();
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
    await page.getByRole("button", { name: /^还差 8 .可保存$/ }).click();
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

  test("角色与人物分享页条目外链指向 Bangumi 角色/人物页", async ({ page }) => {
    const cases = [
      {
        kind: "character",
        subjectLabel: "角色",
        searchPlaceholder: "输入角色名称",
        segment: "character",
        localizedName: "阿尔托莉雅·潘德拉贡",
      },
      {
        kind: "person",
        subjectLabel: "人物",
        searchPlaceholder: "输入人物名称",
        segment: "person",
        localizedName: "宫崎骏",
      },
    ] as const;

    for (const item of cases) {
      await page.goto(`/${item.kind}`);
      await fillSlotByKind(page, {
        slot: 1,
        subjectLabel: item.subjectLabel,
        searchPlaceholder: item.searchPlaceholder,
        query: `${item.kind}-q1`,
      });
      await expect(page.locator("article h3", { hasText: item.localizedName }).first()).toBeVisible();

      page.once("dialog", async (dialog) => {
        await dialog.accept();
      });
      await page.getByRole("button", { name: /^还差 8 .可保存$/ }).click();
      await expect(page).toHaveURL(`/${item.kind}/s/${SHARE_ID}`, { timeout: 30_000 });

      const link = page.getByTitle("在 Bangumi 查看").first();
      await expect(link).toHaveAttribute(
        "href",
        new RegExp(`^https://bgm\\.tv/${item.segment}/\\d+$`)
      );
    }
  });

  test("作品分享页条目外链会按条目来源跳转 TMDB/Apple Music", async ({ page }) => {
    await page.goto("/work");

    await fillSlotByKind(page, {
      slot: 1,
      subjectLabel: "作品",
      searchPlaceholder: "输入作品名称",
      query: "work-tmdb-movie",
    });
    await fillSlotByKind(page, {
      slot: 2,
      subjectLabel: "作品",
      searchPlaceholder: "输入作品名称",
      query: "work-itunes-song",
    });

    page.once("dialog", async (dialog) => {
      await dialog.accept();
    });
    await page.getByRole("button", { name: /^还差 7 .可保存$/ }).click();
    await expect(page).toHaveURL(`/work/s/${SHARE_ID}`, { timeout: 30_000 });

    const tmdbLink = page.getByTitle("在 TMDB 查看").first();
    await expect(tmdbLink).toHaveAttribute("href", "https://www.themoviedb.org/movie/550");

    const appleLink = page.getByTitle("在 Apple Music 查看").first();
    await expect(appleLink).toHaveAttribute(
      "href",
      "https://music.apple.com/cn/album/love-story/123456?i=909253"
    );
  });

  test("趋势页角色与人物外链分别指向 Bangumi 角色/人物页", async ({ page }) => {
    await mockTrendsApi(page);

    await page.goto("/trends?kind=character&period=24h&view=year");
    await page.getByRole("button", { name: "今天" }).click();
    await expect(page.getByRole("button", { name: "类型" })).toHaveCount(0);
    await expect(page.getByRole("button", { name: "现代" })).toHaveCount(0);
    await expect(page.getByRole("button", { name: "经典" })).toHaveCount(0);

    const trendLink = page.locator('a[title="在 Bangumi 查看"]').first();
    await expect(trendLink).toHaveAttribute("href", "https://bgm.tv/character/88001", {
      timeout: 15_000,
    });

    await page.getByRole("button", { name: "人物" }).click();
    await expect(trendLink).toHaveAttribute("href", "https://bgm.tv/person/99002", {
      timeout: 15_000,
    });
  });

  test("趋势页作品外链会按条目来源指向 TMDB", async ({ page }) => {
    await mockTrendsApi(page);

    await page.goto("/trends?kind=work&period=24h&view=overall");
    await page.getByRole("button", { name: "今天" }).click();
    const trendLink = page.locator('a[title="在 TMDB 查看"]').first();
    await expect(trendLink).toHaveAttribute("href", "https://www.themoviedb.org/movie/77001", {
      timeout: 15_000,
    });
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

  test("自定义模式首次提示只出现一次并可测试保存图片", async ({ page }) => {
    await page.goto("/custom");
    await expect(page.getByRole("heading", { name: "使用须知" })).toBeVisible();

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

    await page.getByRole("button", { name: "测试保存图片" }).click();
    await expect(page.getByText("测试图片已触发下载。如果你能正常保存它，当前环境通常可用。")).toBeVisible();
    const downloadName = await page.evaluate(() => {
      const g = window as typeof window & { __MY9_LAST_DOWNLOAD_NAME__?: string };
      return g.__MY9_LAST_DOWNLOAD_NAME__ || "";
    });
    expect(downloadName).toBe("my9-custom-test.png");

    await page.getByRole("button", { name: "我知道了" }).click();
    await expect(page.getByRole("heading", { name: "使用须知" })).toHaveCount(0);
    await expect(page.getByRole("button", { name: "使用须知" })).toBeVisible();
    await page.getByRole("button", { name: "使用须知" }).click();
    await expect(page.getByRole("heading", { name: "使用须知" })).toBeVisible();
    await page.getByRole("button", { name: "我知道了" }).click();

    await page.reload();
    await expect(page.getByRole("heading", { name: "使用须知" })).toHaveCount(0);
  });

  test("自定义模式上传裁切在移动端预览不拉伸", async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 812 });
    await page.goto("/custom");
    await page.getByRole("button", { name: "我知道了" }).click();

    await page.getByLabel("选择第 1 格作品").click();
    await page.getByRole("button", { name: "上传" }).click();
    await page.locator('input[type="file"]').setInputFiles({
      name: "landscape.svg",
      mimeType: "image/svg+xml",
      buffer: Buffer.from(LANDSCAPE_SVG, "utf8"),
    });

    await expect(page.getByRole("heading", { name: "裁切上传图片" })).toBeVisible();
    await expect(page.locator(".reactEasyCrop_Image")).toBeVisible();

    const mediaRatio = await page.evaluate(() => {
      const media = document.querySelector(".reactEasyCrop_Image") as HTMLImageElement | null;
      if (!media) return null;
      const rect = media.getBoundingClientRect();
      return {
        renderedRatio: rect.width / rect.height,
        naturalRatio: media.naturalWidth / media.naturalHeight,
      };
    });

    expect(mediaRatio).not.toBeNull();
    expect(Math.abs((mediaRatio?.renderedRatio || 0) - (mediaRatio?.naturalRatio || 0))).toBeLessThan(0.05);
  });

  test("自定义模式上传裁切在移动端对精确比例图不额外裁边", async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 812 });
    await page.goto("/custom");
    await page.getByRole("button", { name: "我知道了" }).click();

    await page.getByLabel("选择第 1 格作品").click();
    await page.getByRole("button", { name: "上传" }).click();
    await page.locator('input[type="file"]').setInputFiles({
      name: "portrait.svg",
      mimeType: "image/svg+xml",
      buffer: Buffer.from(PORTRAIT_SVG, "utf8"),
    });

    await expect(page.getByRole("heading", { name: "裁切上传图片" })).toBeVisible();
    await expect(page.locator(".reactEasyCrop_Image")).toBeVisible();

    const sizeInfo = await page.evaluate(() => {
      const media = document.querySelector(".reactEasyCrop_Image") as HTMLImageElement | null;
      const cropArea = document.querySelector(".reactEasyCrop_CropArea") as HTMLDivElement | null;
      const container = document.querySelector(".reactEasyCrop_Container") as HTMLDivElement | null;
      if (!media || !cropArea || !container) return null;
      const mediaRect = media.getBoundingClientRect();
      const cropRect = cropArea.getBoundingClientRect();
      const containerRect = container.getBoundingClientRect();
      return {
        containerWidth: containerRect.width,
        containerHeight: containerRect.height,
        mediaWidth: mediaRect.width,
        mediaHeight: mediaRect.height,
        cropWidth: cropRect.width,
        cropHeight: cropRect.height,
      };
    });

    expect(sizeInfo).not.toBeNull();
    expect(Math.abs((sizeInfo?.mediaWidth || 0) - (sizeInfo?.cropWidth || 0))).toBeLessThan(1.5);
    expect(Math.abs((sizeInfo?.mediaHeight || 0) - (sizeInfo?.cropHeight || 0))).toBeLessThan(1.5);
  });

  test("自定义模式支持单源搜索、上传裁切、本地草稿和直接导图", async ({ page }) => {
    let shareRequestCount = 0;
    page.on("request", (request) => {
      if (request.url().includes("/api/share")) {
        shareRequestCount += 1;
      }
    });

    await page.goto("/custom");
    await page.getByRole("button", { name: "我知道了" }).click();

    await expect(page.getByRole("button", { name: "保存页面" })).toHaveCount(0);
    await expect(page.getByRole("button", { name: "生成分享链接" })).toHaveCount(0);
    await expect(page.getByText("大家的构成")).toHaveCount(0);
    await expect(page.getByRole("button", { name: /^还差 9 部可生成$/ })).toBeEnabled();
    await expect(page.getByRole("button", { name: /^还差 9 部可生成$/ })).toHaveClass(/opacity-45/);

    await page.getByPlaceholder("输入你的昵称").fill("本地玩家");

    await page.getByLabel("选择第 1 格作品").click();
    await page.getByPlaceholder("搜索 Bangumi 条目").fill("bgm");
    await page.getByRole("button", { name: "搜索", exact: true }).click();
    await page.getByRole("button", { name: /Bangumi 作品/ }).click();
    await page.getByLabel("条目标题").fill("");
    await page.getByRole("button", { name: "填入第 1 格" }).click();
    await expect(page.getByText("已填入第 1 格")).toBeVisible();

    await page.getByLabel("选择第 1 格作品").click();
    await expect(page.getByPlaceholder("搜索 Bangumi 条目")).toBeVisible();
    await expect(page.getByLabel("条目标题")).toHaveValue("");
    await expect(page.getByText("原始名称：Bangumi 作品")).toBeVisible();
    await page.getByRole("button", { name: "关闭" }).click();

    await page.getByLabel("选择第 2 格作品").click();
    await page.getByRole("button", { name: "TMDB" }).click();
    await page.getByPlaceholder("搜索 TMDB 条目").fill("tmdb");
    await page.getByRole("button", { name: "搜索", exact: true }).click();
    await page.getByRole("button", { name: /搏击俱乐部/ }).click();
    await page.getByRole("button", { name: "填入第 2 格" }).click();
    await expect(page.getByText("已填入第 2 格")).toBeVisible();

    await page.getByLabel("选择第 3 格作品").click();
    await page.getByRole("button", { name: "上传" }).click();
    await page.locator('input[type="file"]').setInputFiles({
      name: "upload.png",
      mimeType: "image/png",
      buffer: Buffer.from(ONE_PIXEL_PNG_BASE64, "base64"),
    });
    await expect(page.getByRole("heading", { name: "裁切上传图片" })).toBeVisible();
    await page.getByRole("button", { name: "确认使用" }).click();
    await page.getByRole("button", { name: "填入第 3 格" }).click();
    await expect(page.getByText("已填入第 3 格")).toBeVisible();

    await expect(page.getByText("未命名条目")).toBeVisible();
    await expect(page.getByText("搏击俱乐部")).toBeVisible();
    await expect(page.getByText("upload")).toBeVisible();

    await page.reload();
    await expect(page.getByRole("heading", { name: "使用须知" })).toHaveCount(0);
    await expect(page.getByPlaceholder("输入你的昵称")).toHaveValue("本地玩家");
    await expect(page.getByText("3 / 9 已选择")).toBeVisible();
    await expect(page.getByText("未命名条目")).toBeVisible();
    await expect(page.getByText("搏击俱乐部")).toBeVisible();

    const partialGenerateButton = page.getByRole("button", { name: /^还差 6 部可生成$/ });
    await expect(partialGenerateButton).toBeEnabled();
    await expect(partialGenerateButton).toHaveClass(/opacity-45/);

    page.once("dialog", async (dialog) => {
      await dialog.accept();
    });
    await partialGenerateButton.click();
    await expect(page.getByRole("heading", { name: "生成图片" })).toBeVisible();
    await expect(page.getByAltText("自定义图片预览")).toBeVisible({ timeout: 15_000 });
    const customQrSwitch = page.getByRole("switch", { name: "附带分享链接" });
    const customShowNameSwitch = page.getByRole("switch", { name: "显示名称" });
    await expect(customQrSwitch).toHaveAttribute("aria-checked", "true");
    await expect(customShowNameSwitch).toHaveAttribute("aria-checked", "true");
    await expect(page.getByLabel("自定义标题")).toBeVisible();

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

    await customQrSwitch.click();
    await expect(customQrSwitch).toHaveAttribute("aria-checked", "false");
    await expect(page.getByLabel("自定义标题")).toHaveCount(0);
    await page.getByRole("button", { name: "保存图片" }).click();

    const exportInfoWithoutQr = await page.evaluate(() => {
      const g = window as typeof window & {
        __MY9_LAST_CUSTOM_EXPORT__?: { qrUrl?: string | null; height?: number; showNames?: boolean };
      };
      return g.__MY9_LAST_CUSTOM_EXPORT__ || null;
    });
    expect(exportInfoWithoutQr?.qrUrl).toBeNull();
    expect(exportInfoWithoutQr?.height).toBe(1440);
    expect(exportInfoWithoutQr?.showNames).toBeTruthy();

    await customQrSwitch.click();
    await expect(customQrSwitch).toHaveAttribute("aria-checked", "true");
    await expect(page.getByLabel("自定义标题")).toBeVisible();
    await page.getByLabel("自定义标题").fill("");
    await page.getByRole("button", { name: "保存图片" }).click();

    const exportInfo = await page.evaluate(() => {
      const g = window as typeof window & {
        __MY9_LAST_CUSTOM_EXPORT__?: { qrUrl?: string | null; showNames?: boolean; height?: number };
      };
      return g.__MY9_LAST_CUSTOM_EXPORT__ || null;
    });
    const downloadName = await page.evaluate(() => {
      const g = window as typeof window & { __MY9_LAST_DOWNLOAD_NAME__?: string };
      return g.__MY9_LAST_DOWNLOAD_NAME__ || "";
    });
    expect(exportInfo?.qrUrl?.endsWith("/custom")).toBeTruthy();
    expect(exportInfo?.showNames).toBeTruthy();
    expect(exportInfo?.height).toBe(1660);
    expect(downloadName).toBe("构成本地玩家的9部作品.png");
    expect(shareRequestCount).toBe(0);
  });
});
