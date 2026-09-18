import type { ShareGame, ShareSelectionStats } from "@/lib/share/types";
import type { RelatedSelectionPreviews } from "@/lib/share/related-selections";

const UPDATED_AT = Date.parse("2026-09-18T01:00:00Z");

const CATALOG = [
  { id: "12596", name: "Minecraft", localizedName: "我的世界", releaseYear: 2011 },
  { id: "62229", name: "ゼルダの伝説 ブレス オブ ザ ワイルド", localizedName: "塞尔达传说 旷野之息", releaseYear: 2017 },
  { id: "172808", name: "Stardew Valley", localizedName: "星露谷物语", releaseYear: 2016 },
  { id: "284100", name: "ELDEN RING", localizedName: "艾尔登法环", releaseYear: 2022 },
  { id: "195573", name: "Red Dead Redemption 2", localizedName: "荒野大镖客：救赎 2", releaseYear: 2018 },
  { id: "17852", name: "Terraria", localizedName: "泰拉瑞亚", releaseYear: 2011 },
  { id: "225878", name: "明日方舟", localizedName: "明日方舟", releaseYear: 2019 },
  { id: "18011", name: "League of Legends", localizedName: "英雄联盟", releaseYear: 2009 },
  { id: "284157", name: "原神", localizedName: "原神", releaseYear: 2020 },
  { id: "1859", name: "Plants vs. Zombies", localizedName: "植物大战僵尸", releaseYear: 2009 },
  { id: "273991", name: "Apex Legends", localizedName: "Apex 英雄", releaseYear: 2019 },
  { id: "360097", name: "崩坏：星穹铁道", localizedName: "崩坏：星穹铁道", releaseYear: 2023 },
  { id: "194792", name: "王者荣耀", localizedName: "王者荣耀", releaseYear: 2015 },
];

const COLORS = ["#37796a", "#477f96", "#aa774d", "#7d713c", "#994b46", "#657944", "#576582", "#386e82", "#8770a2"];
const COUNTS = [328, 286, 241, 204, 177, 153, 129, 106, 88, 64];
const COMMENTS = [
  "从第一间小木屋开始，把想象里的世界一点点搭出来。",
  "路边的风景，总能让我暂时忘记原本要去的地方。",
  "忙完一天之后，回农场看看。",
  "在一次次迷路和重来之后，终于走到了这里。",
  "有时只是骑着马，在地图上漫无目的地走。",
  "再往地下挖一点，说不定就有新的发现。",
  "音乐、角色，还有那些一直记得的故事。",
  "留在记忆里的，是和朋友一起玩的晚上。",
  "想去看看山那边的风景。",
];

function createPlaceholderCover(title: string, color: string, index: number) {
  const lines = [title.slice(0, 8), title.slice(8)];
  const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="300" height="400" viewBox="0 0 300 400">
    <rect width="300" height="400" fill="${color}"/>
    <circle cx="250" cy="70" r="130" fill="white" fill-opacity="0.08"/>
    <path d="M-40 290 145 85 350 320V400H-40Z" fill="black" fill-opacity="0.09"/>
    <path d="M-30 360 100 230 320 380V400H-30Z" fill="white" fill-opacity="0.08"/>
    <text x="26" y="56" fill="white" fill-opacity="0.55" font-family="sans-serif" font-size="13" letter-spacing="3">MY9 / ${(index + 1).toString().padStart(2, "0")}</text>
    <text x="24" y="226" fill="white" fill-opacity="0.14" font-family="sans-serif" font-size="128" font-weight="700">${(index + 1).toString().padStart(2, "0")}</text>
    <text x="26" y="316" fill="white" font-family="sans-serif" font-size="25" font-weight="600">${lines[0]}</text>
    <text x="26" y="352" fill="white" font-family="sans-serif" font-size="25" font-weight="600">${lines[1]}</text>
    <text x="26" y="384" fill="white" fill-opacity="0.55" font-family="sans-serif" font-size="10" letter-spacing="2">LOCAL PREVIEW</text>
  </svg>`;
  return `data:image/svg+xml;charset=utf-8,${encodeURIComponent(svg)}`;
}

export function createResonancePreview(empty = false): {
  games: ShareGame[];
  selectionStats: ShareSelectionStats;
  relatedSelectionPreviews: RelatedSelectionPreviews;
} {
  const games = CATALOG.slice(0, 9).map((game, index) => ({
    ...game,
    cover: createPlaceholderCover(game.localizedName, COLORS[index], index),
    comment: COMMENTS[index],
  }));
  const relatedSelectionPreviews: RelatedSelectionPreviews = {};

  for (const [index, game] of games.entries()) {
    const others = CATALOG.filter((item) => item.id !== game.id);
    const rotated = [...others.slice(index), ...others.slice(0, index)];
    relatedSelectionPreviews[game.id] = {
      delayMs: 900,
      result: {
        items: empty ? [] : rotated.slice(0, 10).map((item, rank) => ({
          subjectId: item.id,
          name: item.localizedName,
          count: COUNTS[rank] - index * 3,
        })),
        matchedShares: empty ? 0 : 1000,
        kindShares: 319940,
        updatedAt: UPDATED_AT,
      },
    };
  }

  return {
    games,
    selectionStats: {
      counts: Object.fromEntries(games.map((game, index) => [game.id, empty ? 0 : 48215 - index * 4129])),
      updatedAt: UPDATED_AT,
    },
    relatedSelectionPreviews,
  };
}
