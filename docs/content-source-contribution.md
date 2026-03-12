# 新增内容源贡献指南

本文基于 `verify/pr-6-merge` 相对 `main`（merge-base: `7839ee162dd8d76685288a2a3a92fe310c80c8b6`）新增 iTunes 支持的实际改动，总结“新增内容源”时应遵循的标准流程与检查清单。

## 1. 这次 iTunes 改动覆盖了哪些层

| 层 | 文件（示例） | 本次变更目的 |
| --- | --- | --- |
| 路由分发 | `app/api/subjects/search/route.ts` | 为 `song`/`album` 分发到 iTunes handler |
| 数据源实现 | `lib/itunes/search.ts` | iTunes API 请求、结果映射、`SubjectSearchResponse` 构造 |
| API 中间层 | `lib/itunes/route.ts` | 查询规范化、内存缓存、限流、错误与缓存头处理 |
| kind 元数据 | `lib/subject-kind.ts` | 新增 kind、统一 `selectionUnit`、标题生成函数 |
| 搜索返回类型 | `lib/share/types.ts` | `source` 联合类型新增 `"itunes"` |
| 分享入库与恢复 | `app/api/share/route.ts`、`lib/share/compact.ts` | 透传并压缩 `storeUrls`（外链） |
| 主界面与只读页 | `app/components/My9V3App.tsx`、`My9Readonly*` | 标题/文案单位适配、解析新 `source` |
| 条目外链与来源标识 | `SelectedGamesList.tsx`、`ReadonlySelectedGamesList.tsx`、`TrendsClientPage.tsx` | song/album 跳转 Apple Music，显示来源标签 |
| 页脚归因与图标 | `components/layout/SiteFooter.tsx`、`components/subject/SubjectKindIcon.tsx` | 新来源归因展示、kind 图标 |
| 分享图文案 | `utils/image/exportShareImage.ts` | 标题从“九部”切到按单位动态生成 |

## 2. 新增内容源标准流程（按顺序）

### 2.1 确定 kind 与 source 的路由关系

1. 在 `lib/subject-kind.ts` 增加新 kind（及其 `label`/`longLabel`/`selectionUnit` 等元信息）。
2. 在 `app/api/subjects/search/route.ts` 增加 kind 到 handler 的分发规则。

说明：当前仓库采用“按 kind 决定 source”的单路由策略，同一 kind 默认只走一个内容源。

### 2.2 实现 source 的搜索适配层

1. 新建 `lib/<source>/search.ts`：
   - 请求第三方 API；
   - 将原始结果映射为 `ShareSubject`；
   - 构造 `SubjectSearchResponse`（`source` 必须是新字面量）。
2. 新建 `lib/<source>/route.ts`：
   - 统一使用 `normalizeSearchQuery`；
   - 补齐缓存头、内存缓存、inflight 去重、限流；
   - 429/500 响应统一返回 `ok: false` + `error`。

### 2.3 补全类型与持久化通路

1. 在 `lib/share/types.ts` 扩展 `SubjectSearchResponse["source"]` 联合类型。
2. 如需保留来源外链（如 iTunes `trackViewUrl`）：
   - `app/api/share/route.ts`：入站 sanitize（仅允许 `http/https`）；
   - `lib/share/compact.ts`：压缩存储与反序列化都要透传该字段。

### 2.4 完成前端展示与跳转闭环

1. 搜索页：接收并识别新 `source`。
2. 列表/只读页/趋势页：补充 `subjectLink` 与来源 label。
3. 页脚归因：新增来源品牌展示（如 Apple Music / TMDB / Bangumi）。
4. kind 图标：`components/subject/SubjectKindIcon.tsx` 增加分支。

### 2.5 统一文案，不写死“九部”

1. 在 `lib/subject-kind.ts` 维护 `selectionUnit`。
2. 标题统一经 `getSubjectKindShareTitle(kind)` 生成。
3. 涉及标题的页面与分享图全部改为动态单位。

## 3. 变更清单（PR 自检）

- [ ] 新 kind 已加入 `SubjectKind` 联合与 `SUBJECT_KIND_ORDER`
- [ ] `app/api/subjects/search/route.ts` 已分发到新 source
- [ ] 新 source 的 `search.ts` 与 `route.ts` 均已实现
- [ ] `SubjectSearchResponse.source` 已扩展并在前端消费
- [ ] 如含外链字段，已完成 sanitize + compact 存储透传
- [ ] 填写页、只读页、趋势页、页脚来源、图标已更新
- [ ] 标题/提示文案无“九部”硬编码遗留
- [ ] `npm run lint` 通过
- [ ] 新增/更新 `tests/v3-interaction.spec.ts` 覆盖关键流程

## 4. 这次 iTunes 变更暴露的高频遗漏点

1. 只改搜索接口，不改分享存储：会导致分享后来源外链丢失。
2. 只改 kind，不改标题生成：会出现“九部单曲/九部专辑”文案错误。
3. 只改列表页，不改趋势页与页脚：来源跳转和归因不一致。
4. 忘记扩展 `source` 联合类型：前端可能把新来源回退成旧来源处理。

## 5. 提交与评审建议

1. PR 描述建议按“路由分发 / source 实现 / 存储兼容 / UI 展示 / 测试结果”分段。
2. 如新增第三方凭据，必须补充到 `.env.local` 说明，且不得提交真实密钥。
3. 涉及外链落库时，需明确 URL 白名单策略（至少限制协议为 `http/https`）。
