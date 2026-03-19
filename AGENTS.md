# 仓库指南（My9）

本指南面向贡献者与自动化代理，目标是与当前代码库实践保持一致。

## 项目结构与模块组织
- `app/`：App Router 页面与 API 路由。
  - 首页：`/`（`app/page.tsx`，类型选择入口）
  - 填写页：`/[kind]`
  - 分享只读页：`/[kind]/s/[shareId]`
  - 趋势页：`/trends`
  - API：`app/api/*`
- `app/components/`：主业务组件（如 `My9V3App`、`v3/*`）。
- `components/`：跨页面复用组件（`layout/`、`share/`、`subject/`、`ui/`）。
- `lib/`：领域逻辑与工具（Bangumi 搜索、分享存储、`subject-kind` 等）。
- `tests/`：Playwright E2E 用例（当前为 `*.spec.ts`）。
- `docs/`：运维与排障文档（含分享存储 v2 操作手册、内容源接入指南）。
- `scripts/`：迁移/归档/校验脚本。
- `scripts/playwright-webserver.cjs`：E2E 专用构建与 3001 服务脚本。
- `screenshot/`：验收截图产物。

## 构建、开发与测试命令
- `npm install`：安装依赖（建议 Node 20.9+）。
- `npm run dev`：本地开发（默认 `http://localhost:3000`）。
- `npm run build`：生产构建。
- `npm start`：启动生产构建产物。
- `npm run cf:verify-access`：只读核验 Cloudflare account token、zone、Workers routes 与 R2 bucket 对齐情况。
- `npm run cf:build`：执行 OpenNext Cloudflare 构建（输出 `.open-next/`）。
- `npm run cf:build:test`：按 `wrangler.jsonc` 的 `env.test` 生成测试域部署产物。
- `npm run cf:preview`：先构建，再通过 Wrangler 本地预览 Worker。
- `npm run cf:deploy`：先构建，再部署到 Cloudflare Workers。
- `npm run cf:deploy:test`：构建并部署到 `my9test.shatranj.space` 对应的 Cloudflare Worker 环境。
- `npm run d1:migrate:local` / `npm run d1:migrate:remote`：应用生产 D1 migrations。
- `npm run d1:migrate:local:test` / `npm run d1:migrate:remote:test`：应用测试环境 D1 migrations。
- `npm run d1:rebuild-trends`：从 D1 事实表重建 kind 粒度趋势表。
- `npm run d1:cleanup-trends`：清理过旧日/小时趋势计数。
- `npm run lint`：运行 ESLint。
- `npm run test:e2e`：运行 Playwright E2E。

说明：
- 仓库以 `npm` + `package-lock.json` 为准，避免切换包管理器引发锁文件噪音。

## Agent 端口与测试约定（强约束）
- `3000` 端口保留给开发者手动调试，自动化代理不得占用、停止或清理该端口进程。
- 自动化测试统一使用 `3001`。
- Playwright 通过 `scripts/playwright-webserver.cjs` 启动：
  - 使用独立构建目录 `.next-e2e`
  - 启动端口 `3001`
- 不要删除或覆盖开发者本地使用的 `.next`。

## 代码风格与实现约定
- 语言：TypeScript（`strict`），路径别名 `@/*`。
- 样式：Tailwind CSS；使用 `cn(...)` 合并类名。
- 组件与文件命名遵循现有风格（PascalCase 组件，`components/ui` 下文件名小写）。
- 优先做最小改动，保持当前交互与文案风格一致。

## 测试实践（当前状态）
- 本仓库已配置 Playwright。
- 新增/修改交互时，优先补充或更新 `tests/v3-interaction.spec.ts`。
- 涉及布局问题时，可补截图验证（保存到 `screenshot/`）。

## 环境变量与外部服务
- 在 `.env.local`（勿提交）中配置：
  - `CLOUDFLARE_API_TOKEN`
  - `CLOUDFLARE_ACCOUNT_ID`
  - `BANGUMI_ACCESS_TOKEN`
  - `BANGUMI_USER_AGENT`
  - 可选：`MY9_DB_WRANGLER_ENV`（本地 Node 侧通过 `getPlatformProxy()` 选择 `wrangler` 环境，例：`test`）
  - 可选：`MY9_TRENDS_24H_SOURCE=day|hour`（默认 `day`；小时窗口初始化完成后再切 `hour`）
  - 可选：`MY9_ANALYTICS_ACCOUNT_ID`（未设置时可回退到 `CLOUDFLARE_ACCOUNT_ID`）
  - 可选：`MY9_ANALYTICS_API_TOKEN`（Workers Analytics Engine SQL 读权限；未设置时 `cf:sync-secrets` 可临时回退到 `CLOUDFLARE_API_TOKEN`）
  - 可选：`MY9_TREND_CLEANUP_DAYS`（默认 `190`，勿低于 `180`，否则影响 `180d` 趋势）
  - 可选：`NEXT_PUBLIC_GA_ID`
  - 可选：`NEXT_PUBLIC_SITE_URL`（测试域部署时需设为 `https://my9test.shatranj.space`）
  - 可选：`SITE_URL`（服务端覆盖；未设置时回退到 `NEXT_PUBLIC_SITE_URL`）
- Cloudflare Workers 生产部署还需在 `wrangler.jsonc` 中绑定：
  - `MY9_DB`（D1）
  - `MY9_SHARE_VIEW_ANALYTICS`（Workers Analytics Engine）
  - `ASSETS`（静态资源）
- Cloudflare 部署认证统一使用 account token，不再使用全局 `CLOUDFLARE_API_KEY`。
- 分享图封面当前通过 `wsrv.nl` 在前端拉取并绘制；修改该链路时需评估跨域与流量成本影响。
- 严禁提交任何真实密钥（Cloudflare/R2/CRON）。若误泄露，必须立即旋转并更新环境变量。
- OpenNext Cloudflare 当前不建议把原生 Windows PowerShell 作为正式构建/部署环境；发布前至少在 Linux CI 或 WSL2 上验证一次 `npm run cf:build`。

## 分享存储 v2 运维
- 当前运行时为 D1-only；分享写入、读取、趋势聚合、浏览量总表、cron 清理都只依赖 D1。
- D1 schema 通过 `migrations/d1` 维护；新库先执行 `npm run d1:migrate:local` / `npm run d1:migrate:remote`。
- 趋势重建使用 `npm run d1:rebuild-trends`，输入事实表为 `my9_share_subject_slot_v1`，输出为 `my9_trend_subject_kind_*_v3`。
- Cloudflare Workers Cron 现在有两条：
  - `30 * * * *`：按小时增量 rollup `my9_share_subject_slot_v1` 到 `my9_trend_subject_kind_*_v3`
  - `5 16 * * *`：北京时间 `00:05` 跑日常维护
- daily cron 会清理 `MY9_TREND_CLEANUP_DAYS` 之前的日/小时趋势数据，并把截至前一个已闭合北京时间自然日的分享页访问量累计回写到 `my9_share_view_total_v1`（每个 `share_id` 一行）。
- test 环境不跑 cron；需要人工执行部署和运维脚本验证。

## 提交与 PR 建议
- 提交信息简短、祈使/现在时，聚焦单一改动。
- PR 说明建议包含：改动范围、复现/验证步骤、必要截图、环境变量变更。

## 新增内容源贡献（必读）
- 以 iTunes 接入（`verify/pr-6-merge`）为样本，新增内容源时统一遵循 `docs/content-source-contribution.md`。
- 涉及新增/切换内容源的 PR，说明中至少要覆盖：kind 与 source 路由关系、搜索实现、分享存储兼容、前端外链与归因、测试结果。
