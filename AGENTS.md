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
- `npm install`：安装依赖（建议 Node 18+）。
- `npm run dev`：本地开发（默认 `http://localhost:3000`）。
- `npm run build`：生产构建。
- `npm start`：启动生产构建产物。
- `npm run lint`：运行 ESLint。
- `npm run test:e2e`：运行 Playwright E2E。
- `node scripts/migrate-shares-v1-to-v2.mjs`：将 `my9_shares_v1` 迁移到 v2 存储模型（支持 checkpoint）。
- `node scripts/verify-shares-v2-migration.mjs`：校验迁移覆盖率（`missing_count`/`orphan_alias_count`）。
- `node scripts/rebuild-trend-hour-window.mjs`：用现有分享数据重建“昨天 00:00（北京时间）到当前”的小时粒度趋势窗口（建议连续执行两次）。
- `node scripts/archive-shares-cold.mjs`：归档 30 天前热数据到 R2，并清理过旧日/小时粒度趋势计数。

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
  - `BANGUMI_ACCESS_TOKEN`
  - `BANGUMI_USER_AGENT`
  - `NEON_DATABASE_PGHOST_UNPOOLED`（或 `NEON_DATABASE_PGHOST`）
  - `NEON_DATABASE_PGUSER`
  - `NEON_DATABASE_PGPASSWORD`（或 `NEON_DATABASE_POSTGRES_PASSWORD`）
  - `NEON_DATABASE_PGDATABASE`（或 `NEON_DATABASE_POSTGRES_DATABASE`）
  - 可选：`NEON_DATABASE_PGPORT`、`NEON_DATABASE_PGSSLMODE`（默认 `require`）
  - 可选：`NEXT_PUBLIC_ENABLE_VERCEL_ANALYTICS=1`（默认关闭，避免额外请求）
  - 可选：`NEXT_PUBLIC_ENABLE_VERCEL_SPEED_INSIGHTS=1`（默认关闭，避免额外请求）
  - 生产环境默认禁用内存 fallback（数据库异常会直接报错）；可用 `MY9_ALLOW_MEMORY_FALLBACK=1` 临时放开
  - 可选：`MY9_ENABLE_V1_FALLBACK=0`（默认开启 v1 读取兜底；迁移稳定后再关闭）
  - 可选：`MY9_TRENDS_24H_SOURCE=day|hour`（默认 `day`；小时窗口初始化完成后再切 `hour`）
  - `R2_ENDPOINT`、`R2_BUCKET`、`R2_ACCESS_KEY_ID`、`R2_SECRET_ACCESS_KEY`
  - 可选：`R2_REGION=auto`
  - `CRON_SECRET`（生产环境建议必配，用于保护 `/api/cron/archive`）
  - 可选：`MY9_ARCHIVE_OLDER_THAN_DAYS`（默认 `30`）
  - 可选：`MY9_ARCHIVE_BATCH_SIZE`（默认 `500`）
  - 可选：`MY9_ARCHIVE_CLEANUP_TREND_DAYS`（默认 `190`，勿低于 `180`，否则影响 `180d` 趋势）
- 分享图封面当前通过 `wsrv.nl` 在前端拉取并绘制；修改该链路时需评估跨域与流量成本影响。
- 严禁提交任何真实密钥（Neon/R2/CRON）。若误泄露，必须立即旋转并更新环境变量。

## 分享存储 v2 运维
- 迁移脚本默认读取 `my9_shares_v1`，并写入 `my9_share_registry_v2` / `my9_share_alias_v1` / `my9_subject_dim_v1` / `my9_trend_subject_*`。
- 迁移完成后先执行 `node scripts/verify-shares-v2-migration.mjs`；仅当 `missing_count=0` 且 `orphan_alias_count=0` 才允许考虑关闭 v1 兜底。
- 日常归档通过 `app/api/cron/archive` 触发，调度配置在 `vercel.json`（当前 `5 16 * * *`，即北京时间 `00:05`，每天一次）。
- 生产切换顺序：`v2 优先 + v1 兜底` -> 全量迁移与校验 -> 关闭兜底 -> 稳定观察后再删除 v1 表。

## 提交与 PR 建议
- 提交信息简短、祈使/现在时，聚焦单一改动。
- PR 说明建议包含：改动范围、复现/验证步骤、必要截图、环境变量变更。

## 新增内容源贡献（必读）
- 以 iTunes 接入（`verify/pr-6-merge`）为样本，新增内容源时统一遵循 `docs/content-source-contribution.md`。
- 涉及新增/切换内容源的 PR，说明中至少要覆盖：kind 与 source 路由关系、搜索实现、分享存储兼容、前端外链与归因、测试结果。
