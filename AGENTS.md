# 仓库指南

本指南帮助贡献者与自动化代理在 My9（Next.js）项目中保持一致协作。

## 项目结构与模块组织
- `app/` App Router 入口、布局、页面与 API 路由（`app/api/<name>/route.ts`）。包含 `app/[locale]/`（i18n）、以及 `app/utils`、`app/hooks`、`app/components` 等功能域代码。
- `components/` 共享 UI 组件；基础组件位于 `components/ui/`（如 `button.tsx`、`dialog.tsx`）。
- `lib/` 跨领域工具（如 `lib/utils.ts` 提供用于合并 Tailwind 类名的 `cn`）。
- `messages/` 预留为本地化资源；`screenshot/` 存放示例图片。

## 构建、测试与开发命令
- `npm install` 安装依赖（建议 Node 18+）。
- `npm run dev` 启动本地开发服务 `http://localhost:3000`。
- `npm run build` 使用 Next.js 进行生产构建。
- `npm start` 启动生产构建产物。
- `npm run lint` 运行 ESLint（Next.js 预设），可加 `-- --fix` 自动修复。
注：为保持一致性，请优先使用 `npm`（已存在 `package-lock.json`），避免因包管理器切换产生的锁文件变动。

## 代码风格与命名规范
- 语言：TypeScript，启用 `strict`；使用路径别名 `@/*`。
- 格式：2 空格缩进，使用分号；通过 ESLint 保持一致。
- React：组件导出使用 PascalCase；hooks 以 `use*` 开头；`components/ui` 下文件名小写，导出为 PascalCase。
- 样式：Tailwind CSS；使用 `lib/utils` 中的 `cn(...)` 组合类名。
- 路由：API 位于 `app/api/<name>/route.ts`。

## 测试指南
当前未配置测试框架。若新增测试：
- 组件优先使用 Jest + Testing Library，端到端使用 Playwright。
- 文件命名：靠近源码或 `__tests__/` 中的 `*.test.ts(x)`。
- 确保可本地运行并记录额外配置。

## Agent 端口约定
- `3000` 端口保留给开发者本地手动测试，自动化代理不得占用、停止或清理该端口相关进程。
- 自动化代理运行 Playwright / E2E 时统一使用 `3001` 端口。
- 当需要本地启动服务用于 agent 校验时，显式使用 `-p 3001`（例如 `npm start -- -p 3001`）。
- 自动化代理使用独立构建目录 `.next-e2e` 进行测试，禁止删除或覆盖开发者本地使用的 `.next` 目录。

## 提交与合并请求
- 提交信息：简短、祈使/现在时；中英文皆可。例如：“添加水印与缓存机制”“Translate HTML entity in names”。关联议题请引用 `#123`。
- PR：提供清晰描述、UI 变更截图、复现步骤及涉及的环境变量；聚焦单一改动，避免无关重构。

## 安全与配置提示
- 在 `.env.local`（勿提交）中配置：`STEAMGRIDDB_API_KEY`、`BANGUMI_ACCESS_TOKEN`、`BANGUMI_USER_AGENT`。
- 避免输出敏感信息日志。远程图片域名在 `next.config.js` 的 `images.remotePatterns` 中配置。
