# Cloudflare Auto Deploy

当前仓库已配置基于 GitHub Actions 的自动部署：

- 推送到 `main`：执行生产部署，目标域名为 `my9.shatranj.space`
- 推送到 `open-next`：执行测试部署，目标域名为 `my9test.shatranj.space`

工作流文件：

- `.github/workflows/deploy-production.yml`
- `.github/workflows/deploy-test.yml`

## GitHub 仓库需配置的 Secrets

在 GitHub 仓库 `Settings > Secrets and variables > Actions` 中添加：

- `CLOUDFLARE_ACCOUNT_ID`
- `CLOUDFLARE_API_TOKEN`

说明：

- 当前运行时 secrets 已存储在 Cloudflare Worker 中，GitHub Actions 只负责构建和部署。
- 生产与测试部署共用同一组 Cloudflare 凭据，但分别调用不同的 npm script。

## 部署命令

- 生产：`npm run cf:deploy`
- 测试：`npm run cf:deploy:test`

## 关于 Cloudflare 原生 Git 集成

Cloudflare Workers Builds 也支持直接连接 GitHub / GitLab，在 Cloudflare Dashboard 内完成“推送即构建部署”。

本仓库当前选择 GitHub Actions 的原因：

- 配置可版本化，随仓库一起管理
- 继续沿用现有 `CLOUDFLARE_API_TOKEN` / `CLOUDFLARE_ACCOUNT_ID`
- 不依赖额外的 Dashboard 手工 Git 绑定流程
