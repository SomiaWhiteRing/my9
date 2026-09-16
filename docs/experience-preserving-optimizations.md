# 保留体验的性能优化（2026-09-17）

本轮复用原有业务逻辑，优先减少搜索、分享 JSON 的 Next 路由与适配成本。不调整内容刷新频率，不增加 HTML 缓存，不改变搜索重试或引入过期数据回退。

## 改动

- Worker 对精确路径 `/api/subjects/search` 和 `/api/share` 的 GET 直接调用共享处理器。Next API 路由调用同一实现；其他方法和尾斜杠等路径仍由 Next 处理。JSON、状态、缓存头、限流顺序与配额保持原实现。保留 OpenNext 的环境变量初始化和请求上下文；Bangumi/TMDB 密钥改在请求时读取，避免静态导入早于绑定初始化。
- 回补 OpenNext AWS #1189 的首页缓存修复：manifest 匹配 `/`，缓存资产及标签查询使用 `/index`。构建脚本在运行 OpenNext 之前自动应用，限定 core 3.9.16 并校验替换片段；重复执行不重复修改。升级依赖时须审核或移除补丁。使用仓库的 `npm run cf:build` / `cf:build:test`，不要绕过包装脚本直接构建。没有增加缓存 TTL。
- 趋势结果和样本缓存过期时直接返回 miss，不再 DELETE；成功刷新沿用原 upsert。避免一次多余写入，也避免旧读取删除并发写入的新缓存。不会返回过期内容，原缓存键空间有界。
- 分享页和 HEAD 共用参数、规范化、存储读取和 kind 重定向判断。普通、成功的分享 HEAD 省去正文渲染；无效路径、读取失败/缺失、RSC、预览与编码路径继续交给 Next。HEAD 不计访问量；不构造需正文生成的 Content-Length、ETag 或样式预载 Link。GET 的 SEO 和预载不变。
- 赞赏内容首次打开时才构造，仍在原客户端包中，不增加点击后的代码下载。首次打开后保留 Radix 组件，以维持关闭动画和焦点恢复；数量仍取构建快照/原配置，首次实际需要时按原 `zh-CN` 规则格式化并复用。
- 分享页的 kind 重定向移到存储异常捕获之外，修复 Next 重定向异常被误当作存储失败的问题。

## 本轮未采用

趋势 COUNT/MIN/MAX 跨视图持久缓存暂缓。现有样本缓存键只有 kind/period，既没有事实数据版本，也没有精确滚动窗口边界；仅靠 rollup 检查点无法覆盖补写/维护导致的事实变化。不能用延长 TTL 或冻结时间窗口代替一致性保证。

趋势 API 的原生入口是后续扩展，本轮先覆盖已有消融证据的搜索和分享读取。框架初始化时点、Smart Placement、依赖整体升级、HTML ISR、图像组件替换和轻量 SSR 架构均未纳入。

## 验证及上线观察

使用既有 lint、TypeScript 检查及隔离 WSL Cloudflare 构建；不新增测试代码，不运行浏览器/UI 测试。部署前后应分搜索/分享 JSON、HTML、HEAD、首页观察状态分布、CPU 分位数、每千请求 CPU 与 TTFB。原生 API 保留现有限流；本地无外部服务的 CPU 对照不能验证真实上游恢复情况。

本次验证结果：

- `npm run lint`、`tsc --noEmit --incremental false` 通过。
- WSL Ubuntu、Node 22.20.0，干净 `npm ci` 后 `npm run cf:build` 和 `wrangler deploy --dry-run` 通过。验证目录不含生产密钥，构建沿用现有分享数量快照；未部署。
- 用已有 `profile/bench.mjs` 运行正式 Worker 产物：分享 JSON 200 次、搜索未命中 200 次、搜索命中 200 次均为 200，三类 JSON 与原基线逐字节一致。分享 HTML 150 次、RSC 150 次、HEAD 50 次均为 200，HEAD 无正文；分享 `<main>` 与原基线逐字节一致。
- 本轮本地进程 CPU 均值：分享 JSON 6.17ms、搜索未命中 3.75ms、搜索命中 2.19ms、分享 HTML 24.79ms、RSC 14.38ms、HEAD 5.31ms。只是单轮本地验证值，不是线上预测或同期收益百分比。
- 本地 `populateCache local` 后，首页 HTML/RSC 均为 `x-opennext-cache: HIT`，分别与本次构建的 `index.html`/`index.rsc` 逐字节一致。直接 `wrangler dev` 不会自动填充 OpenNext 静态缓存；生产既有 `opennextjs-cloudflare deploy` 会填充。
- 现有 Wrangler 的本地 runtime 提示兼容日期从 2026-03-14 回退到 2026-03-12；这次未做框架依赖升级。未覆盖 UI 交互验收、所有上游错误/限流分支或生产冷启动分布。

线上分享 HTML 平均 245ms 的函数级归因仍未完成。原生 API 优化主要影响 API；首页、HEAD、弹窗修复不能用来声称已解释或解决这部分 HTML CPU。
