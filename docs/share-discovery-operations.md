# 查找构成：数据接入与运维

填写页和分享页的「大家的构成」旁提供橘黄色「查找构成」。默认读取全站浏览量前 20 的每日快照；名称搜索采用去首尾空白后的完整名称、数据库 BINARY 比较，按创建时间和分享 ID 降序，每页 20 条，使用范围游标继续加载。列表为 CSS 类型色块，不读取作品/封面。名称相同不代表同一个人，未署名构成仅能通过热门榜或原分享链接访问。

UI 复用填写页的搜索栏、反馈和固定高度弹窗。只有打开/提交/翻页才请求；同一组件内已加载查询复用内存。详情链接禁用预取。开发预览仍可在 `/game/find-preview` 查看，生产返回 404。

## 数据路径

- API：`GET /api/shares/discover`，参数 `query`、`cursor`。只读，无结果为 200；无效名称/游标为 400；数据库或快照未就绪为 503。游标携带名称、最后一条的创建时间和分享 ID，不能跨名称复用。
- `0005_share_discovery.sql` 建立 `(creator_name, created_at DESC, share_id DESC)` 部分索引，仅索引非空署名。它不更新主表内容，不触发共现事件。查询显式指定索引，缺迁移时失败，不退化为历史全表扫描。
- 默认榜只读 `my9_system_checkpoint_v1` 中 `system:share-discovery:popular:v1` 一行。快照含版本、完整 20 条列表和浏览量截止日期。
- 既有北京时间 00:05 的每日浏览量 rollup 成功后，调用 `refreshShareDiscoverySnapshot()`。每类取有效分享前 20，再选全站前 20；单条 INSERT SELECT 原子发布结果和 rollup 水位。失败保留前一份快照，错误进入既有任务日志，不新增 cron。
- 新功能没有浏览量曝光埋点，不新增 Analytics Engine 查询、外部搜索、R2、KV、Queues 或 DO。

## 首次发布

在上主站代码前，使用临时 SQL 写凭据完成索引迁移和第一份快照。脚本从 `.env.local`/`.env` 读取既有只读 `MY9_SQL_API_TOKEN` 和临时 `MY9_SQL_MIGRATION_TOKEN`，不打印凭据。命令末尾是仓库外的绝对账本目录，记录每条 SQL 的 D1 meta。

```text
node scripts/bootstrap-share-discovery.mjs migrate production ABSOLUTE_LEDGER_DIRECTORY
node scripts/bootstrap-share-discovery.mjs refresh production ABSOLUTE_LEDGER_DIRECTORY
node scripts/bootstrap-share-discovery.mjs inspect production ABSOLUTE_LEDGER_DIRECTORY
```

测试环境将 `production` 换成 `test`，不在生产试跑测试数据。初始化护栏为累计 195 万读或 99 万写时停止发起新操作，给最后一次调用留余量；不是平台硬限制，也不包括同库其他业务流量。响应不确定时先检查迁移和索引，不盲目重建。索引定义与登记不一致会停止，要求先核查。

`migrate` 只处理 0005，不执行其他待迁移项。`refresh` 需要已有成功的浏览量汇总水位，否则不会写快照。迁移执行一次；线上每日更新由既有任务完成，脚本用于首次初始化或明确需要的人工恢复。任务结束从本地移除临时写 token；只读 token 保留。

## 2026-09-18 生产初始化测量

本次用户已批准按[成本报告](share-discovery-production-cost-2026-09-18.md)提交和部署，初始化预算 200 万读取、100 万写入、50–100 MB 持久空间。

| 操作 | 读取 | 写入 | 说明 |
| --- | ---: | ---: | --- |
| 建索引 | 1,668,660 | 693,072 | SQL 约 11.68 秒；包含 D1 内部计量 |
| migration 登记 | 1 | 3 | 与建索引同一次 D1 请求 |
| 首份热门快照 | 1,382 | 2 | 20 条，JSON 2,343 字节（含版本） |
| 搜索同名多页：首屏 | 55 | 0 | 取 21 条，显示 20 条 |
| 搜索同名多页：下一页 | 55 | 0 | 无第一页已展示条目重叠 |
| 搜索「阿菜」 | 15 | 0 | 返回 6 条 |
| 无结果名称 | 1 | 0 | 不扫描其他署名 |
| 搜索另读浏览量水位 | 1 | 0 | 每次搜索再加 1 行 |
| 默认榜读取 | 1 | 0 | 不在用户请求中刷新 |

生产执行计划确认游标使用 `(creator_name=? AND (created_at,share_id)<(?,?))` 范围索引，无历史全表扫描。上述完整搜索样本分别为 56、56、16、2 行读取，均低于 150 行预算。生产空间从 1,981,304,832 增至 2,009,788,416 字节，净增 **28,483,584 字节，约 28.48 MB**；初始快照使用既有空闲页未进一步增加文件。空间差额含同期正常业务变化，不声称精确拆分到单独索引页。

DDL 读取大于“只扫一次主表”的理想模型，仍在 200 万预算内，因此核验使用有界查询，不再全索引 COUNT。将来正常增长仍按每月 1 万额外写入、后台 5 万读取以及每次默认榜 5 行/搜索 150 行的预算跟踪，实际值参考 meta。CPU/日志尚需上线后稳定观测，不能用 SQL 耗时代替 Worker CPU。

原始操作账本和只读查询计量在仓库外 `C:/Users/旻/.codex/artifacts/my9-share-discovery-release-20260918`。初始化后通过 Linux CI 的 ESLint、OpenNext 构建再部署；HTTP 核验与 SQL 计量用于数据接入验收，不代替浏览器交互测试。

## 恢复

若快照刷新失败，先修复浏览量汇总或数据异常，再运行 `refresh`；缺索引时不要绕过 INDEXED BY 改成全表查找。回滚主站版本不会删除索引或快照，不必紧急 DROP INDEX。无独立定时器需要清理，旧主站不会使用新增快照。
