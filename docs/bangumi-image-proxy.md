# Bangumi 图片代理迁移记录

2026-09-25，My9 的常规 Bangumi 封面地址改为 `https://bgm-pic.shatranj.space/{lain|img}/pic/...`。对应的 Nginx 配置保存在 [`deploy/nginx/bangumi-images.conf`](../deploy/nginx/bangumi-images.conf)，参考 [Yuri-NagaSaki/bangumi-proxy](https://github.com/Yuri-NagaSaki/bangumi-proxy) 的图片代理配置。

配置仅接受 `/pic/` 图片路径的 GET/HEAD 请求；带查询参数或其他路径的 Bangumi 图片仍使用原有 `/api/image/bgm` 路径。图片请求必须带有 `shatranj.space` 或其子域的 Referer；带 Origin 的请求也必须来自该域。浏览器可缓存图片 30 天，Cloudflare 边缘不缓存，以便每次请求都执行来源检查。代理未配置本地图片缓存。

旧图片域名 `bgm-img.shatranj.space` 由 Worker 路由返回 `403`，防止历史边缘缓存继续公开图片。

部署时已核验：有效封面返回 `200` 和 `image/jpeg`，不合规来源返回 `403`，无效路径返回 `404`；原有 `/api/image/bgm` 路径仍可用。

回退网站侧地址时，可将 `lib/image-proxy.ts` 中的常规封面 URL 重新指向 `/api/image/bgm?url=...`。更新 Nginx 配置前运行 `nginx -t`，通过后再重新加载。
