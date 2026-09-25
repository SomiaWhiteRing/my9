# Bangumi 图片代理迁移记录

2026-09-25，My9 的常规 Bangumi 封面地址改为 `https://bgm-img.shatranj.space/{lain|img}/pic/...`。对应的 Nginx 配置保存在 [`deploy/nginx/bangumi-images.conf`](../deploy/nginx/bangumi-images.conf)，参考 [Yuri-NagaSaki/bangumi-proxy](https://github.com/Yuri-NagaSaki/bangumi-proxy) 的图片代理配置。

配置仅接受 `/pic/` 图片路径的 GET/HEAD 请求；带查询参数或其他路径的 Bangumi 图片仍使用原有 `/api/image/bgm` 路径。图片响应设置 30 天缓存，并允许分享图导出所需的跨域读取。代理未配置本地图片缓存。

部署时已核验：有效封面返回 `200` 和 `image/jpeg`，重复请求的 `CF-Cache-Status` 为 `HIT`；无效路径返回 `404`；原有 `/api/image/bgm` 路径仍可用。生产自动部署成功，线上 JavaScript 包包含新图片域名。

回退网站侧地址时，可将 `lib/image-proxy.ts` 中的常规封面 URL 重新指向 `/api/image/bgm?url=...`。更新 Nginx 配置前运行 `nginx -t`，通过后再重新加载。
