# artifact-registry

本地内容寻址制品仓库 HTTP 服务。元数据存于 SQLite（WAL），文件内容按
SHA-256 摘要存于数据目录的 \`blobs/\` 下，相同摘要只保留一份物理内容。

## 启动

数据目录只能来自启动参数或环境变量，二者都未提供时进程退出：

\`\`\`sh
go run ./cmd/registryd -addr 127.0.0.1:8080 -data ./var
# 或
REGISTRY_DATA_DIR=./var go run ./cmd/registryd -addr 127.0.0.1:8080
\`\`\`

可选参数：\`-shutdown-timeout\`（默认 15s）。收到 SIGINT/SIGTERM 时优雅关闭：
先停止接收新连接并等待在途请求完成，再关闭 SQLite。

## 核心 API

所有错误返回稳定的 JSON 结构：\`{"code": "...", "message": "..."}\`。

### 创建上传

\`\`\`sh
curl -X POST localhost:8080/v1/uploads -H 'Content-Type: application/json' -d '{
  "tenant": "acme",
  "object_name": "releases/app.tar",
  "sha256": "<64位小写hex>",
  "size": 300000
}'
# 201 {"id":"...","tenant":"acme","object_name":"releases/app.tar","size":300000,"sha256":"...","state":"open"}
\`\`\`

### 上传分块（左闭右闭，单块 ≤ 1MiB，原始二进制）

\`\`\`sh
curl -X POST localhost:8080/v1/uploads/<id>/chunks \\
  -H 'Content-Range: bytes 0-199999/300000' --data-binary @part1.bin
# 204；相同区间相同字节重发幂等，重叠或内容不一致返回 409
\`\`\`

### 完成上传（可重试）

\`\`\`sh
curl -X POST localhost:8080/v1/uploads/<id>/complete
# 200 {"id":"...","state":"committed",...}
# 区间未恰好覆盖 size 返回 409；摘要不匹配返回 422
\`\`\`

### 读取对象 / 按摘要读取

\`\`\`sh
curl localhost:8080/v1/objects/acme/releases/app.tar
curl -H 'Range: bytes=10-19' localhost:8080/v1/objects/acme/releases/app.tar
# 206，带 Content-Range / Accept-Ranges / Content-Length
curl localhost:8080/v1/blobs/<sha256>
\`\`\`

多重或后缀 Range 返回 400，越界返回 416。

### 健康检查

\`\`\`sh
curl localhost:8080/healthz   # 200 ok
\`\`\`

## 设计要点

- 上传状态机：\`open → committing → committed\`，失败进入终态 \`conflict\`；
  重启时 \`committing\` 回退为 \`open\`，孤儿暂存文件被清理。
- 分块先写临时文件、fsync、原子重命名，元数据行在字节落盘后提交。
- 完成时流式拼接分块并计算 SHA-256，校验通过后在事务内发布对象并原子
  重命名到 \`blobs/<前两位>/<完整摘要>\`，失败不留可见半成品。
- 对象名禁止绝对路径、\`..\` 穿越与反斜杠；租户为独立命名空间。
