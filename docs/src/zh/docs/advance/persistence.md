---
title: 备份与恢复
titleTemplate: 高级
description: OneDis 与 kv-engine 的持久化职责
---

# 备份与恢复

OneDis 不生成 Redis RDB 或 AOF 文件。数据持久化、WAL、tablet/object storage、快照以及备份
恢复均由 kv-engine 部署负责；OneDis 只在 RESP 层提交数据操作并在优雅关闭时要求 kv-engine
同步 WAL。

因此 `SAVE`、`BGSAVE` 以及 Redis replication 命令会返回明确的 unsupported 错误，不能把其
返回值用作备份完成信号。

## 生产操作原则

1. 按 kv-engine 的部署模式配置本地、对象存储或分层 tablet store。
2. 使用 kv-engine 提供的快照/备份能力，并同时记录存储格式版本和 OneDis 版本。
3. 恢复到隔离环境后，先验证普通 KV、TTL 和 JSON，再验证 Search/Vector generation。
4. Search 或 Vector 派生索引不一致时，按运行手册执行索引重建，不能回放 Redis AOF。
5. 只有 `/readyz` 恢复成功且校验通过后才重新接入流量。

具体流程见仓库中的 `docs/operations/production-runbook.md` 和
`docs/operations/upgrade-rollback.md`。
