---
title: 通用命令
titleTemplate: 命令
description: Rudis 通用命令概述，包括 AUTH、CLIENT、ECHO、PING 和 SELECT 命令。
---

# 通用命令

通用命令是一组用于管理客户端连接和基本服务器交互的命令。这些命令不直接操作数据，而是提供连接管理、服务器状态检查和其他基础功能。

## 命令列表

<div class="command-cards">
  <a href="./generic/auth" class="command-card">
    <div class="card-title">AUTH</div>
    <div class="card-description">用于验证服务器端连接密码</div>
  </a>
  <a href="./generic/client" class="command-card">
    <div class="card-title">CLIENT</div>
    <div class="card-description">用于获取或设置客户端连接的相关信息</div>
  </a>
  <a href="./generic/echo" class="command-card">
    <div class="card-title">ECHO</div>
    <div class="card-description">打印给定的字符串，主要用于测试连接</div>
  </a>
  <a href="./generic/ping" class="command-card">
    <div class="card-title">PING</div>
    <div class="card-description">用于测试与服务器的连接是否正常</div>
  </a>
  <a href="./generic/select" class="command-card">
    <div class="card-title">SELECT</div>
    <div class="card-description">切换到指定的数据库</div>
  </a>
</div>

## 使用场景

通用命令通常在客户端连接建立后立即使用，或者在需要验证连接状态时使用。例如，PING 命令经常用于心跳检测，确保连接仍然有效。

详细了解每个命令的用法和参数，请参阅上述链接的各个命令文档。