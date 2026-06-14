---
title: 列表命令
titleTemplate: 命令
description: Rudis 列表命令概述，包括 LINDEX、LLEN、LPOP、LPUSH、LPUSHX、LRANGE、LSET、RPUSH、RPUSHX 和 RPOP 命令。
---

# 列表命令

列表命令允许您将字符串元素存储在列表中，支持从两端插入和弹出元素。列表是有序的，可以包含重复元素。

## 命令列表

<div class="command-cards">
  <a href="./list/lindex" class="command-card">
    <div class="card-title">LINDEX</div>
    <div class="card-description">返回列表中指定索引位置的元素</div>
  </a>
  <a href="./list/llen" class="command-card">
    <div class="card-title">LLEN</div>
    <div class="card-description">返回列表的长度</div>
  </a>
  <a href="./list/lpop" class="command-card">
    <div class="card-title">LPOP</div>
    <div class="card-description">移除并返回列表的第一个元素</div>
  </a>
  <a href="./list/lpush" class="command-card">
    <div class="card-title">LPUSH</div>
    <div class="card-description">将一个或多个值插入到列表头部</div>
  </a>
  <a href="./list/lpushx" class="command-card">
    <div class="card-title">LPUSHX</div>
    <div class="card-description">将值插入到已存在的列表头部</div>
  </a>
  <a href="./list/lrange" class="command-card">
    <div class="card-title">LRANGE</div>
    <div class="card-description">返回列表中指定范围内的元素</div>
  </a>
  <a href="./list/lset" class="command-card">
    <div class="card-title">LSET</div>
    <div class="card-description">通过索引设置列表元素的值</div>
  </a>
  <a href="./list/rpush" class="command-card">
    <div class="card-title">RPUSH</div>
    <div class="card-description">将一个或多个值插入到列表尾部</div>
  </a>
  <a href="./list/rpushx" class="command-card">
    <div class="card-title">RPUSHX</div>
    <div class="card-description">将值插入到已存在的列表尾部</div>
  </a>
  <a href="./list/rpop" class="command-card">
    <div class="card-title">RPOP</div>
    <div class="card-description">移除并返回列表的最后一个元素</div>
  </a>
</div>

## 使用场景

列表命令非常适合用于消息队列、时间线、最近浏览记录等场景。LPUSH 和 RPOP（或 RPUSH 和 LPOP）组合可以实现队列功能，而 LPUSH 和 LPOP 组合可以实现栈功能。

详细了解每个命令的用法和参数，请参阅上述链接的各个命令文档。