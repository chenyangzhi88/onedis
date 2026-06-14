---
title: List Commands
titleTemplate: Commands
description: Overview of Rudis list commands including LINDEX, LLEN, LPOP, LPUSH, LPUSHX, LRANGE, LSET, RPUSH, RPUSHX, and RPOP commands.
---

# List Commands

List commands allow you to store string elements in lists, supporting insertion and popping from both ends. Lists are ordered and can contain duplicate elements.

## Command List

<div class="command-cards">
  <a href="./list/lindex" class="command-card">
    <div class="card-title">LINDEX</div>
    <div class="card-description">Returns the element at the specified index position in the list</div>
  </a>
  <a href="./list/llen" class="command-card">
    <div class="card-title">LLEN</div>
    <div class="card-description">Returns the length of the list</div>
  </a>
  <a href="./list/lpop" class="command-card">
    <div class="card-title">LPOP</div>
    <div class="card-description">Removes and returns the first element of the list</div>
  </a>
  <a href="./list/lpush" class="command-card">
    <div class="card-title">LPUSH</div>
    <div class="card-description">Inserts one or more values at the head of the list</div>
  </a>
  <a href="./list/lpushx" class="command-card">
    <div class="card-title">LPUSHX</div>
    <div class="card-description">Inserts a value at the head of an existing list</div>
  </a>
  <a href="./list/lrange" class="command-card">
    <div class="card-title">LRANGE</div>
    <div class="card-description">Returns elements in the specified range of the list</div>
  </a>
  <a href="./list/lset" class="command-card">
    <div class="card-title">LSET</div>
    <div class="card-description">Sets the value of a list element by index</div>
  </a>
  <a href="./list/rpush" class="command-card">
    <div class="card-title">RPUSH</div>
    <div class="card-description">Inserts one or more values at the tail of the list</div>
  </a>
  <a href="./list/rpushx" class="command-card">
    <div class="card-title">RPUSHX</div>
    <div class="card-description">Inserts a value at the tail of an existing list</div>
  </a>
  <a href="./list/rpop" class="command-card">
    <div class="card-title">RPOP</div>
    <div class="card-description">Removes and returns the last element of the list</div>
  </a>
</div>

## Use Cases

List commands are ideal for message queues, timelines, recent browsing records, and other scenarios. The combination of LPUSH and RPOP (or RPUSH and LPOP) can implement queue functionality, while the combination of LPUSH and LPOP can implement stack functionality.

For detailed usage and parameters of each command, please refer to the individual command documentation linked above.