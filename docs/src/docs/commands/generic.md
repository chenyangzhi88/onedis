---
title: Generic Commands
titleTemplate: Commands
description: Overview of Rudis generic commands including AUTH, CLIENT, ECHO, PING, and SELECT commands.
---

# Generic Commands

Generic commands are a set of commands used to manage client connections and basic server interactions. These commands don't directly manipulate data, but provide connection management, server status checking, and other basic functionalities.

## Command List

<div class="command-cards">
  <a href="./generic/auth.md" class="command-card">
    <div class="card-title">AUTH</div>
    <div class="card-description">Used to authenticate the server connection password</div>
  </a>
  <a href="./generic/client.md" class="command-card">
    <div class="card-title">CLIENT</div>
    <div class="card-description">Used to get or set client connection related information</div>
  </a>
  <a href="./generic/echo.md" class="command-card">
    <div class="card-title">ECHO</div>
    <div class="card-description">Prints the given string, mainly used for testing connections</div>
  </a>
  <a href="./generic/ping.md" class="command-card">
    <div class="card-title">PING</div>
    <div class="card-description">Used to test if the connection to the server is normal</div>
  </a>
  <a href="./generic/select.md" class="command-card">
    <div class="card-title">SELECT</div>
    <div class="card-description">Switches to the specified database</div>
  </a>
</div>

## Use Cases

Generic commands are typically used immediately after a client connection is established, or when verifying connection status. For example, the PING command is often used for heartbeat detection to ensure the connection remains valid.

For detailed usage and parameters of each command, please refer to the individual command documentation linked above.