---
title: Configuration
titleTemplate: Guides
description: Essential information to help you get set up with Tachiyomi.
---

# Configuration

Overview of `config/unidb.toml`, the shared configuration file.

## Specify configuration file startup

Rudis is able to start without a configuration file using a built-in default configuration, however this setup is only recommended for testing and development purposes.

The proper way to configure Rudis in this repository is by editing `config/unidb.toml` and placing server settings under the `onedis_server` section.

```sh
./rudis-server --config config/unidb.toml
```

The supported server configuration fields are documented in the `onedis_server` section of `config/unidb.toml`.

## Passing arguments via the command line

You can also pass Rudis configuration parameters using the command line directly. This is very useful for testing purposes. The following is an example that starts a new Rudis instance using port 6380 as a replica of the instance running at 127.0.0.1 port 6379.

```
./rudis-server --port 6380
```

Command-line arguments still use the `--key value` form and override values loaded from `config/unidb.toml`.

At startup, Rudis reads `config/unidb.toml`, extracts the `onedis_server` section, and then applies command-line overrides on top of it.

## Changing Rudis configuration while the server is running

Rudis supports dynamic configuration changes without restarting the server. You can modify certain configuration parameters at runtime using the CONFIG SET command. For example:

```
CONFIG SET maxclients 1000
CONFIG SET appendfsync everysec
```

Not all configuration options can be changed at runtime. Some settings require a server restart to take effect, such as port binding or persistence file locations. You can check which parameters can be modified dynamically by using the CONFIG GET command to retrieve current values.

## Server Configuration

### Password

- version: `0.0.1`

After setting the password for the client to connect to the server, password verification is required for the client to connect to the Rudis service, otherwise the command cannot be executed.

### Port

- version: `0.0.1`

Accept connections on the specified port, default is 6379 (IANA #815344). If port 0 is specified Rudis will not listen on a TCP socket.

### Appendonly

- version: `0.0.1`

Specify whether to log after each update operation. Rudis does not write data to the disk by default. If not enabled, it may result in data loss for a period of time in the event of a power outage.

### Appendfilename

- version: `0.0.1`

Specify the update log file name, which defaults to appendonly.aof

### Dbfilename

- version: `0.0.1`

Specify the local database file name, with a default value of dump.rdb.

### Save

- version: `0.0.1`

Specify how long to synchronize data to a data file.

### Databases

- version: `0.0.1`

Set the number of databases. The default database is DB 0. You can use the select dbid command on each connection to select a different database, but the dbid must be a value between 0 and databases -1.

### Bind

- version: `0.0.1`

The bound host address effectively controls the network interface that Rudis server listens to, thereby achieving safer and more proprietary network access settings.

### Maxclients

- version: `0.0.1`

Set the maximum number of client connections at the same time, with a default value of 0, indicating no restrictions. When the number of client connections reaches the limit, Rudis will close new connections and return a max number of clients reached error message to the client.


### Hz

- version: `0.0.1`

By modifying the value of the hz parameter, you can adjust the frequency of Rudis executing periodic tasks, thereby changing the efficiency of Rudis clearing expired keys and clearing timeout connections.
