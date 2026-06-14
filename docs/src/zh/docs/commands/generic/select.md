# SELECT

切换到指定的数据库。Redis 默认有 16 个数据库，编号从 0 到 15。

## Syntax

```
SELECT index
```

## Return

Simple string reply: OK

## Examples

```
redis> SELECT 0
OK
redis> SELECT 15
OK
redis> SELECT 16
ERR invalid DB index
```