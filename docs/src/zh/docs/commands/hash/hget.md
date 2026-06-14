# HGET

返回哈希表中指定字段的值。

## Syntax

```
HGET key field
```

## Return

Bulk string reply: 给定字段的值，如果字段不存在或键不存在则返回 nil。

## Examples

```
redis> HSET myhash field1 "foo"
(integer) 1
redis> HGET myhash field1
"foo"
redis> HGET myhash field2
(nil)
```