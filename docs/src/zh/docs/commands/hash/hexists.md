# HEXISTS

检查哈希表中指定字段是否存在。

## Syntax

```
HEXISTS key field
```

## Return

Integer reply: 如果字段存在返回 1，如果字段不存在或键不存在返回 0。

## Examples

```
redis> HSET myhash field1 "foo"
(integer) 1
redis> HEXISTS myhash field1
(integer) 1
redis> HEXISTS myhash field2
(integer) 0
```