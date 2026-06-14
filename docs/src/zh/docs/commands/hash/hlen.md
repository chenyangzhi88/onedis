# HLEN

返回哈希表中字段的数量。

## Syntax

```
HLEN key
```

## Return

Integer reply: 哈希表中字段的数量，如果键不存在则返回 0。

## Examples

```
redis> HSET myhash field1 "Hello"
(integer) 1
redis> HSET myhash field2 "World"
(integer) 1
redis> HLEN myhash
(integer) 2
```