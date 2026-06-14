# HKEYS

返回哈希表中所有的字段名。

## Syntax

```
HKEYS key
```

## Return

Array reply: 以列表形式返回哈希表中的所有字段名。

## Examples

```
redis> HSET myhash field1 "Hello"
(integer) 1
redis> HSET myhash field2 "World"
(integer) 1
redis> HKEYS myhash
1) "field1"
2) "field2"
```