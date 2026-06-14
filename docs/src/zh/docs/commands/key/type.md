# TYPE

返回存储在键中的值的类型字符串表示。可以返回的不同类型有：string（字符串）、list（列表）、set（集合）、zset（有序集合）和 hash（哈希）。

## Syntax

```
TYPE key
```

## Return

Simple string reply: 键的类型，如果键不存在则返回 none。

## Examples

```
redis> SET key1 "value"
OK
redis> LPUSH key2 "value"
(integer) 1
redis> SADD key3 "value"
(integer) 1
redis> TYPE key1
string
redis> TYPE key2
list
redis> TYPE key3
set
```