# HSTRLEN

返回哈希表中指定字段值的字符串长度。

## Syntax

```
HSTRLEN key field
```

## Return

Integer reply: 字段值的字符串长度，如果字段不存在或键不存在则返回 0。

## Examples

```
redis> HSET myhash f1 "HelloWorld"
(integer) 1
redis> HSTRLEN myhash f1
(integer) 10
redis> HSTRLEN myhash f2
(integer) 0
```