# HMSET

同时将一个或多个字段-值对设置到哈希表中。如果字段已经存在，将会被覆盖。

## Syntax

```
HMSET key field value [field value ...]
```

## Return

Simple string reply: OK

## Examples

```
redis> HMSET myhash field1 "Hello" field2 "World"
OK
redis> HGET myhash field1
"Hello"
redis> HGET myhash field2
"World"
```