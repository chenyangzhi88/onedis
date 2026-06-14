# KEYS

查找所有符合给定模式的键。支持 glob 风格的模式匹配。

## Syntax

```
KEYS pattern
```

## Return

Array reply: 匹配模式的键列表。

## Examples

```
redis> MSET firstname Jack lastname Stuntman age 35
OK
redis> KEYS *name*
1) "firstname"
2) "lastname"
redis> KEYS a??
1) "age"
redis> KEYS *
1) "firstname"
2) "lastname"
3) "age"
```