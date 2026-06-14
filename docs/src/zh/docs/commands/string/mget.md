# MGET

返回所有指定键的值。对于不存在的键，返回 nil。

## Syntax

```
MGET key [key ...]
```

## Return

Array reply: 指定键的值列表。

## Examples

```
redis> SET key1 "Hello"
OK
redis> SET key2 "World"
OK
redis> MGET key1 key2 nonexisting
1) "Hello"
2) "World"
3) (nil)
```