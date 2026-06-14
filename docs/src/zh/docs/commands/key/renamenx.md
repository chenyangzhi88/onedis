# RENAMENX

仅在新键不存在时，将键重命名为新键。如果新键已存在则操作失败。

## Syntax

```
RENAMENX key newkey
```

## Return

Integer reply: 

- 1 如果键被成功重命名
- 0 如果新键已存在
- 错误 如果键不存在

## Examples

```
redis> SET mykey "Hello"
OK
redis> SET myotherkey "World"
OK
redis> RENAMENX mykey myotherkey
(integer) 0
redis> RENAMENX mykey mynewkey
(integer) 1
```