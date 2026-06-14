# INCRBYFLOAT

将键存储的浮点数值加上指定的浮点数。如果键不存在，则在执行操作前将其设置为 0。如果键的值不是有效的浮点数，则返回错误。

## Syntax

```
INCRBYFLOAT key increment
```

## Return

Bulk string reply: 加上指定浮点数之后键的值。

## Examples

```
redis> SET mykey "10.50"
OK
redis> INCRBYFLOAT mykey 0.1
"10.6"
redis> INCRBYFLOAT mykey -5
"5.6"
redis> SET mykey "5.0e3"
OK
redis> INCRBYFLOAT mykey 2.0e2
"5200"
```