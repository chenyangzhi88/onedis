# INCRBY

将键存储的数字值加上指定的整数。如果键不存在，则在执行操作前将其设置为 0。如果键的值不是整数或超出范围，则返回错误。

## Syntax

```
INCRBY key increment
```

## Return

Integer reply: 加上指定整数之后键的值。

## Examples

```
redis> SET mykey "10"
OK
redis> INCRBY mykey 5
(integer) 15
redis> SET mykey "234293482390480948029348230948"
OK
redis> INCRBY mykey 5
ERR value is not an integer or out of range
```