# GETRANGE

返回键存储的字符串值中指定开始和结束位置之间的子字符串。开始和结束偏移量都是基于 0 的索引，负数偏移量表示从字符串末尾开始计数。

## Syntax

```
GETRANGE key start end
```

## Return

Bulk string reply: 指定范围内的字符串。

## Examples

```
redis> SET mykey "This is a string"
OK
redis> GETRANGE mykey 0 3
"This"
redis> GETRANGE mykey -3 -1
"ing"
redis> GETRANGE mykey 0 -1
"This is a string"
redis> GETRANGE mykey 10 100
"string"
```