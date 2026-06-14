# LSET

设置列表中指定索引位置的元素。索引从 0 开始，负数索引表示从列表末尾开始计数。

## Syntax

```
LSET key index value
```

## Return

Simple string reply: OK，如果索引超出范围则返回错误。

## Examples

```
redis> LPUSH mylist "one"
(integer) 1
redis> LPUSH mylist "two"
(integer) 2
redis> LPUSH mylist "three"
(integer) 3
redis> LSET mylist 0 "four"
OK
redis> LSET mylist -2 "five"
OK
redis> LRANGE mylist 0 -1
1) "four"
2) "five"
3) "one"
```