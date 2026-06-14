# LINDEX

返回列表中指定索引位置的元素。索引从 0 开始，负数索引表示从列表末尾开始计数。

## Syntax

```
LINDEX key index
```

## Return

Bulk string reply: 列表中指定索引位置的元素，如果索引超出范围则返回 nil。

## Examples

```
redis> LPUSH mylist "World"
(integer) 1
redis> LPUSH mylist "Hello"
(integer) 2
redis> LINDEX mylist 0
"Hello"
redis> LINDEX mylist -1
"World"
redis> LINDEX mylist 3
(nil)
```