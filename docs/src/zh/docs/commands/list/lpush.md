# LPUSH

将一个或多个值插入到列表的头部（左边）。如果键不存在，则在执行操作前创建一个空列表。

## Syntax

```
LPUSH key value [value ...]
```

## Return

Integer reply: 执行插入操作后列表的长度。

## Examples

```
redis> LPUSH mylist "world"
(integer) 1
redis> LPUSH mylist "hello"
(integer) 2
redis> LPUSH mylist "foo" "bar"
(integer) 4
redis> LRANGE mylist 0 -1
1) "bar"
2) "foo"
3) "hello"
4) "world"
```