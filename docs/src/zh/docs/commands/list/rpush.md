# RPUSH

将一个或多个值插入到列表的尾部（右边）。如果键不存在，则在执行操作前创建一个空列表。

## Syntax

```
RPUSH key value [value ...]
```

## Return

Integer reply: 执行插入操作后列表的长度。

## Examples

```
redis> RPUSH mylist "hello"
(integer) 1
redis> RPUSH mylist "world"
(integer) 2
redis> RPUSH mylist "foo" "bar"
(integer) 4
redis> LRANGE mylist 0 -1
1) "hello"
2) "world"
3) "foo"
4) "bar"
```