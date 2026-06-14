# RPUSHX

仅当列表键已经存在时，将一个或多个值插入到列表的尾部（右边）。如果键不存在，则不执行任何操作。

## Syntax

```
RPUSHX key value [value ...]
```

## Return

Integer reply: 执行插入操作后列表的长度，如果键不存在则返回 0。

## Examples

```
redis> RPUSHX mylist "world"
(integer) 0
redis> RPUSH mylist "hello"
(integer) 1
redis> RPUSHX mylist "world"
(integer) 2
redis> LRANGE mylist 0 -1
1) "hello"
2) "world"
```