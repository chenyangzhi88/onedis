# MSET

同时设置多个键值对。如果某个键已经存在，会被覆盖。

## Syntax

```
MSET key value [key value ...]
```

## Return

Simple string reply: OK

## Examples

```
redis> MSET key1 "Hello" key2 "World"
OK
redis> GET key1
"Hello"
redis> GET key2
"World"
```