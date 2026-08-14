# JSON Commands

Onedis supports a focused RedisJSON-compatible command subset for storing, retrieving, and
manipulating JSON documents.

## Commands

### JSON.SET
Set the JSON value at key

**Syntax:**
```
JSON.SET key [path] value [NX|XX]
```

**Parameters:**
- `key`: The key to set
- `path`: The JSON path (default is "$")
- `value`: The JSON value to set
- `NX`: Only set if key does not exist
- `XX`: Only set if key exists

**Returns:**
- `OK` if successful
- `nil` if condition not met (NX/XX)

**Example:**
```redis
JSON.SET user:1 $ '{"name":"John","age":30}'
JSON.SET user:2 $ '{"name":"Jane","age":25}' NX
```

### JSON.GET
Get the JSON value at key

**Syntax:**
```
JSON.GET key [path]
```

**Parameters:**
- `key`: The key to get
- `path`: The JSON path (default is "$")

**Returns:**
- JSON string if key exists
- `nil` if key does not exist

**Example:**
```redis
JSON.GET user:1
JSON.GET user:1 $
JSON.GET user:1 $.name
```

### JSON.DEL
Delete a key or path

**Syntax:**
```
JSON.DEL key [path]
```

**Parameters:**
- `key`: The key to delete
- `path`: The JSON path (optional, deletes entire key if not specified)

**Returns:**
- Number of paths deleted (1 if key was deleted, 0 if key did not exist)

**Example:**
```redis
JSON.DEL user:1
JSON.DEL user:2 $.profile.city
```

### JSON.TYPE

Return the JSON type at a path.

```redis
JSON.TYPE user:1 $
JSON.TYPE user:1 $.age
```

The result is one of `object`, `array`, `string`, `integer`, `number`, `boolean`, or `null`.

## Data Storage

The main key stores type, TTL, and a document version. JSON nodes live under versioned internal
keys:

- scalars are individual nodes;
- objects use a structural generation node, while field names come from child keys;
- arrays keep logical order in a compact directory of stable element ids.

Composite reads use one bounded subtree scan and reconstruct the result in memory. Root replacement
writes a fresh version and atomically switches the main key; obsolete nodes are retired and removed
by version compaction. It never persists cleanup from WAL recovery. Array deletion removes one
element subtree and one directory entry, without rewriting later element values.

Existing fields on the same JSON key may update concurrently. The implementation uses a shared key
structure barrier plus CAS observations for the main metadata, traversed ancestor nodes, the parent,
and target node. Independent fields do not conflict; same-path writes and ancestor/descendant changes
are retried. Repeated conflicts fall back to a parent-path structural lock, so a hot object with many
concurrent field additions makes progress instead of exhausting the retry budget.

This is the v2 indexed layout. Legacy string-backed JSON documents and the v1 indexed node encoding
are intentionally unsupported; existing data must be recreated when upgrading to this layout.

## Limitations

Current limitations:

1. Paths support the root (`$` or `.`), dotted object fields, and non-negative array indexes.
2. Wildcards, recursive descent, filters, quoted field selectors, and negative indexes are not
   supported.
3. The command surface currently contains `JSON.SET`, `JSON.GET`, `JSON.DEL`, and `JSON.TYPE` only.

## Benchmark

Run the dedicated JSON matrix against a disposable benchmark instance (default port `6391`):

```bash
bash scripts/bench_json_commands.sh benchmarks/json/json-benchmark.csv
```

The matrix covers leaf/root reads, same-key and random-key writes, wide-object growth, root
replacement, object deletion, and array-head deletion at pipelines 1, 16, and 64. It flushes the
selected benchmark database and must not be pointed at production.
