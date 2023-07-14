# boringdb

## Why
Is an LSM based database. Written for fun. Tried to write the part most interesting to me:
* LSM
* Compaction
* Log
* Compression (TODO)

## File layout
The SSTable is Columnar as opposed to row based. I took this approach because I was thinking of using different compression techniques for different columns.

## Benchmarks
The database uses Redis so we can bench easily using redis-benchmark
```
redis-benchmark -t set,get -c 1 -r 1000000
```

## Docs:
* https://github.com/google/leveldb/blob/main/doc/impl.md
