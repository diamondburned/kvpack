# Benchmarks

## IsZero

![graph](https://matrix-client.matrix.org/_matrix/media/r0/download/matrix.org/alABfPNyutENBnxHlNljMfac)

| Name                                       | Iterations | Time per Op  | Throughput         |
| ------------------------------------------ | ---------- | ------------ | ------------------ |
| BenchmarkIsZeroStreq/byte-8                | 171917054  | 6.613 ns/op  | 1209.68 MB/s       |
| BenchmarkIsZeroStreq/dbyte-8               | 156105722  | 7.422 ns/op  | 2155.62 MB/s       |
| BenchmarkIsZeroStreq/big-8                 | 16297855   | 65.91 ns/op  | 31072.66 MB/s      |
| BenchmarkIsZeroStreq/massive-8             | 69648      | 15566 ns/op  | 33682.58 MB/s      |
| BenchmarkIsZeroStreq/gigantic-8            | 454539020  | 2.617 ns/op  | 1602525636.73 MB/s |
| BenchmarkIsZeroStreq/byte_parallel-8       | 829908123  | 1.410 ns/op  | 5673.39 MB/s       |
| BenchmarkIsZeroStreq/dbyte_parallel-8      | 696951979  | 1.583 ns/op  | 10105.05 MB/s      |
| BenchmarkIsZeroStreq/big_parallel-8        | 53131268   | 20.36 ns/op  | 100584.94 MB/s     |
| BenchmarkIsZeroStreq/massive_parallel-8    | 213414     | 5011 ns/op   | 104623.58 MB/s     |
| BenchmarkIsZeroStreq/gigantic_parallel-8   | 1000000000 | 0.8339 ns/op | 5029684228.34 MB/s |
| BenchmarkIsZero8Bytes/byte-8               | 327868894  | 3.433 ns/op  | 2329.99 MB/s       |
| BenchmarkIsZero8Bytes/dbyte-8              | 313772871  | 3.931 ns/op  | 4069.99 MB/s       |
| BenchmarkIsZero8Bytes/big-8                | 5515704    | 201.2 ns/op  | 10179.95 MB/s      |
| BenchmarkIsZero8Bytes/massive-8            | 24033      | 47869 ns/op  | 10952.55 MB/s      |
| BenchmarkIsZero8Bytes/gigantic-8           | 2608       | 467295 ns/op | 8975.71 MB/s       |
| BenchmarkIsZero8Bytes/byte_parallel-8      | 1000000000 | 1.060 ns/op  | 7547.98 MB/s       |
| BenchmarkIsZero8Bytes/dbyte_parallel-8     | 932357254  | 1.238 ns/op  | 12926.06 MB/s      |
| BenchmarkIsZero8Bytes/big_parallel-8       | 18687656   | 57.45 ns/op  | 35645.74 MB/s      |
| BenchmarkIsZero8Bytes/massive_parallel-8   | 80784      | 14416 ns/op  | 36367.27 MB/s      |
| BenchmarkIsZero8Bytes/gigantic_parallel-8  | 8664       | 119811 ns/op | 35007.77 MB/s      |
| BenchmarkIsZero16Bytes/byte-8              | 100000000  | 10.21 ns/op  | 783.30 MB/s        |
| BenchmarkIsZero16Bytes/dbyte-8             | 262728376  | 4.515 ns/op  | 3544.14 MB/s       |
| BenchmarkIsZero16Bytes/big-8               | 4765987    | 256.6 ns/op  | 7981.03 MB/s       |
| BenchmarkIsZero16Bytes/massive-8           | 18633      | 62969 ns/op  | 8326.09 MB/s       |
| BenchmarkIsZero16Bytes/gigantic-8          | 2210       | 543225 ns/op | 7721.12 MB/s       |
| BenchmarkIsZero16Bytes/byte_parallel-8     | 446812456  | 2.618 ns/op  | 3055.72 MB/s       |
| BenchmarkIsZero16Bytes/dbyte_parallel-8    | 815567397  | 1.396 ns/op  | 11462.87 MB/s      |
| BenchmarkIsZero16Bytes/big_parallel-8      | 16607584   | 69.63 ns/op  | 29414.54 MB/s      |
| BenchmarkIsZero16Bytes/massive_parallel-8  | 66019      | 17365 ns/op  | 30191.68 MB/s      |
| BenchmarkIsZero16Bytes/gigantic_parallel-8 | 7453       | 150101 ns/op | 27943.21 MB/s      |
