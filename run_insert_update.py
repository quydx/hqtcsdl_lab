from clients import mysql_config, mongo_config
from benchmarker import QueryBenchmark

if __name__ == "__main__":
    benchmark = QueryBenchmark(mysql_config, mongo_config)

    nums = [100, 1000, 10000]
    for num in nums:
        benchmark.benchmark_concurrent_insert(num)

    benchmark.plot_results("insert")
    benchmark.close()
    benchmark.results = []

    for num in nums:
        benchmark.benchmark_concurrent_update(num)
    # concurrent update 100000
    benchmark.plot_results("update")
    benchmark.close()

