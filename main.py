import random
import string
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import mysql.connector
from pymongo import MongoClient
import matplotlib.pyplot as plt


class QueryBenchmark:
    def __init__(self, mysql_config, mongo_config, threads=10, inserts_per_thread=100):
        # MySQL setup
        self.mysql_config = mysql_config
        self.mysql_conn = mysql.connector.connect(**mysql_config)
        self.mysql_cursor = self.mysql_conn.cursor(dictionary=True)

        # MongoDB setup
        self.mongo_client = MongoClient(mongo_config["uri"])
        self.mongo_db = self.mongo_client[mongo_config["db"]]
        self.mongo_col = self.mongo_db[mongo_config["collection"]]

        # For tracking
        self.queries = []
        self._ensure_mysql_indexes()
        self._ensure_mongo_indexes()
        self.threads = threads
        self.inserts_per_thread = inserts_per_thread
        self.inserted_cccds = []

    def _ensure_mysql_indexes(self):
        try:
            self.mysql_cursor.execute("SHOW INDEX FROM thanh_vien WHERE Key_name = 'idx_thanh_vien_cccd'")
            exists = self.mysql_cursor.fetchone()
            if not exists:
                self.mysql_cursor.execute("CREATE INDEX idx_thanh_vien_cccd ON thanh_vien(cccd)")
                self.mysql_conn.commit()
        except mysql.connector.Error as e:
            print("⚠️ MySQL indexing error:", e)

    def _ensure_mongo_indexes(self):
        self.mongo_col.create_index("cccd", name="idx_cccd")

    def add_query(self, label, mysql_query, mongo_pipeline):
        self.queries.append((label, mysql_query, mongo_pipeline))

    def run_mysql(self, query):
        start = time.time()
        self.mysql_cursor.execute(query)
        result = self.mysql_cursor.fetchall()
        end = time.time()
        return result, end - start

    def run_mongo(self, pipeline):
        start = time.time()
        result = list(self.mongo_col.aggregate(pipeline))
        end = time.time()
        return result, end - start

    def run_all(self):
        self.mysql_times = []
        self.mongo_times = []
        self.labels = []

        for label, mysql_query, mongo_pipeline in self.queries:
            self.labels.append(label)

            mysql_result, mysql_time = self.run_mysql(mysql_query)
            mongo_result, mongo_time = self.run_mongo(mongo_pipeline)

            print(f"\n=== {label} ===")
            print(f"MySQL: {len(mysql_result)} rows in {mysql_time:.4f} sec")
            print(f"Mongo: {len(mongo_result)} docs in {mongo_time:.4f} sec")

            self.mysql_times.append(mysql_time)
            self.mongo_times.append(mongo_time)

    def _run_mysql_query(self, query):
        start = time.perf_counter()
        self.mysql_cursor.execute(query)
        result = self.mysql_cursor.fetchall()
        elapsed = time.perf_counter() - start
        return elapsed, result

    def _run_mongo_query(self, pipeline):
        start = time.perf_counter()
        result = list(self.mongo_col.aggregate(pipeline))
        elapsed = time.perf_counter() - start
        return elapsed, result

    def run(self):
        self.results = []
        for name, mysql_q, mongo_pipeline in self.queries:
            mysql_time, _ = self._run_mysql_query(mysql_q)
            mongo_time, _ = self._run_mongo_query(mongo_pipeline)
            self.results.append((name, mysql_time, mongo_time))

    def plot_results(self):
        names = [r[0] for r in self.results]
        mysql_times = [r[1] for r in self.results]
        mongo_times = [r[2] for r in self.results]

        x = range(len(names))
        width = 0.35

        fig, ax = plt.subplots()
        mysql_bars = ax.bar([i - width / 2 for i in x], mysql_times, width, label="MySQL")
        mongo_bars = ax.bar([i + width / 2 for i in x], mongo_times, width, label="MongoDB")

        ax.set_ylabel('Execution Time (seconds)')
        ax.set_title('Query Performance: MySQL vs MongoDB')
        ax.set_xticks(x)
        ax.set_xticklabels(names, rotation=30, ha='right')
        ax.legend()

        # Add time labels on top of each bar
        def add_labels(bars, times):
            for bar, time in zip(bars, times):
                height = bar.get_height()
                ax.annotate(f'{time:.4f}s',
                            xy=(bar.get_x() + bar.get_width() / 2, height),
                            xytext=(0, 3),  # offset
                            textcoords="offset points",
                            ha='center', va='bottom', fontsize=8)

        add_labels(mysql_bars, mysql_times)
        add_labels(mongo_bars, mongo_times)

        plt.tight_layout()
        plt.show()

    def _random_cccd(self):
        return ''.join(random.choices(string.digits, k=14))

    def _random_name(self):
        return ''.join(random.choices(string.ascii_letters + " ", k=10))

    def _insert_mysql(self):
        conn = mysql.connector.connect(**self.mysql_config)
        cursor = conn.cursor()

        batch = []
        for _ in range(self.inserts_per_thread):
            name = self._random_name()
            cccd = self._random_cccd()
            batch.append((cccd, name, '1990-01-01', 'Khác'))

        query = """
            INSERT INTO thanh_vien (cccd, ten, ngay_sinh, gioi_tinh)
            VALUES (%s, %s, %s, %s)
            """
        cursor.executemany(query, batch)
        conn.commit()
        cursor.close()
        conn.close()

    def _insert_mongo(self):
        docs = []
        for _ in range(self.inserts_per_thread):
            docs.append({
                "ten": self._random_name(),
                "cccd": self._random_cccd(),
                "ngay_sinh": "1990-01-01",
                "gioi_tinh": "Khác"
            })
        self.mongo_col.insert_many(docs)

    def benchmark_concurrent_write_mysql(self, num):
        start = time.perf_counter()
        with ThreadPoolExecutor(max_workers=self.threads) as executor:
            futures = [executor.submit(self._insert_mysql) for _ in range(self.threads)]
            for future in futures:
                future.result()
        duration = time.perf_counter() - start
        print(f"MySQL write time: {duration:.4f}s")

        self.results.append((f"Concurrent Write {num}", duration, None))

    def benchmark_concurrent_write_mongo(self, num):
        start = time.perf_counter()
        with ThreadPoolExecutor(max_workers=self.threads) as executor:
            futures = [executor.submit(self._insert_mongo) for _ in range(self.threads)]
            for future in futures:
                future.result()
        duration = time.perf_counter() - start
        print(f"MongoDB write time: {duration:.4f}s")
        name, mysql_time, _ = self.results[-1]
        self.results[-1] = (name, mysql_time, duration)

    def _update_mysql(self):
        conn = mysql.connector.connect(**self.mysql_config)
        cursor = conn.cursor()
        updates = []
        for i in range(self.inserts_per_thread):
            if i < len(self.inserted_cccds):
                cccd = self.inserted_cccds[i]
                new_name = self._random_name()
                updates.append((new_name, cccd))
        cursor.executemany(
            "UPDATE thanh_vien SET ten = %s WHERE cccd = %s",
            updates
        )
        conn.commit()
        cursor.close()
        conn.close()

    def _update_mongo(self):
        updates = []
        for i in range(self.inserts_per_thread):
            if i < len(self.inserted_cccds):
                cccd = self.inserted_cccds[i]
                new_name = self._random_name()
                updates.append((cccd, new_name))
        for cccd, new_name in updates:
            self.mongo_col.update_one({"cccd": cccd}, {"$set": {"ten": new_name}})

    def benchmark_concurrent_insert(self):
        self.inserted_cccds.clear()
        print("Running concurrent insert benchmark...")
        start = time.perf_counter()
        with ThreadPoolExecutor(max_workers=self.threads) as executor:
            mysql_futures = [executor.submit(self._insert_mysql) for _ in range(self.threads)]
        mysql_time = time.perf_counter() - start

        self.inserted_cccds.clear()
        start = time.perf_counter()
        with ThreadPoolExecutor(max_workers=self.threads) as executor:
            mongo_futures = [executor.submit(self._insert_mongo) for _ in range(self.threads)]
        mongo_time = time.perf_counter() - start

        self.results.append(("Concurrent Insert", mysql_time, mongo_time))
        print(f"MySQL insert: {mysql_time:.4f}s, MongoDB insert: {mongo_time:.4f}s")

    def benchmark_concurrent_update(self, num):
        print("Running concurrent update benchmark...")
        start = time.perf_counter()
        with ThreadPoolExecutor(max_workers=self.threads) as executor:
            mysql_futures = [executor.submit(self._update_mysql) for _ in range(self.threads)]
        mysql_time = time.perf_counter() - start

        start = time.perf_counter()
        with ThreadPoolExecutor(max_workers=self.threads) as executor:
            mongo_futures = [executor.submit(self._update_mongo) for _ in range(self.threads)]
        mongo_time = time.perf_counter() - start

        self.results.append((f"Concurrent Update {num}", mysql_time, mongo_time))
        print(f"MySQL update: {mysql_time:.4f}s, MongoDB update: {mongo_time:.4f}s")

    def close(self):
        self.mysql_cursor.close()
        self.mysql_conn.close()
        self.mongo_client.close()


# ---------------------- Script Usage ----------------------

if __name__ == "__main__":
    # Setup configs
    mysql_config = {
        "host": "127.0.0.1",
        "port": 3306,
        "user": "root",
        "password": "rootpassword",  # Change this
        "database": "nhakhau_db"
    }

    mongo_config = {
        "uri": "mongodb://localhost:27017/",
        "db": "nhakhau_db",
        "collection": "nhankhau"
    }

    benchmark = QueryBenchmark(mysql_config, mongo_config)

    sample_cccd = "08637940265423"
    benchmark.add_query(
        "Find by CCCD",
        f"""
           SELECT * FROM thanh_vien
           WHERE cccd = '{sample_cccd}'
           """,
        [
            {"$match": {"cccd": sample_cccd}}
        ]
    )
    # Query for residents in a specific district
    benchmark.add_query(
        "District Filter",
        """
        SELECT
            tv.ten AS ho_ten,
            tv.gioi_tinh,
            tv.ngay_sinh,
            dc.ten_dia_chi,
            hk.ten_chu_ho
        FROM thanh_vien tv
        JOIN thanh_vien_dia_chi tdc ON tv.cccd = tdc.thanh_vien_cccd
        JOIN dia_chi dc ON tdc.dia_chi_id = dc.id_dia_chi
        JOIN ho_khau hk ON tv.id_ho_khau = hk.id_ho_khau
        WHERE tv.da_tu_vong = FALSE
        AND dc.ten_dia_chi LIKE '%Quận JohnPhường%'
        ORDER BY tv.ten
        """,
        [
            {"$match": {
                "da_tu_vong": False,
                "dia_chi.ten_dia_chi": {"$regex": "Quận JohnPhường", "$options": "i"}
            }},
            {"$project": {
                "_id": 0,
                "ho_ten": "$ten",
                "gioi_tinh": 1,
                "ngay_sinh": 1,
                "ten_chu_ho": "$ho_khau.ten_chu_ho",
                "dia_chi": {
                    "$filter": {
                        "input": "$dia_chi",
                        "as": "dc",
                        "cond": {"$regexMatch": {
                            "input": "$$dc.ten_dia_chi",
                            "regex": "Quận JohnPhường",
                            "options": "i"
                        }}
                    }
                }
            }},
            {"$sort": {"ho_ten": 1}}
        ]
    )
    # Query for people with more than 1 address
    benchmark.add_query(
        "Multiple Addresses",
        """
        SELECT tv.cccd, tv.ten, COUNT(tdc.dia_chi_id) AS address_count
        FROM thanh_vien tv
        JOIN thanh_vien_dia_chi tdc ON tv.cccd = tdc.thanh_vien_cccd
        WHERE tv.da_tu_vong = FALSE
        GROUP BY tv.cccd, tv.ten
        HAVING COUNT(tdc.dia_chi_id) > 1
        """,
        [
            {"$match": {
                "da_tu_vong": False,
                "$expr": {"$gt": [{"$size": "$dia_chi"}, 1]}
            }}
        ]
    )

    # Find the number of household members (thanh_vien) per gender who
    # lived at more than 2 different addresses for more than 5 years total,
    # grouped by ethnic group (dan_toc) and gender (gioi_tinh) and ordered by total duration.
    benchmark.add_query(
        " Household Members",
        """
                SELECT 
            gioi_tinh,
            dan_toc,
            COUNT(*) AS so_nguoi,
            SUM(tong_so_ngay_o) AS tong_so_ngay
        FROM (
            SELECT 
                tv.cccd,
                tv.gioi_tinh,
                tv.dan_toc,
                COUNT(DISTINCT tvd.dia_chi_id) AS so_dia_chi,
                SUM(DATEDIFF(IFNULL(tvd.den_ngay, CURDATE()), tvd.tu_ngay)) AS tong_so_ngay_o
            FROM 
                thanh_vien tv
            JOIN 
                thanh_vien_dia_chi tvd ON tv.cccd = tvd.thanh_vien_cccd
            GROUP BY 
                tv.cccd, tv.gioi_tinh, tv.dan_toc
            HAVING 
                COUNT(DISTINCT tvd.dia_chi_id) > 2 AND
                SUM(DATEDIFF(IFNULL(tvd.den_ngay, CURDATE()), tvd.tu_ngay)) > 365 * 5
        ) sub
        GROUP BY 
            gioi_tinh, dan_toc
        ORDER BY 
            tong_so_ngay DESC;

        """
        ,
        [
            {"$unwind": "$dia_chi"},
            {"$addFields": {
                "ngay_o": {
                    "$divide": [
                        {"$subtract": [
                            {"$ifNull": [{"$toDate": "$dia_chi.den_ngay"}, datetime.utcnow()]},
                            {"$toDate": "$dia_chi.tu_ngay"}
                        ]},
                        1000 * 60 * 60 * 24  # milliseconds to days
                    ]
                }
            }},
            {"$group": {
                "_id": "$cccd",
                "ten": {"$first": "$ten"},
                "gioi_tinh": {"$first": "$gioi_tinh"},
                "dan_toc": {"$first": "$dan_toc"},
                "tong_ngay_o": {"$sum": "$ngay_o"},
                "so_dia_chi": {"$addToSet": "$dia_chi.id_dia_chi"}
            }},
            {"$addFields": {
                "so_dia_chi": {"$size": "$so_dia_chi"}
            }},
            {"$match": {
                "so_dia_chi": {"$gt": 2},
                "tong_ngay_o": {"$gt": 365 * 5}
            }},
            {"$group": {
                "_id": {"gioi_tinh": "$gioi_tinh", "dan_toc": "$dan_toc"},
                "so_nguoi": {"$sum": 1},
                "tong_so_ngay": {"$sum": "$tong_ngay_o"}
            }},
            {"$sort": {"tong_so_ngay": -1}}
        ]
    )
    #Find household heads (chu_ho) who have:
    # More than 3 family members (quan_he ≠ "Chủ hộ")
    # At least one member has moved through more than 3 different addresses
    # Their members stayed at any address for more than 3 years
    benchmark.add_query(
        "Find complex household",
        """
        SELECT 
            hk.cccd_chu_ho,
            hk.ten_chu_ho,
            COUNT(DISTINCT tv.cccd) AS so_thanh_vien,
            MAX(addr_stats.so_dia_chi) AS max_dia_chi_per_thanh_vien,
            MAX(addr_stats.max_so_ngay_o) AS max_ngay_o
        FROM ho_khau hk
        JOIN thanh_vien tv ON tv.id_ho_khau = hk.id_ho_khau
        JOIN (
            SELECT 
                tvd.thanh_vien_cccd,
                COUNT(*) AS so_dia_chi,
                MAX(DATEDIFF(COALESCE(tvd.den_ngay, CURDATE()), tvd.tu_ngay)) AS max_so_ngay_o
            FROM thanh_vien_dia_chi tvd
            GROUP BY tvd.thanh_vien_cccd
        ) addr_stats ON addr_stats.thanh_vien_cccd = tv.cccd
        WHERE tv.da_tu_vong = FALSE
        GROUP BY hk.cccd_chu_ho, hk.ten_chu_ho
        HAVING 
            so_thanh_vien > 3 AND 
            max_dia_chi_per_thanh_vien > 3 AND 
            max_ngay_o > 365 * 3
        ORDER BY so_thanh_vien DESC;

        """,
        [
            {"$match": {"ho_khau.quan_he": {"$ne": "Chủ hộ"}}},
            {"$unwind": "$dia_chi"},
            {"$addFields": {
                "ngay_o": {
                    "$divide": [
                        {"$subtract": [
                            {"$ifNull": [{"$toDate": "$dia_chi.den_ngay"}, datetime.utcnow()]},
                            {"$toDate": "$dia_chi.tu_ngay"}
                        ]},
                        1000 * 60 * 60 * 24
                    ]
                }
            }},
            {"$group": {
                "_id": "$cccd",
                "ten": {"$first": "$ten"},
                "cccd_chu_ho": {"$first": "$ho_khau.cccd_chu_ho"},
                "ten_chu_ho": {"$first": "$ho_khau.ten_chu_ho"},
                "so_dia_chi": {"$addToSet": "$dia_chi.id_dia_chi"},
                "max_ngay_o": {"$max": "$ngay_o"}
            }},
            {"$addFields": {
                "so_dia_chi": {"$size": "$so_dia_chi"}
            }},
            {"$match": {
                "so_dia_chi": {"$gt": 3},
                "max_ngay_o": {"$gt": 365 * 3}
            }},
            {"$group": {
                "_id": "$cccd_chu_ho",
                "ten_chu_ho": {"$first": "$ten_chu_ho"},
                "so_thanh_vien": {"$sum": 1},
                "max_dia_chi_per_thanh_vien": {"$max": "$so_dia_chi"},
                "max_ngay_o": {"$max": "$max_ngay_o"}
            }},
            {"$match": {"so_thanh_vien": {"$gt": 3}}},
            {"$sort": {"so_thanh_vien": -1}}
        ]
    )


    # Run and show results
    benchmark.run()
    # concurrent write 1000
    num = benchmark.inserts_per_thread * benchmark.threads
    benchmark.benchmark_concurrent_write_mysql(num)
    benchmark.benchmark_concurrent_write_mongo(num)
    # concurrent write 10000
    benchmark.inserts_per_thread = 1000
    num = benchmark.inserts_per_thread * benchmark.threads
    benchmark.benchmark_concurrent_write_mysql(num)
    benchmark.benchmark_concurrent_write_mongo(num)
    # concurrent write 100000
    benchmark.inserts_per_thread = 10000
    num = benchmark.inserts_per_thread * benchmark.threads
    benchmark.benchmark_concurrent_write_mysql(num)
    benchmark.benchmark_concurrent_write_mongo(num)
    # concurrent update 1000
    benchmark.inserts_per_thread = 100
    num = benchmark.inserts_per_thread * benchmark.threads
    benchmark.benchmark_concurrent_update(num)

    # concurrent update 10000
    benchmark.inserts_per_thread = 1000
    num = benchmark.inserts_per_thread * benchmark.threads
    benchmark.benchmark_concurrent_update(num)

    # concurrent update 100000
    benchmark.inserts_per_thread = 10000
    num = benchmark.inserts_per_thread * benchmark.threads
    benchmark.benchmark_concurrent_update(num)

    # concurrent update 100000
    benchmark.plot_results()
    benchmark.close()
