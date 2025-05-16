import random
import string
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import mysql.connector
from loguru import logger
from pymongo import MongoClient, UpdateOne
import matplotlib.pyplot as plt

from clients import mongo_config
from utils import divide_chunks, scale_list_repeat


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
        self.inserted_cccds_mysql = []
        self.inserted_cccds_mongo = []
        self.results = []

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

    def plot_results(self, name: str):
        names = [r[0] for r in self.results]
        mysql_times = [r[1] for r in self.results]
        mongo_times = [r[2] for r in self.results]

        x = range(len(names))
        width = 0.35

        fig, ax = plt.subplots(figsize=(14, 6))
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
                ax.annotate(f'{time:.3f}',
                            xy=(bar.get_x() + bar.get_width() / 2, height),
                            xytext=(0, 3),  # offset
                            textcoords="offset points",
                            ha='center', va='bottom', fontsize=8)

        add_labels(mysql_bars, mysql_times)
        add_labels(mongo_bars, mongo_times)

        plt.tight_layout()
        plt.savefig(f"{name}.png", dpi=300, bbox_inches='tight')  # Save the image
        plt.show()

    def _random_cccd(self):
        return ''.join(random.choices(string.digits, k=14))

    def _random_name(self):
        return ''.join(random.choices(string.ascii_letters + " ", k=10))

    def _insert_mysql(self, batch):
        conn = mysql.connector.connect(**self.mysql_config)
        cursor = conn.cursor()
        query = """
            INSERT INTO thanh_vien (cccd, ten, ngay_sinh, gioi_tinh)
            VALUES (%s, %s, %s, %s)
        """
        for record in batch:
            cursor.execute(query, record)
        conn.commit()
        cursor.close()
        conn.close()

    def _insert_mongo(self, docs):
        for doc in docs:
            self.mongo_col.insert_one(doc)

    def _update_mysql(self, updates):
        conn = mysql.connector.connect(**self.mysql_config)
        cursor = conn.cursor()
        query = "UPDATE thanh_vien SET ten = %s WHERE cccd = %s"
        for new_name, cccd in updates:
            cursor.execute(query, (new_name, cccd))
        conn.commit()
        cursor.close()
        conn.close()

    def _update_mongo(self, updates):
        client = MongoClient(mongo_config["uri"])
        col = client[mongo_config["db"]][mongo_config["collection"]]

        for cccd, new_name in updates:
            col.update_one({"cccd": cccd}, {"$set": {"ten": new_name}})

        client.close()

    def benchmark_concurrent_insert(self, num: int):
        # mysql insert
        print("Running concurrent insert benchmark...")
        batch = []
        for _ in range(num):
            name = self._random_name()
            cccd = self._random_cccd()
            batch.append((cccd, name, '1990-01-01', 'Khác'))
            self.inserted_cccds_mysql.append(cccd)

        chunks = divide_chunks(batch, self.threads)
        start = time.perf_counter()
        with ThreadPoolExecutor(max_workers=self.threads) as executor:
            mysql_futures = [executor.submit(self._insert_mysql, chunk) for chunk in chunks]
        mysql_time = time.perf_counter() - start


        # mongodb insert
        docs = []
        for _ in range(num):
            cccd = self._random_cccd()
            self.inserted_cccds_mongo.append(cccd)
            docs.append({
                "ten": self._random_name(),
                "cccd": self._random_cccd(),
                "ngay_sinh": "1990-01-01",
                "gioi_tinh": "Khác"
            })
        chunks = divide_chunks(docs, self.threads)
        start = time.perf_counter()
        with ThreadPoolExecutor(max_workers=self.threads) as executor:
            mongo_futures = [executor.submit(self._insert_mongo, chunk) for chunk in chunks]
        mongo_time = time.perf_counter() - start

        self.results.append((f"Concurrent Insert {num}", mysql_time, mongo_time))
        print(f"MySQL insert: {mysql_time:.4f}s, MongoDB insert: {mongo_time:.4f}s")

    def benchmark_concurrent_update(self, num: int):
        print("Running concurrent update benchmark...")
        updates = []
        inserted_cccds = scale_list_repeat(self.inserted_cccds_mysql, num)
        logger.info(f"update mysql {len(inserted_cccds)}")
        for i in range(num):
            if i < len(inserted_cccds):
                cccd = inserted_cccds[i]
                new_name = self._random_name()
                updates.append((new_name, cccd))
        chunks = divide_chunks(updates, self.threads)

        start = time.perf_counter()
        with ThreadPoolExecutor(max_workers=self.threads) as executor:
            mysql_futures = [executor.submit(self._update_mysql, chunk) for chunk in chunks]
            for future in mysql_futures:
                future.result()
        mysql_time = time.perf_counter() - start

        updates = []
        inserted_cccds = scale_list_repeat(self.inserted_cccds_mongo, num)
        logger.info(f"update mongo {len(inserted_cccds)}")
        for i in range(num):
            if i < len(inserted_cccds):
                cccd = inserted_cccds[i]
                new_name = self._random_name()
                updates.append((cccd, new_name))
        chunks = divide_chunks(updates, self.threads)

        start = time.perf_counter()
        with ThreadPoolExecutor(max_workers=self.threads) as executor:
            mongo_futures = [executor.submit(self._update_mongo, chunk) for chunk in chunks]
            for future in mongo_futures:
                future.result()
        mongo_time = time.perf_counter() - start

        self.results.append((f"Concurrent Update {num}", mysql_time, mongo_time))
        print(f"MySQL update: {mysql_time:.4f}s, MongoDB update: {mongo_time:.4f}s")

    def close(self):
        self.mysql_cursor.close()
        self.mysql_conn.close()
        self.mongo_client.close()


# ---------------------- Script Usage ----------------------

