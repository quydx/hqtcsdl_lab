from datetime import datetime

from clients import mysql_config, mongo_config
from benchmarker import QueryBenchmark

if __name__ == "__main__":
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
    # concurrent write

    # concurrent update 100000
    benchmark.plot_results("queries")
    benchmark.close()
