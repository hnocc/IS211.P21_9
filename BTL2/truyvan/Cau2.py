from pymongo import MongoClient
from tabulate import tabulate
from datetime import datetime

# Kết nối MongoDB
client = MongoClient(
    host="26.29.12.16",
    port=40000,
    username="nguyen",
    password="22520978",
    authSource="admin"
)

db = client["BTL2"]
collection_lop = db["LOPHOC"]

# Hàm in kết quả
def run_query(name, pipeline, collection):
    print(f"\n {name}")
    docs = list(collection.aggregate(pipeline))
    if not docs:
        print("Không có dữ liệu phù hợp.")
        return

    rows = []
    headers = set()

    for doc in docs:
        row = {}
        if isinstance(doc.get("_id"), dict):
            row.update(doc["_id"])
        else:
            row["_id"] = doc.get("_id")
        for k, v in doc.items():
            if k != "_id":
                row[k] = v
        headers.update(row.keys())
        rows.append(row)

    headers = list(headers)
    print(tabulate([[r.get(h, "") for h in headers] for r in rows], headers=headers, tablefmt="grid"))

# Tính quý hiện tại
now = datetime.now()
current_quarter = (now.month - 1) // 3 + 1
current_year = now.year

# Pipeline: Tính doanh thu học phí theo chuyên ngành và trung tâm
pipeline2 = [
    {
        "$addFields": {
            "ngayBatDauParsed": {
                "$dateFromString": {
                    "dateString": "$ngayBatDau"
                }
            }
        }
    },
    {
        "$addFields": {
            "quy": {"$ceil": {"$divide": [{"$month": "$ngayBatDauParsed"}, 3]}},
            "nam": {"$year": "$ngayBatDauParsed"}
        }
    },
    {
        "$match": {
            "quy": current_quarter,
            "nam": current_year
        }
    },
    {
        "$lookup": {
            "from": "DANGKY",
            "localField": "_id",
            "foreignField": "maLop",
            "as": "dsDangKy"
        }
    },
    {
        "$addFields": {
            "soHocVien": {"$size": "$dsDangKy"}
        }
    },
    {
        "$project": {
            "tenKhoaHoc": 1,
            "maTrungTam": 1,
            "hocPhi": 1,
            "doanhThu": {"$multiply": ["$hocPhi", "$soHocVien"]}
        }
    },
    {
        "$group": {
            "_id": {
                "tenKhoaHoc": "$tenKhoaHoc",
                "maTrungTam": "$maTrungTam"
            },
            "tongDoanhThu": {"$sum": "$doanhThu"}
        }
    },
    {"$sort": {"_id.maTrungTam": 1, "_id.tenKhoaHoc": 1}}
]

# Chạy truy vấn
run_query("2. Doanh thu học phí quý gần nhất theo tên khóa học", pipeline2, collection_lop)

client.close()
