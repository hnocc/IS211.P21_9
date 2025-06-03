from pymongo import MongoClient
from tabulate import tabulate
from datetime import datetime, timedelta

# Kết nối MongoDB
client = MongoClient(
    host="26.29.12.16",
    port=40000,
    username="nguyen",
    password="22520978",
    authSource="admin"
)

db = client["BTL2"]
collection_dk = db["DANGKY"]

# Hàm hiển thị 
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

# Thời điểm 1 năm trước
one_year_ago = datetime.now() - timedelta(days=365)

# Pipeline: chuyên ngành có nhiều HV nhất tại mỗi trung tâm
pipeline3 = [
    {
        "$lookup": {
            "from": "LOPHOC",
            "localField": "maLop",
            "foreignField": "_id",
            "as": "lop"
        }
    },
    {"$unwind": "$lop"},

    # Ép kiểu ngày nếu đang lưu dạng string
    {
        "$addFields": {
            "ngayBatDauParsed": {
                "$dateFromString": {
                    "dateString": "$lop.ngayBatDau"
                }
            }
        }
    },

    # Chỉ xét lớp trong 1 năm gần nhất
    {
        "$match": {
            "ngayBatDauParsed": {"$gte": one_year_ago}
        }
    },

    # Gom theo chuyên ngành và trung tâm
    {
        "$group": {
            "_id": {
                "tenKhoaHoc": "$lop.tenKhoaHoc",
                "maTrungTam": "$lop.maTrungTam"
            },
            "soHocVien": {"$sum": 1}
        }
    },

    # Gom tiếp theo trung tâm để lấy chuyên ngành có số HV cao nhất
    {
        "$sort": {"_id.maTrungTam": 1, "soHocVien": -1}
    },
    {
        "$group": {
            "_id": "$_id.maTrungTam",
            "chuyenNganh": {"$first": "$_id.tenKhoaHoc"},
            "soHocVien": {"$first": "$soHocVien"}
        }
    },

    {"$sort": {"_id": 1}}
]

# Chạy truy vấn
run_query("3. Chuyên ngành có nhiều học viên nhất tại mỗi trung tâm (1 năm gần đây)", pipeline3, collection_dk)

client.close()
