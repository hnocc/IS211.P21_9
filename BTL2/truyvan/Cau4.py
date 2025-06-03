from pymongo import MongoClient
from tabulate import tabulate

# ===== CẤU HÌNH KẾT NỐI MONGOS =====
MONGOS_HOST = "26.29.12.16"
MONGOS_PORT = 40000
USERNAME = "nguyen"
PASSWORD = "22520978"
AUTH_DB = "admin"

# ===== KẾT NỐI ĐẾN MONGODB =====
client = MongoClient(
    host=MONGOS_HOST,
    port=MONGOS_PORT,
    username=USERNAME,
    password=PASSWORD,
    authSource=AUTH_DB
)

db = client["BTL2"]

# ===== TRUY VẤN: Thống kê lớp học và học phí theo trung tâm =====
print("\n 4. Thống kê số lớp học, tổng học phí và trung bình học phí của từng trung tâm:")

lophoc_col = db["LOPHOC"]
pipeline4 = [
    {
        "$group": {
            "_id": "$maTrungTam",
            "soLop": { "$sum": 1 },
            "tongHocPhi": { "$sum": "$hocPhi" },
            "trungBinhHocPhi": { "$avg": "$hocPhi" }
        }
    },
    {
        "$lookup": {
            "from": "TRUNGTAM",
            "localField": "_id",
            "foreignField": "_id",
            "as": "trungtam"
        }
    },
    { "$unwind": "$trungtam" },
    {
        "$project": {
            "_id": 0,
            "Trung tâm": "$trungtam.ten",
            "Số lớp": "$soLop",
            "Tổng học phí": "$tongHocPhi",
            "Trung bình học phí": { "$round": [ "$trungBinhHocPhi", 2 ] }
        }
    }
]

result1 = list(lophoc_col.aggregate(pipeline4))
if result1:
    print(tabulate(result1, headers="keys", tablefmt="grid"))
else:
    print("Không có dữ liệu lớp học.")

client.close()