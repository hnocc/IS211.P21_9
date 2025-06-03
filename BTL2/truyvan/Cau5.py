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

# ===== TRUY VẤN: Lớp có số lượng học viên đăng ký nhiều nhất =====
print("\n 5. Lớp có số lượng học viên đăng ký nhiều nhất:")

dangky_col = db["DANGKY"]
pipeline5 = [
    {
        "$group": {
            "_id": "$maLop",
            "soLuongDangKy": { "$sum": 1 }
        }
    },
    { "$sort": { "soLuongDangKy": -1 } },
    { "$limit": 1 },
    {
        "$lookup": {
            "from": "LOPHOC",
            "localField": "_id",
            "foreignField": "_id",
            "as": "lophoc"
        }
    },
    { "$unwind": "$lophoc" },
    {
        "$project": {
            "_id": 0,
            "Mã lớp": "$_id",
            "Tên khóa học": "$lophoc.tenKhoaHoc",
            "Trình độ": "$lophoc.trinhDo",
            "Giảng viên": "$lophoc.maGiangVien",
            "Số lượng học viên": "$soLuongDangKy"
        }
    }
]

result2 = list(dangky_col.aggregate(pipeline5))
if result2:
    print(tabulate(result2, headers="keys", tablefmt="grid"))
else:
    print("Không có dữ liệu đăng ký.")

client.close()