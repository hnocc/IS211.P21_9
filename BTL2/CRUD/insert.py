from pymongo import MongoClient

# ===== Cấu hình kết nối mongos =====
MONGOS_HOST = "26.29.12.16"   
MONGOS_PORT = 40000           
USERNAME = "nguyen"            
PASSWORD = "22520978"    
AUTH_DB = "admin"

# ===== Kết nối tới mongos =====
client = MongoClient(
    host=MONGOS_HOST,
    port=MONGOS_PORT,
    username=USERNAME,
    password=PASSWORD,
    authSource=AUTH_DB
)

# ===== Truy cập vào database và collection sharded =====
db = client["BTL2"]

# Thêm một giảng viên
giangvien = {
"_id": "GV555",
  "hoTen": "Nguyễn Minh Nhựt",
  "email": "NguyenMinhNhut@example.com",
  "soDienThoai": "0126794284",
  "chuyenMon": "CSDLPT",
  "maTrungTam": "TT001"
}

db.GIANGVIEN.insert_one(giangvien)

print(" Đã thêm giảng viên thành công")
client.close()
