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

db.GIANGVIEN.update_one(
    { "_id": "GV111" },
    { "$set": { "email": "demoupdate_lan2@example.com" } }
)


print(" Cập nhật thành công.")
client.close()