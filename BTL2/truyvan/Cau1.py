from pymongo import MongoClient
from pprint import pprint
from tabulate import tabulate
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
collection = db["GIANGVIEN"]

# ===== Truy vấn giảng viên TT001 =====
print("Dữ liệu giảng viên ở TT001:")
docs_tt001 = list(collection.find({ "maTrungTam": "TT001" }))
if docs_tt001:
    headers = docs_tt001[0].keys()
    rows = [doc.values() for doc in docs_tt001]
    print(tabulate(rows, headers=headers, tablefmt="grid"))
else:
    print("Không có dữ liệu TT001.")

# ===== Explain truy vấn TT001 =====
print("\n Explain truy vấn maTrungTam=TT001:")
explain_tt001 = collection.find({ "maTrungTam": "TT001" }).explain()
shard_tt001 = explain_tt001['queryPlanner']['winningPlan']['shards'][0]
print(f"→ Shard name: {shard_tt001['shardName']}")
print(f"→ Server info: {shard_tt001['serverInfo']}")

# ===== Truy vấn giảng viên TT002 =====
print("\n Dữ liệu giảng viên ở TT002:")
docs_tt002 = list(collection.find({ "maTrungTam": "TT002" }))
if docs_tt002:
    headers = docs_tt002[0].keys()
    rows = [doc.values() for doc in docs_tt002]
    print(tabulate(rows, headers=headers, tablefmt="grid"))
else:
    print("Không có dữ liệu TT002.")

# ===== Explain truy vấn TT002 =====
print("\n Explain truy vấn maTrungTam=TT002:")
explain_tt002 = collection.find({ "maTrungTam": "TT002" }).explain()
shard_tt002 = explain_tt002['queryPlanner']['winningPlan']['shards'][0]
print(f"→ Shard name: {shard_tt002['shardName']}")
print(f"→ Server info: {shard_tt002['serverInfo']}")

# ===== Truy vấn toàn bộ giảng viên =====
print("\n Danh sách toàn bộ giảng viên:")
docs_all = list(collection.find({}))
if docs_all:
    headers = docs_all[0].keys()
    rows = [doc.values() for doc in docs_all]
    print(tabulate(rows, headers=headers, tablefmt="grid"))
else:
    print("Không có giảng viên nào.")

# ===== Explain truy vấn toàn bộ =====
print("\n Explain truy vấn toàn bộ giảng viên:")
explain_all = collection.find({}).explain()
for shard in explain_all['queryPlanner']['winningPlan']['shards']:
    print(f"→ Shard name: {shard['shardName']}")
    print(f"→ Server info: {shard['serverInfo']}")

# ===== Đóng kết nối =====
client.close()