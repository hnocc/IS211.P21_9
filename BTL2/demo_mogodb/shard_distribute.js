sh.enableSharding("BTL2")

// === 1. TRUNGTAM ===
db = db.getSiblingDB("BTL2")
db.TRUNGTAM.createIndex({ _id: "hashed" })
sh.shardCollection("BTL2.TRUNGTAM", { _id: "hashed" })

// === 2. GIANGVIEN ===
db.GIANGVIEN.createIndex({ maTrungTam: 1 })
sh.shardCollection("BTL2.GIANGVIEN", { maTrungTam: 1 })
sh.splitAt("BTL2.GIANGVIEN", { maTrungTam: "TT002" })
sh.moveChunk("BTL2.GIANGVIEN", { maTrungTam: "TT001" }, "BTL2_Shard1")
sh.moveChunk("BTL2.GIANGVIEN", { maTrungTam: "TT002" }, "BTL2_Shard2")

// === 3. HOCVIEN ===
db.HOCVIEN.createIndex({ trungtam_id: 1 })
sh.shardCollection("BTL2.HOCVIEN", { trungtam_id: 1 })
sh.splitAt("BTL2.HOCVIEN", { trungtam_id: "TT002" })
sh.moveChunk("BTL2.HOCVIEN", { trungtam_id: "TT001" }, "BTL2_Shard1")
sh.moveChunk("BTL2.HOCVIEN", { trungtam_id: "TT002" }, "BTL2_Shard2")

// === 4. NHANVIEN ===
db.NHANVIEN.createIndex({ trungtam_id: 1 })
sh.shardCollection("BTL2.NHANVIEN", { trungtam_id: 1 })
sh.splitAt("BTL2.NHANVIEN", { trungtam_id: "TT002" })
sh.moveChunk("BTL2.NHANVIEN", { trungtam_id: "TT001" }, "BTL2_Shard1")
sh.moveChunk("BTL2.NHANVIEN", { trungtam_id: "TT002" }, "BTL2_Shard2")

// === 5. LOPHOC ===
db.LOPHOC.createIndex({ maTrungTam: 1 })
sh.shardCollection("BTL2.LOPHOC", { maTrungTam: 1 })
sh.splitAt("BTL2.LOPHOC", { maTrungTam: "TT002" })
sh.moveChunk("BTL2.LOPHOC", { maTrungTam: "TT001" }, "BTL2_Shard1")
sh.moveChunk("BTL2.LOPHOC", { maTrungTam: "TT002" }, "BTL2_Shard2")

// === 6. DANGKY ===
db.DANGKY.createIndex({ maLop: 1 })
sh.shardCollection("BTL2.DANGKY", { maLop: 1 })
// ⚠️ Nếu bạn có lớp LH001 - LH003 thuộc TT001, LH004 - LH006 thuộc TT002
sh.splitAt("BTL2.DANGKY", { maLop: "LH004" })
sh.moveChunk("BTL2.DANGKY", { maLop: "LH001" }, "BTL2_Shard1")
sh.moveChunk("BTL2.DANGKY", { maLop: "LH004" }, "BTL2_Shard2")

print("\n✅ Hoàn tất shard + phân phối chunk!")
