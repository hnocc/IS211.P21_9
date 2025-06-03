-- Procedure: PROC_CHUYEN_KHO

CREATE OR REPLACE PROCEDURE PROC_CHUYEN_KHO (
    p_sanpham_id      IN NUMBER,
    p_chinhanh_nguon  IN VARCHAR2,
    p_chinhanh_nhan   IN VARCHAR2,
    p_soluong         IN NUMBER
)
IS
    v_link_nguon   VARCHAR2(100);
    v_link_nhan    VARCHAR2(100);
    v_sl_hienco    NUMBER;
BEGIN
    -- Xác định DB LINK tương ứng
    v_link_nguon := CASE p_chinhanh_nguon
                  WHEN 'CN1' THEN 'CN1.KHOSANPHAM_QLKHO'
                  WHEN 'CN2' THEN 'CN2.KHOSANPHAM_QLKHO@CN2_LINK'
                  WHEN 'CN3' THEN 'CN3.KHOSANPHAM_QLKHO@CN3_LINK'
               END;

    v_link_nhan := CASE p_chinhanh_nhan
                  WHEN 'CN1' THEN 'CN1.KHOSANPHAM_QLKHO'
                  WHEN 'CN2' THEN 'CN2.KHOSANPHAM_QLKHO@CN2_LINK'
                  WHEN 'CN3' THEN 'CN3.KHOSANPHAM_QLKHO@CN3_LINK'
               END;


    -- Kiểm tra số lượng hiện có tại chi nhánh nguồn
    BEGIN
        EXECUTE IMMEDIATE '
            SELECT SOLUONGNHAP
            FROM ' || v_link_nguon || '
            WHERE SANPHAM_ID = :1 AND CHINHANH_ID = :2'
        INTO v_sl_hienco
        USING p_sanpham_id, p_chinhanh_nguon;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            DBMS_OUTPUT.PUT_LINE('Không tìm thấy sản phẩm tại chi nhánh nguồn.');
            ROLLBACK;
            RETURN;
    END;

    -- Kiểm tra có đủ số lượng không
    IF v_sl_hienco < p_soluong THEN
        DBMS_OUTPUT.PUT_LINE('Không đủ hàng để chuyển. Số lượng hiện có tại CN' || p_chinhanh_nguon || ': ' || v_sl_hienco);
        ROLLBACK;
        RETURN;
    END IF;

    -- Bắt đầu khối giao dịch chuyển kho
    BEGIN
        -- 1. Trừ hàng ở chi nhánh nguồn
        EXECUTE IMMEDIATE '
            UPDATE ' || v_link_nguon || '
            SET SOLUONGNHAP = SOLUONGNHAP - :1
            WHERE SANPHAM_ID = :2 AND CHINHANH_ID = :3'
        USING p_soluong, p_sanpham_id, p_chinhanh_nguon;

        -- 2. Nếu hết hàng tại nguồn → xóa dòng
        IF v_sl_hienco = p_soluong THEN
            EXECUTE IMMEDIATE '
                DELETE FROM ' || v_link_nguon || '
                WHERE SANPHAM_ID = :1 AND CHINHANH_ID = :2'
            USING p_sanpham_id, p_chinhanh_nguon;

            DBMS_OUTPUT.PUT_LINE('Đã xóa dòng kho tại CN' || p_chinhanh_nguon || ' vì hết hàng.');
        END IF;

        -- 3. Cộng vào kho chi nhánh nhận
        BEGIN
            EXECUTE IMMEDIATE '
                UPDATE ' || v_link_nhan || '
                SET SOLUONGNHAP = SOLUONGNHAP + :1
                WHERE SANPHAM_ID = :2 AND CHINHANH_ID = :3'
            USING p_soluong, p_sanpham_id, p_chinhanh_nhan;
        -- Nếu chưa có thì thêm dòng mới
            IF SQL%ROWCOUNT = 0 THEN
                EXECUTE IMMEDIATE '
                    INSERT INTO ' || v_link_nhan || '
                    (CHINHANH_ID, SANPHAM_ID, SOLUONGNHAP, NGAYCAPNHAT)
                    VALUES (:1, :2, :3, SYSDATE)'
                USING p_chinhanh_nhan, p_sanpham_id, p_soluong;

                DBMS_OUTPUT.PUT_LINE('Đã thêm dòng kho mới tại CN' || p_chinhanh_nhan);
            END IF;
        END;

        -- 4. Commit toàn bộ nếu thành công
        COMMIT;
        DBMS_OUTPUT.PUT_LINE('Đã chuyển thành công ' || p_soluong || ' sản phẩm (ID=' || p_sanpham_id || ') từ CN' ||
                             p_chinhanh_nguon || ' sang CN' || p_chinhanh_nhan);
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            DBMS_OUTPUT.PUT_LINE('Lỗi khi chuyển kho: ' || SQLERRM);
    END;

END;
/

EXEC PROC_CHUYEN_KHO(101, 'CN1', 'CN2', 8);

SELECT * FROM CN1.KHOSANPHAM_QLKHO  WHERE SANPHAM_ID = 101;

SELECT * FROM CN2.KHOSANPHAM_QLKHO@CN2_LINK WHERE SANPHAM_ID = 101;





-- Giải thích tham số:
-- 101 → Mã sản phẩm (SANPHAM_ID)
-- 2   → Mã chi nhánh nguồn (CHINHANH_ID)
-- 1   → Mã chi nhánh đích
-- 5   → Số lượng sản phẩm cần chuyển


SELECT * FROM CN2.KHOSANPHAM_QLKHO@CN2_NVKHO_LINK;
