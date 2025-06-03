CREATE OR REPLACE PROCEDURE PROC_XULY_KHUYENMAI_HETHONG (
    p_thuonghieu IN VARCHAR2
)
IS
    v_dt_th   NUMBER := 0;  -- Doanh thu của thương hiệu
    v_dt_all  NUMBER := 0;  -- Doanh thu toàn hệ thống
    v_tile    NUMBER := 0;  -- Tỷ lệ doanh thu thương hiệu

    -- Khoảng thời gian: từ đầu quý đến hiện tại
    p_tungay  DATE := TRUNC(SYSDATE, 'Q');
    p_denngay DATE := TRUNC(SYSDATE);
BEGIN
    --------------------------------------------------------------------
    -- 1. Tính tổng doanh thu của thương hiệu theo các chi nhánh
    --------------------------------------------------------------------
    SELECT SUM(DOANHTHU) INTO v_dt_th FROM (
        SELECT SP.GIABAN * CT.SOLUONG AS DOANHTHU
        FROM CN1.CTHD CT
        JOIN CN1.HOADON HD ON CT.HOADON_ID = HD.HOADON_ID
        JOIN CN1.SANPHAM SP ON CT.SANPHAM_ID = SP.SANPHAM_ID
        WHERE SP.THUONGHIEU = p_thuonghieu AND HD.NGAYTAO BETWEEN p_tungay AND p_denngay

        UNION ALL

        SELECT SP.GIABAN * CT.SOLUONG AS DOANHTHU
        FROM CN2.CTHD@CN2_LINK CT
        JOIN CN2.HOADON@CN2_LINK HD ON CT.HOADON_ID = HD.HOADON_ID
        JOIN CN2.SANPHAM@CN2_LINK SP ON CT.SANPHAM_ID = SP.SANPHAM_ID
        WHERE SP.THUONGHIEU = p_thuonghieu AND HD.NGAYTAO BETWEEN p_tungay AND p_denngay

        UNION ALL

        SELECT SP.GIABAN * CT.SOLUONG AS DOANHTHU
        FROM CN3.CTHD@CN3_LINK CT
        JOIN CN3.HOADON@CN3_LINK HD ON CT.HOADON_ID = HD.HOADON_ID
        JOIN CN3.SANPHAM@CN3_LINK SP ON CT.SANPHAM_ID = SP.SANPHAM_ID
        WHERE SP.THUONGHIEU = p_thuonghieu AND HD.NGAYTAO BETWEEN p_tungay AND p_denngay
    );

    --------------------------------------------------------------------
    -- 2. Tính tổng doanh thu toàn hệ thống (mọi thương hiệu)
    --------------------------------------------------------------------
    SELECT SUM(TONGTIEN) AS DOANHTHU INTO v_dt_all FROM (
        SELECT HD.TONGTIEN
        FROM CN1.HOADON HD
        WHERE HD.NGAYTAO BETWEEN p_tungay AND p_denngay

        UNION ALL

        SELECT HD.TONGTIEN
        FROM CN2.HOADON@CN2_LINK HD
        WHERE HD.NGAYTAO BETWEEN p_tungay AND p_denngay

        UNION ALL

        SELECT HD.TONGTIEN
        FROM CN3.HOADON@CN3_LINK HD
        WHERE HD.NGAYTAO BETWEEN p_tungay AND p_denngay
    );

    --------------------------------------------------------------------
    -- 3. So sánh và xử lý khuyến mãi nếu doanh thu quá thấp
    --------------------------------------------------------------------
    IF v_dt_th IS NULL THEN
        DBMS_OUTPUT.PUT_LINE('Quý này thương hiệu "' || p_thuonghieu || '" chưa bán được sản phẩm nào.');
    ELSE
        v_tile := ROUND(v_dt_th / NULLIF(v_dt_all, 0) * 100, 2);
    
        IF v_tile < 5 THEN
            DBMS_OUTPUT.PUT_LINE('Doanh thu thấp: ' || v_tile || '%. Áp dụng giảm giá có kiểm tra.');
    
            -- Áp dụng giảm 10% GIABAN nhưng không được nhỏ hơn GIANHAP
            -- ROUND để đảm bảo giá giảm là số nguyên đồng
    
            -- CN1
            UPDATE CN1.SANPHAM
            SET GIABAN = ROUND(GIABAN * 0.9, 0)
            WHERE THUONGHIEU = p_thuonghieu AND ROUND(GIABAN * 0.9, 0) >= GIANHAP;
    
            -- CN2
            UPDATE CN2.SANPHAM@CN2_LINK
            SET GIABAN = ROUND(GIABAN * 0.9, 0)
            WHERE THUONGHIEU = p_thuonghieu AND ROUND(GIABAN * 0.9, 0) >= GIANHAP;
    
            -- CN3
            UPDATE CN3.SANPHAM@CN3_LINK
            SET GIABAN = ROUND(GIABAN * 0.9, 0)
            WHERE THUONGHIEU = p_thuonghieu AND ROUND(GIABAN * 0.9, 0) >= GIANHAP;
    
            COMMIT;
        ELSE
            DBMS_OUTPUT.PUT_LINE('Doanh thu ổn định: ' || v_tile || '%. Không cần giảm giá.');
        END IF;
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Lỗi xử lý khuyến mãi: ' || SQLERRM);
END;

select distinct THUONGHIEU FROM CN1.SANPHAM;
-- Asus lenovo sony lg apple hp samsung panasonic 

SET SERVEROUTPUT ON;
EXEC PROC_XULY_KHUYENMAI_HETHONG('Lenovo');

SET SERVEROUTPUT ON;
EXEC PROC_XULY_KHUYENMAI_HETHONG('Panasonic'); 

select * from khosanpham_qlkho where sanpham_id = 210;
