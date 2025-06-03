/* Function 3: Hàm này tính tỷ lệ phần trăm doanh thu của một thương hiệu cụ thể 
so với tổng doanh thu của toàn hệ thống trong một khoảng thời gian xác định.*/

CREATE OR REPLACE FUNCTION FN_TYLE_DOANHTHU_THUONGHIEU (
    p_thuonghieu IN VARCHAR2,
    p_tungay     IN DATE,
    p_denngay    IN DATE
) RETURN SYS_REFCURSOR
IS
    rc SYS_REFCURSOR;
BEGIN
    OPEN rc FOR
    SELECT
        TH.THUONGHIEU,
        TH.DOANHTHU_THUONGHIEU,
        HT.DOANHTHU_HETHONG,
        ROUND(TH.DOANHTHU_THUONGHIEU / HT.DOANHTHU_HETHONG * 100, 2) AS TILE_PHANTRAM
    FROM (
        SELECT SP.THUONGHIEU, SUM(CT.SOLUONG * SP.GIABAN) AS DOANHTHU_THUONGHIEU
        FROM (
            SELECT * FROM CN1.CTHD
            UNION ALL SELECT * FROM CN2.CTHD@CN2_GD_LINK
            UNION ALL SELECT * FROM CN3.CTHD@CN3_GD_LINK
        ) CT
        JOIN (
            SELECT * FROM CN1.HOADON
            UNION ALL SELECT * FROM CN2.HOADON@CN2_GD_LINK
            UNION ALL SELECT * FROM CN3.HOADON@CN3_GD_LINK
        ) HD ON CT.HOADON_ID = HD.HOADON_ID
        JOIN (
            SELECT * FROM CN1.SANPHAM
            UNION ALL SELECT * FROM CN2.SANPHAM@CN2_GD_LINK
            UNION ALL SELECT * FROM CN3.SANPHAM@CN3_GD_LINK
        ) SP ON CT.SANPHAM_ID = SP.SANPHAM_ID
        WHERE SP.THUONGHIEU = p_thuonghieu
          AND HD.NGAYTAO BETWEEN p_tungay AND p_denngay
        GROUP BY SP.THUONGHIEU
    ) TH,
    (
        SELECT SUM(CT.SOLUONG * SP.GIABAN) AS DOANHTHU_HETHONG
        FROM (
            SELECT * FROM CN1.CTHD
            UNION ALL SELECT * FROM CN2.CTHD@CN2_GD_LINK
            UNION ALL SELECT * FROM CN3.CTHD@CN3_GD_LINK
        ) CT
        JOIN (
            SELECT * FROM CN1.HOADON
            UNION ALL SELECT * FROM CN2.HOADON@CN2_GD_LINK
            UNION ALL SELECT * FROM CN3.HOADON@CN3_GD_LINK
        ) HD ON CT.HOADON_ID = HD.HOADON_ID
        JOIN (
            SELECT * FROM CN1.SANPHAM
            UNION ALL SELECT * FROM CN2.SANPHAM@CN2_GD_LINK
            UNION ALL SELECT * FROM CN3.SANPHAM@CN3_GD_LINK
        ) SP ON CT.SANPHAM_ID = SP.SANPHAM_ID
        WHERE HD.NGAYTAO BETWEEN p_tungay AND p_denngay
    ) HT;

    RETURN rc;
END;
/


CREATE OR REPLACE FUNCTION FN_TYLE_DOANHTHU_THUONGHIEU (
    p_thuonghieu IN VARCHAR2,
    p_tungay     IN DATE,
    p_denngay    IN DATE
) RETURN SYS_REFCURSOR
IS
    rc SYS_REFCURSOR;
BEGIN
    OPEN rc FOR
    WITH 
    CTE_CTHD AS (
        SELECT * FROM CN1.CTHD
        UNION ALL SELECT * FROM CN2.CTHD@CN2_GD_LINK
        UNION ALL SELECT * FROM CN3.CTHD@CN3_GD_LINK
    ),
    CTE_HOADON AS (
        SELECT * FROM CN1.HOADON WHERE NGAYTAO BETWEEN p_tungay AND p_denngay
        UNION ALL SELECT * FROM CN2.HOADON@CN2_GD_LINK WHERE NGAYTAO BETWEEN p_tungay AND p_denngay
        UNION ALL SELECT * FROM CN3.HOADON@CN3_GD_LINK WHERE NGAYTAO BETWEEN p_tungay AND p_denngay
    ),
    CTE_SANPHAM AS (
        SELECT * FROM CN1.SANPHAM
        UNION ALL SELECT * FROM CN2.SANPHAM@CN2_GD_LINK
        UNION ALL SELECT * FROM CN3.SANPHAM@CN3_GD_LINK
    ),
    DOANHTHU_CHITIET AS (
        SELECT 
            SP.THUONGHIEU,
            CT.SOLUONG * SP.GIABAN AS DOANHTHU
        FROM CTE_CTHD CT
        JOIN CTE_HOADON HD ON CT.HOADON_ID = HD.HOADON_ID
        JOIN CTE_SANPHAM SP ON CT.SANPHAM_ID = SP.SANPHAM_ID
    )
    SELECT
        TH.THUONGHIEU,
        TH.DOANHTHU_THUONGHIEU,
        HT.DOANHTHU_HETHONG,
        ROUND(TH.DOANHTHU_THUONGHIEU / NULLIF(HT.DOANHTHU_HETHONG, 0) * 100, 2) AS TILE_PHANTRAM
    FROM (
        SELECT THUONGHIEU, SUM(DOANHTHU) AS DOANHTHU_THUONGHIEU
        FROM DOANHTHU_CHITIET
        WHERE THUONGHIEU = p_thuonghieu
        GROUP BY THUONGHIEU
    ) TH,
    (
        SELECT SUM(DOANHTHU) AS DOANHTHU_HETHONG
        FROM DOANHTHU_CHITIET
    ) HT;

    RETURN rc;
END;
/

-- Thuc thi function 

SET SERVEROUTPUT ON;
DECLARE
    tenthuonghieu_input    VARCHAR2(100) := '&tenthuonghieu_input';
    tungay_input           DATE := TO_DATE('&tungay_input', 'YYYY-MM-DD');
    denngay_input          DATE := TO_DATE('&denngay_input', 'YYYY-MM-DD');
    rc                     SYS_REFCURSOR;
    v_thuonghieu           VARCHAR2(100);
    v_doanhthu_th          NUMBER;
    v_doanhthu_hethong     NUMBER;
    v_tile                 NUMBER;
BEGIN
    -- Gọi function trả con trỏ
    rc := FN_TYLE_DOANHTHU_THUONGHIEU(tenthuonghieu_input, tungay_input, denngay_input);

    -- Duyệt toàn bộ kết quả trong con trỏ
    LOOP
        FETCH rc INTO v_thuonghieu, v_doanhthu_th, v_doanhthu_hethong, v_tile;
        EXIT WHEN rc%NOTFOUND;

        -- In kết quả ra
        DBMS_OUTPUT.PUT_LINE('Thương hiệu               : ' || v_thuonghieu);
        DBMS_OUTPUT.PUT_LINE('Doanh thu                 : ' || v_doanhthu_th || ' đồng');
        DBMS_OUTPUT.PUT_LINE('Tổng doanh thu hệ thống   : ' || v_doanhthu_hethong || ' đồng');
        DBMS_OUTPUT.PUT_LINE('Tỉ lệ đóng góp            : ' || v_tile || ' %');
        DBMS_OUTPUT.PUT_LINE('-------------------------------------------------------------');
    END LOOP;

    CLOSE rc;
END;
/

DROP FUNCTION FN_TYLE_DOANHTHU_THUONGHIEU;
