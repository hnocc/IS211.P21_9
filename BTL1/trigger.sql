CREATE OR REPLACE TRIGGER TRG_TUDONG_CHUYEN_KHO_KHI_MUA
AFTER INSERT ON CN3.CTHD
FOR EACH ROW
DECLARE
    v_cn_id          VARCHAR2(10);     
    v_tonkho_local   NUMBER := 0;      
    v_sl_can_chuyen  NUMBER := 0;      
    v_sl_cn1         NUMBER := 0;      
    v_sl_cn2         NUMBER := 0;      
BEGIN
    SELECT NV.CHINHANH_ID INTO v_cn_id
    FROM CN3.HOADON HD
    JOIN CN3.NHANVIEN NV ON HD.NHANVIEN_ID = NV.NHANVIEN_ID
    WHERE HD.HOADON_ID = :NEW.HOADON_ID;

    EXECUTE IMMEDIATE '
        SELECT NVL(SOLUONGNHAP, 0)
        FROM ' || v_cn_id || '.KHOSANPHAM_QLKHO' ||
        CASE v_cn_id
            WHEN 'CN1' THEN '@CN1_LINK'
            WHEN 'CN2' THEN '@CN2_LINK'
            ELSE ''
        END || '
        WHERE SANPHAM_ID = :1 AND CHINHANH_ID = :2'
    INTO v_tonkho_local
    USING :NEW.SANPHAM_ID, v_cn_id;

    IF :NEW.SOLUONG > v_tonkho_local THEN
        v_sl_can_chuyen := :NEW.SOLUONG - v_tonkho_local;

        BEGIN
            SELECT NVL(SOLUONGNHAP, 0) INTO v_sl_cn1
            FROM CN1.KHOSANPHAM_QLKHO@CN1_LINK
            WHERE SANPHAM_ID = :NEW.SANPHAM_ID AND CHINHANH_ID = 'CN1';
        EXCEPTION
            WHEN OTHERS THEN v_sl_cn1 := 0;
        END;

        BEGIN
            SELECT NVL(SOLUONGNHAP, 0) INTO v_sl_cn2
            FROM CN2.KHOSANPHAM_QLKHO@CN2_LINK
            WHERE SANPHAM_ID = :NEW.SANPHAM_ID AND CHINHANH_ID = 'CN2';
        EXCEPTION
            WHEN OTHERS THEN v_sl_cn2 := 0;
        END;

        IF v_sl_cn1 >= v_sl_can_chuyen THEN
            UPDATE CN1.KHOSANPHAM_QLKHO@CN1_LINK
            SET SOLUONGNHAP = SOLUONGNHAP - v_sl_can_chuyen
            WHERE SANPHAM_ID = :NEW.SANPHAM_ID AND CHINHANH_ID = 'CN1';

            UPDATE CN3.KHOSANPHAM_QLKHO
            SET SOLUONGNHAP = SOLUONGNHAP + v_sl_can_chuyen
            WHERE SANPHAM_ID = :NEW.SANPHAM_ID AND CHINHANH_ID = v_cn_id;

        ELSIF v_sl_cn2 >= v_sl_can_chuyen THEN
            UPDATE CN2.KHOSANPHAM_QLKHO@CN2_LINK
            SET SOLUONGNHAP = SOLUONGNHAP - v_sl_can_chuyen
            WHERE SANPHAM_ID = :NEW.SANPHAM_ID AND CHINHANH_ID = 'CN2';

            UPDATE CN3.KHOSANPHAM_QLKHO
            SET SOLUONGNHAP = SOLUONGNHAP + v_sl_can_chuyen
            WHERE SANPHAM_ID = :NEW.SANPHAM_ID AND CHINHANH_ID = v_cn_id;

        ELSE
            DECLARE
                v_sl_chuyen_cn1 NUMBER := 0;
                v_sl_chuyen_cn2 NUMBER := 0;
            BEGIN
                IF v_sl_cn1 > 0 THEN
                    v_sl_chuyen_cn1 := LEAST(v_sl_cn1, v_sl_can_chuyen);
                    v_sl_can_chuyen := v_sl_can_chuyen - v_sl_chuyen_cn1;

                    UPDATE CN1.KHOSANPHAM_QLKHO@CN1_LINK
                    SET SOLUONGNHAP = SOLUONGNHAP - v_sl_chuyen_cn1
                    WHERE SANPHAM_ID = :NEW.SANPHAM_ID AND CHINHANH_ID = 'CN1';

                    UPDATE CN3.KHOSANPHAM_QLKHO
                    SET SOLUONGNHAP = SOLUONGNHAP + v_sl_chuyen_cn1
                    WHERE SANPHAM_ID = :NEW.SANPHAM_ID AND CHINHANH_ID = v_cn_id;
                END IF;

                IF v_sl_cn2 > 0 AND v_sl_can_chuyen > 0 THEN
                    v_sl_chuyen_cn2 := LEAST(v_sl_cn2, v_sl_can_chuyen);
                    v_sl_can_chuyen := v_sl_can_chuyen - v_sl_chuyen_cn2;

                    UPDATE CN2.KHOSANPHAM_QLKHO@CN2_LINK
                    SET SOLUONGNHAP = SOLUONGNHAP - v_sl_chuyen_cn2
                    WHERE SANPHAM_ID = :NEW.SANPHAM_ID AND CHINHANH_ID = 'CN2';

                    UPDATE CN3.KHOSANPHAM_QLKHO
                    SET SOLUONGNHAP = SOLUONGNHAP + v_sl_chuyen_cn2
                    WHERE SANPHAM_ID = :NEW.SANPHAM_ID AND CHINHANH_ID = v_cn_id;
                END IF;

                EXECUTE IMMEDIATE '
                    SELECT NVL(SOLUONGNHAP, 0)
                    FROM ' || v_cn_id || '.KHOSANPHAM_QLKHO' ||
                    CASE v_cn_id
                        WHEN 'CN1' THEN '@CN1_LINK'
                        WHEN 'CN2' THEN '@CN2_LINK'
                        ELSE ''
                    END || '
                    WHERE SANPHAM_ID = :1 AND CHINHANH_ID = :2'
                INTO v_tonkho_local
                USING :NEW.SANPHAM_ID, v_cn_id;

                IF v_tonkho_local < :NEW.SOLUONG THEN
                    RAISE_APPLICATION_ERROR(-20010, 'Không đủ hàng để xử lý và không có chi nhánh nào đủ số lượng bổ sung.');
                END IF;
            END;
        END IF;
    END IF;

    UPDATE CN3.KHOSANPHAM_QLKHO
    SET SOLUONGNHAP = SOLUONGNHAP - :NEW.SOLUONG
    WHERE SANPHAM_ID = :NEW.SANPHAM_ID AND CHINHANH_ID = v_cn_id;
END;
/

-- Demo TH1
select * from khosanpham_qlkho where sanpham_id = 298; -- 89
select * from CN1.khosanpham_qlkho@CN1_LINK where sanpham_id = 298; -- 298
insert into CTHD values (100010, 298, 100);
COMMIT;
select * from CTHD where hoadon_id = 100010;

-- Demo TH2
select * from khosanpham_qlkho where soluongnhap < 200; -- 171
select * from CN1.khosanpham_qlkho@CN1_LINK where sanpham_id = 21; -- 78
select * from CN2.khosanpham_qlkho@CN2_LINK where sanpham_id = 21; -- 363
insert into CTHD values (100010, 78, 300);
COMMIT;
select * from CTHD where hoadon_id = 100010;

-- Demo TH3
select * from khosanpham_qlkho where soluongnhap < 100; -- 71
select * from CN1.khosanpham_qlkho@CN1_LINK where sanpham_id = 134; -- 36
select * from CN2.khosanpham_qlkho@CN2_LINK where sanpham_id = 134; -- 264
insert into CTHD values (100010, 134, 350);
COMMIT;
select * from CTHD where hoadon_id = 100010;

-- Demo TH4
select * from khosanpham_qlkho where soluongnhap < 100; -- 62
select * from CN1.khosanpham_qlkho@CN1_LINK where sanpham_id = 103; -- 419
select * from CN2.khosanpham_qlkho@CN2_LINK where sanpham_id = 103; -- 97
insert into CTHD values (100010, 103, 1000);
ROLLBACK;