package dev.impala;

import org.apache.impala.analysis.*;
import org.apache.impala.catalog.View;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class ImpalaParser {
    public static void main(String[] args) {
//        String stmt = "INVALIDATE METADATA cods.pk_report_shops";
        String stmt = "with khoxxx_xxxx AS (\n" +
                "    select *\n" +
                "    from (\n" +
                "        select pa.`order` ma_don\n" +
                "        , ifnull(s1.id, s.id) as station_id\n" +
                "        , row_number() over (partition by pa.`order` order by ntsp.created desc) rn\n" +
                "        from db_test.packagessssss_1111 pa\n" +
                "        inner join db_test.carts ca on ca.id = pa.deliver_cart_id\n" +
                "        inner join db_test.stations s on s.id = pa.transfer_station_id\n" +
                "        left join db_test.new_truck_point_carts ntpc on ca.id = ntpc.cart_id\n" +
                "        left join db_test.new_truck_stop_points ntsp on ntpc.point_id  = ntsp.id and ntsp.type in (1, 2)\n" +
                "        left join (\n" +
                "            select id\n" +
                "            from db_test.stations \n" +
                "            where type in ('station', 'post_office') and working = 1 \n" +
                "            ) s1 on s1.id = ntsp.station_id\n" +
                "        where pa.created >= (now() - interval 30 days) and ca.is_visible = 1 and ca.type = 2     ) t where rn = 1\n" +
                "    )\n" +
                ",\n" +
                "kho_tra AS (\n" +
                "    select *\n" +
                "    from (\n" +
                "        select r.pkg_o ma_don\n" +
                "        , ifnull(s.id, r.station_id ) as station_id\n" +
                "        , row_number() over (partition by r.pkg_o order by ntsp.created desc) rn\n" +
                "        from db_test.pkg_returns r\n" +
                "        inner join db_test.carts ca on ca.id = r.cart_id\n" +
                "        left join db_test.new_truck_point_carts ntpc on ca.id = ntpc.cart_id\n" +
                "        left join db_test.new_truck_stop_points ntsp on ntpc.point_id  = ntsp.id and ntsp.type in (1, 2)\n" +
                "        left join (\n" +
                "            select id\n" +
                "            from db_test.stations \n" +
                "            where type in ('station', 'post_office') and working = 1 \n" +
                "            ) s on s.id = ntsp.station_id\n" +
                "        where r.created >= (now() - interval 30 days) and ca.is_visible = 1 and ca.type = 3     ) t where rn = 1\n" +
                "    )\n" +
                ", \n" +
                "xuat_xe AS ( \n" +
                "    select pkg_order, max(time_xuat_xe) time_xuat_xe\n" +
                "    from (\n" +
                "        select pkg_order, pl.created time_xuat_xe, \n" +
                "        if(regexp_extract(split_part(`desc`, ') (', 2),'\\\\d+',0) = '', regexp_extract(split_part(`desc`, '(', 2),'\\\\d+',0), regexp_extract(split_part(`desc`, ') (', 2),'\\\\d+',0)) station_id\n" +
                "        from db_test.package_logs pl\n" +
                "        where pl.data_date_key >= cast(from_timestamp(now() - interval 30 days,'yyyyMMdd') as int)\n" +
                "        and (action in ( 'cfmTransitD4h', 'cfmTransit') and `desc` like '%để giao cho COD%')\n" +
                "        ) pl \n" +
                "    inner join khoxxx_xxxx tt on tt.ma_don = pl.pkg_order and cast(pl.station_id as int) = tt.station_id\n" +
                "    group by pkg_order\n" +
                "    ) \n" +
                ", \n" +
                "nhap_dich_nhap_tra as (\n" +
                "    select pl.pkg_order, min(if(pa.`order` is not null, pl.created, null)) as time_nhap_dich, min(if(r.ma_don is not null, pl.created, null)) time_nhap_tra\n" +
                "    from (\n" +
                "        select *, case when pl.action in ('cfmImportV4JobQ', 'cfmImportV4') then regexp_extract(pl.`desc`,'\\\\d+',0) \n" +
                "            when action = 'scanBox' then regexp_extract(split_part(pl.`desc`, 'kho ', 2), '\\\\d+',0)\n" +
                "            end as station_id\n" +
                "        from db_test.package_logs pl\n" +
                "        where data_date_key >= cast(from_timestamp(now() - interval 30 days,'yyyyMMdd') as int)\n" +
                "            and pl.action in ('cfmImportV4JobQ', 'cfmImportV4', 'scanBox')\n" +
                "            and ((pl.action in ('cfmImportV4JobQ', 'cfmImportV4') and pl.`desc` not like '%trung chuyển%' ) or (action = 'scanBox' and `desc` like '%nhập%'))\n" +
                "    ) pl \n" +
                "    left join (\n" +
                "        select `order`, created, transfer_station_id\n" +
                "        from db_test.packagessssss_1111 pa \n" +
                "        where pa.created >= (now() - interval 30 days)\n" +
                "        ) pa on pa.`order` = pl.pkg_order and cast(station_id as int) = pa.transfer_station_id\n" +
                "    left join kho_tra r on pl.pkg_order = r.ma_don and cast(pl.station_id as int)  = r.station_id \n" +
                "    where r.ma_don is not null or pa.`order` is not null\n" +
                "    group by pl.pkg_order\n" +
                "    ) \n" +
                ", \n" +
                "action as (\n" +
                "    SELECT pkg_order\n" +
                "        , min(if(type = 'Xuất giao', created, null)) time_xuat_giao\n" +
                "        , min(if(type = 'Xuất nguồn', created, null)) tg_xuat_nguon\n" +
                "        , min(if(type = 'Xuất trả', created, null)) time_xuat_tra\n" +
                "        , min(if(type = 'Lấy TC', created, null)) AS time_lay_tc\n" +
                "    from (  \n" +
                "        select pkg_order, created \n" +
                "            , case\n" +
                "                WHEN pl.action IN ('distributorCfmDeliver','AssignDTeam', 'updateDeliverCod', 'autoAssignDelayPackageDteam') THEN 'Xuất giao'\n" +
                "                WHEN pl.action IN ('cfmTransit', 'cfmTransitD4h','scanBox') THEN 'Xuất nguồn'\n" +
                "                WHEN pl.action IN ('distributorCfmReturning', 'processReturn') THEN 'Xuất trả' \n" +
                "                ELSE 'Lấy TC'\n" +
                "            END AS type\n" +
                "        from db_test.package_logs pl \n" +
                "        where pl.data_date_key >= cast(from_timestamp(now() - interval 30 days,'yyyyMMdd') as int)\n" +
                "            and pl.action IN ('confirmTmpPickedPackageStatus', 'confirmTmpPickedPackageStatusFromPO'\n" +
                "            , 'cfmTmpPickedPkgByPKB2C','confirmPackageHandover', 'cfmImpPkgFrPONew'\n" +
                "            , 'confirmPickedPackageStatusByOperator', 'updateTmpPickingStatus'\n" +
                "            , 'distributorCfmDeliver','AssignDTeam', 'updateDeliverCod', 'autoAssignDelayPackageDteam'\n" +
                "            ,'cfmTransit', 'cfmTransitD4h', 'scanBox',  'distributorCfmReturning', 'processReturn')\n" +
                "            and ((pl.action IN ('confirmTmpPickedPackageStatus') AND pl.new_value = '3' AND pl.old_value  IN ('8','7','1','10','12','2','3'))\n" +
                "                OR (pl.action IN ('confirmTmpPickedPackageStatusFromPO') AND pl.`desc` LIKE '%bưu cục%')\n" +
                "                OR (pl.action IN ('cfmTmpPickedPkgByPKB2C','confirmPackageHandover', 'cfmImpPkgFrPONew') AND pl.new_value = '3')\n" +
                "                OR pl.action = 'confirmPickedPackageStatusByOperator'\n" +
                "                OR (action in ('updateTmpPickingStatus') and new_value in ('1', '4'))                 OR pl.action in ('distributorCfmDeliver','AssignDTeam', 'updateDeliverCod', 'autoAssignDelayPackageDteam')                 OR (pl.action IN ('cfmTransit', 'cfmTransitD4h') or (pl.action = 'scanBox' and `desc` like '%xuất%'))                 OR pl.action in ( 'distributorCfmReturning', 'processReturn'))         ) t\n" +
                "    group by pkg_order\n" +
                "    )\n" +
                "\n" +
                "select pa.`order`, s.id p_station_id, s1.id d_station_id, s3.id r_station_id   \n" +
                "    , CASE \n" +
                "        WHEN s.province_id = 1 AND s.note like '%kho_huyen%' THEN 'Huyện HN' \n" +
                "        WHEN s.province_id = 1 THEN 'Nội thành HN'\n" +
                "        WHEN s.province_id = 126 AND s.note like '%kho_huyen%' THEN 'Huyện HCM'\n" +
                "        WHEN s.province_id = 126 AND s.type IN ('station', 'post_office') THEN 'Nội thành HCM'\n" +
                "        else 'Tỉnh'\n" +
                "    END AS type_kho_nguon \n" +
                "    , CASE \n" +
                "        WHEN s1.province_id = 1 AND s1.note like '%kho_huyen%' THEN 'Huyện HN' \n" +
                "        WHEN s1.province_id = 1 THEN 'Nội thành HN'\n" +
                "        WHEN s1.province_id = 126 AND s1.note like '%kho_huyen%' THEN 'Huyện HCM'\n" +
                "        WHEN s1.province_id = 126 AND s1.type IN ('s1tation', 'pos1t_office') THEN 'Nội thành HCM'\n" +
                "        else 'Tỉnh'\n" +
                "    END As type_kho_dich   \n" +
                "    , CASE \n" +
                "        WHEN s3.province_id = 1 AND s3.note like '%kho_huyen%' THEN 'Huyện HN' \n" +
                "        WHEN s3.province_id = 1 THEN 'Nội thành HN'\n" +
                "        WHEN s3.province_id = 126 AND s3.note like '%kho_huyen%' THEN 'Huyện HCM'\n" +
                "        WHEN s3.province_id = 126 AND s3.type IN ('s1tation', 'pos1t_office') THEN 'Nội thành HCM'\n" +
                "        else 'Tỉnh'\n" +
                "    END as type_kho_tra\n" +
                "    , ifnull(time_xuat_xe, time_nhap_dich) xx_nd, time_xuat_giao, tg_xuat_nguon, time_lay_tc, time_nhap_tra, time_xuat_tra\n" +
                "from db_test.packagessssss_1111 pa \n" +
                "INNER JOIN db_test.stations s ON s.id = pa.station_id INNER JOIN db_test.stations s1 ON s1.id = pa.transfer_station_id INNER JOIN db_test.provinces_with_region_name region ON region.id = pa.pick_province_id INNER JOIN db_test.provinces_with_region_name region1 ON region1.id = pa.customer_province_id left join khoxxx_xxxx t on t.ma_don = pa.`order` \n" +
                "left join kho_tra r on r.ma_don = pa.`order`\n" +
                "left join db_test.stations s3 on r.station_id = s3.id\n" +
                "left join xuat_xe xx on xx.pkg_order = pa.`order` \n" +
                "left join nhap_dich_nhap_tra nd on nd.pkg_order = pa.`order` \n" +
                "left join action a on a.pkg_order = pa.`order` \n" +
                "where pa.created >= (now() - interval 30 days)";
//        stmt = "select * from cods.pk_report_shops";
//        stmt = "INVALIDATE METADATA cods.pk_report_shops";
//        System.out.println(System.getProperty("java.library.path"));
        SqlScanner input = new SqlScanner(new StringReader(stmt));
        SqlParser parser = new SqlParser(input);
        ParseNode node = null;
        try {
            node = (ParseNode) parser.parse().value;
            Set<String> rs = new LinkedHashSet<>();
            if (node instanceof SelectStmt) {
                rs.addAll(extractTableNamesFromSelectStmt((SelectStmt) node));
            }
            System.out.println(rs);
        } catch (Exception e) {
            System.err.println(parser.getErrorMsg(stmt));
            e.printStackTrace();
        }
    }

    public static List<String> extractTableNamesFromSelectStmt(SelectStmt node) {
        List<String> rs = new ArrayList<>();
        for (TableRef tblRef : node.getTableRefs()) {
            if (tblRef instanceof InlineViewRef) {
                InlineViewRef inlineViewRef = (InlineViewRef) tblRef;
                QueryStmt viewStmt = inlineViewRef.getViewStmt();
                if (viewStmt instanceof SelectStmt) {
                    rs.addAll(extractTableNamesFromSelectStmt((SelectStmt) viewStmt));
                }
            } else {
                rs.add(String.join(".", tblRef.getPath()));
            }
        }
        if (node.hasWithClause()) {
            for (View view : node.getWithClause().getViews()) {
                QueryStmt queryStmt = view.getQueryStmt();
                if (queryStmt instanceof SelectStmt)
                    rs.addAll(extractTableNamesFromSelectStmt((SelectStmt) queryStmt));
            }
        }
        return rs;
    }
}
