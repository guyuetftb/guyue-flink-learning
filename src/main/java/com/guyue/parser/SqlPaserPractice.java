package com.guyue.flink.sql.parser;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

import static org.apache.calcite.sql.SqlKind.IDENTIFIER;

/**
 * @ClassName SqlPaserPractice
 * @Description TOOD
 * @Author lipeng
 */
public class SqlPaserPractice {
    public static void main(String[] args) {

        String simpleSql = "select name, sex, address, my_hash_code(name) hash_code from student s1 where name = 'zhangsan'";
        String joinSql = "select * from table1 t1 inner join table2 t2 on t1.id = t2.id";
        String insertSql = "INSERT INTO OutputTable_Sink SELECT users, tag FROM Orders_Source";

        String complexSql1 = "select a1.order_no, a2.product_code, a1.warehouse_code, a1.shop_code, coalesce(a2.product_num,0) as product_num, coalesce(t4.product_payment,0) as product_payment, coalesce(t4.back_amount) as back_amount2, coalesce(t4.post_price,0) as post_price from ( select order_id as order_no, warehouse_code, shop_code from bi_odi_mryx.recent_oms_production_oms_order where create_time>='2019-07-01' and event_class <> 'DELETE' and nvl(status,0)<> '40' and nvl(order_type,0)<>'40' ) t1  ";
        String complexSql = "SELECT sum(my_haso_code(a1.order_no)) as order_no_hashcode, sum(product_to_code(sum(a2.product_code))) as product_code, int_to_string(string_to_timestamp(a1.warehouse_code)) as warehouse_code,a1.shop_code,coalesce(a2.product_num,0) AS product_num,coalesce(t4.product_payment,0) AS product_payment,coalesce(t4.back_amount) AS back_amount2,coalesce(t4.post_price,0) AS post_price FROM \n" +
                "(SELECT order_id AS order_no,warehouse_code,shop_code FROM bi_odi_mryx.recent_oms_production_oms_order WHERE create_time>='${lasthourday}' and event_class<>'DELETE' and nvl(status,0)<>'40' and nvl(order_type,0)<>'40') a1 \n" +
                "INNER JOIN \n" +
                "(SELECT order_id AS order_no,goods_no AS product_code ,goods_name AS product_name,num AS product_num FROM bi_odi_mryx.recent_oms_production_oms_order_detail WHERE  create_time>='2019-07-01' and event_class<>'DELETE') a2 ON a1.order_no = a2.order_no\n" +
                "left JOIN \n" +
                "(SELECT order_no,sku AS product_code,sum(pay_amount)/100 AS payment_price,sum(postage)/100 AS post_price,sum(third_amount)/100 AS third_amount,sum(third_postage_amount)/100 AS third_postage_amount,sum(back_amount)/100 AS back_amount,sum(recharge_amount)/100 AS recharge_amount,sum(card_amount)/100 AS card_amount,sum(coalesce(pay_amount,0)+coalesce(postage,0))/100 AS product_payment,sum(coalesce(pay_amount,0)+coalesce(postage,0)-coalesce(third_amount,0)-coalesce(third_postage_amount,0))/100 AS balance_payment FROM bi_odi_mryx.recent_as_order_split_order_item_split \n" +
                "WHERE create_time>='2019-07-01' and event_class<>'DELETE'  \n" +
                "GROUP BY order_no,sku\n" +
                ") t4 \n" +
                "ON a2.order_no=t4.order_no and a2.product_code=t4.product_code\n" +
                "WHERE order_no <> 0 and product_code <> '' " +
                "GROUP BY order_no, product_code";

        String aa = "select * from tab_1 where a != 'a' and b != 'c' ";
        System.out.println(aa.replace("!=","<>"));

        SqlParser.Config config = SqlParser.configBuilder().setLex(Lex.MYSQL).build();
        SqlParser sqlParser = SqlParser.create(complexSql, config);
        SqlNode rootNode = null;
        try {
            rootNode = sqlParser.parseStmt();
        } catch (SqlParseException e) {
            e.printStackTrace();
        }
        if (rootNode instanceof SqlSelect) {
            System.out.println(" --> SqlSelect");
        } else if (rootNode instanceof SqlInsert) {
            System.out.println(" --> SqlInsert");
        }
        // SqlSelect selectRoot = (SqlSelect) rootNode;
        SqlJoin sqlJoin = null;
        SqlBasicCall sqlBasicCall = null;
        // selectRoot.getFrom();
        parseNode(rootNode);
        System.out.println(rootNode);
    }

    private static void parseNode(SqlNode sqlNode) {
        SqlKind sqlKind = sqlNode.getKind();
        switch (sqlKind) {
            case INSERT:
                SqlNode sqlTarget = ((SqlInsert) sqlNode).getTargetTable();
                SqlNode sqlSource = ((SqlInsert) sqlNode).getSource();
                System.out.println("------------Insert Table = " + sqlTarget.toString());
                parseNode(sqlSource);
                break;
            case SELECT:
                SqlSelect sqlSelect = ((SqlSelect) sqlNode);
                SqlNodeList sqlNodeList = sqlSelect.getSelectList();
                for (SqlNode fieldNode : sqlNodeList) {
                    parseOperator(fieldNode);
                }

                SqlNode sqlFrom = ((SqlSelect) sqlNode).getFrom();
                if (sqlFrom.getKind() == IDENTIFIER) {
                    System.out.println("------------Select Table = " + sqlFrom.toString());
                } else {
                    parseNode(sqlFrom);
                }
                break;
            case JOIN:
                SqlNode leftNode = ((SqlJoin) sqlNode).getLeft();
                SqlNode rightNode = ((SqlJoin) sqlNode).getRight();

                if (leftNode.getKind() == IDENTIFIER) {
                    System.out.println("------------Left Table = " + leftNode.toString());
                } else {
                    parseNode(leftNode);
                }

                if (rightNode.getKind() == IDENTIFIER) {
                    System.out.println("------------Right Table = " + rightNode.toString());
                } else {
                    parseNode(rightNode);
                }
                break;
            case AS:
                //不解析column,所以 as 相关的都是表
                SqlNode identifierNode = ((SqlBasicCall) sqlNode).getOperands()[0];
                if (identifierNode.getKind() != IDENTIFIER) {
                    parseNode(identifierNode);
                } else {
                    System.out.println("------------AS Table = " + identifierNode.toString());
                }
                break;
            case UNION:
                SqlNode unionLeft = ((SqlBasicCall) sqlNode).getOperands()[0];
                SqlNode unionRight = ((SqlBasicCall) sqlNode).getOperands()[1];
                if (unionLeft.getKind() == IDENTIFIER) {
                    System.out.println("------------Union Left Table = " + unionLeft.toString());
                } else {
                    parseNode(unionLeft);
                }
                if (unionRight.getKind() == IDENTIFIER) {
                    System.out.println("------------Union Right Table = " + unionRight.toString());
                } else {
                    parseNode(unionRight);
                }
                break;
            default:
                //do nothing
                break;
        }
    }

    public static String parseOperator(SqlNode sqlNode) {

        SqlKind fieldKind = sqlNode.getKind();
        if (sqlNode instanceof SqlBasicCall) {
            SqlBasicCall basicCall = (SqlBasicCall) sqlNode;
            if (basicCall.getOperator() instanceof SqlUnresolvedFunction) {
                System.out.println(" function quantifier >>>>>>>>>>>>>>> " + basicCall.getOperator().getName());
                if (null != basicCall.getOperands() && basicCall.getOperands().length > 0) {
                    parseOperator(basicCall.operands[0]);
                } else {
                    return basicCall.getOperator().getName();
                }
            } else {
                parseOperator(basicCall.operands[0]);
            }
        }
        return null;
    }
}
