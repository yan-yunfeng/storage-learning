package curd;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.util.Map;

import connector.Connectors;

/**
 * author Yan YunFeng  Email:twd.wuyun@163.com
 * create 19-8-9 下午4:28
 */
public class Mysql {

    public static void main(String[] args) throws Exception{
        Connection connection;
        connection = Connectors.getSource().getConnection();
        String sql = "SELECT * FROM database";
        PreparedStatement ps = connection.prepareStatement(sql);
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            System.out.println(mapData(resultSet).toString());
        }
    }

    private static Map<String,String> mapData(ResultSet resultSet) throws Exception {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int count = metaData.getColumnCount();
        Map<String,String> map = new HashMap<>();
        // 查询下标从1开始 不是0
        for (int i = 1; i <= count; i++) {
            String columnName = metaData.getColumnName(i);
            Object value = resultSet.getObject(i);
            if (value != null) {
                map.put(columnName, value.toString());
            }
        }
        return map;
    }

}
