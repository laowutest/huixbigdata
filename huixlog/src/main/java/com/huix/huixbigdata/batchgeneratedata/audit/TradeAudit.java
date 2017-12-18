package com.huix.huixbigdata.batchgeneratedata.audit;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.huix.huixbigdata.batchgeneratedata.util.HiveUtil;
import com.huix.huixbigdata.batchgeneratedata.util.JDBCUtil;



/**
 * TradeAudit鐢ㄤ簬浜ゆ槗瀹¤
 *
 * @className TradeAudit
 * @author mxlee
 * @email imxlee@foxmail.com
 * @date 2016骞�12鏈�7鏃�
 */
public class TradeAudit {
	
	public static void main(String[] args) throws SQLException {
		audit("2016-12-11 17:17");
	}
	
	/**
	 * 瀹¤mysql涓笌hive涓殑浜ゆ槗
	 * 
	 * @param minute
	 *            2016-12-7 19:58
	 * @return
	 * @throws SQLException
	 */
	public static void audit(String minute) throws SQLException {
		// 1.鏌ヨMySQL
		String sql = "select balance from trade where time=?";
		List<Object> params = new ArrayList<Object>();
		params.add(minute);
		List<Object> listBalance = JDBCUtil.queryRow(sql, params);
		BigDecimal mysqlBalance = new BigDecimal(listBalance.get(0).toString());
		System.out.println("瀹炴椂璁＄畻:锟�" + mysqlBalance);
		// 2.鏌ヨHive鐨勬椂鍊欙紝鑼冨洿鏄痆20161207094900, 20161207095000)
		BigDecimal hiveBalance = HiveUtil.querySale(minute);

		// 3.鍒ゆ柇涓や釜缁撴灉鏄惁鐩哥瓑
		System.out.println("钀藉湴鏁版嵁:锟�" + hiveBalance);
		System.out.println("瀹¤缁撴灉鏄惁鐩哥瓑:" + mysqlBalance.equals(hiveBalance));
	}
}
