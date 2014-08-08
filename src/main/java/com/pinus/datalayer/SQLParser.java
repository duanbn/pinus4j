package com.pinus.datalayer;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.TablesNamesFinder;

/**
 * SQL语句解析器. 依赖JSqlParser实现.
 * 
 * @author duanbn
 */
public class SQLParser {

	/**
	 * 获取sql语句中所有表名. 支持多表查询.
	 * 
	 * @param sql
	 *            sql语句
	 * 
	 * @return 表名
	 */
	public static List<String> parseTableName(String sql) {
		Statement st;
		try {
			st = CCJSqlParserUtil.parse(sql);
		} catch (JSQLParserException e) {
			throw new RuntimeException(e);
		}
		Select selectStatement = (Select) st;
		TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
		List<String> tableNames = tablesNamesFinder.getTableList(selectStatement);
		return tableNames;
	}

	/**
	 * 给表明添加分片表下标.
	 * 
	 * @param sql
	 *            sql语句
	 * @param tableIndex
	 *            分片的表下标.
	 */
	public static String addTableIndex(String sql, int tableIndex) {
		List<String> tableNames = parseTableName(sql);

		Pattern p = null;
		Matcher m = null;
		for (String tableName : tableNames) {
			p = Pattern.compile(tableName);
			m = p.matcher(sql);
			int i = 0;
			while (m.find()) {
                // 用来判断表名前后的字符.
				int start = m.start() + i;
				int end = m.end() + i;
				char cStart = sql.charAt(start - 1);
				char cEnd = sql.charAt(end);
				// 32=' ' and 61='=' and 44=',' and 46='.'
				if ((cStart == 32 || cStart == 61) && (cEnd == 32 || cEnd == 44 || cEnd == 46)) {
					sql = sql.substring(0, start) + tableName + tableIndex + sql.substring(end);
					i++;
				}
			}
		}

		return sql;
	}

}
