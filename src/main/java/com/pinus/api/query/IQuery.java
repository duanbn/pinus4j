package com.pinus.api.query;

/**
 * 查询对象.
 * 
 * @author duanbn
 */
public interface IQuery {

    /**
     * 获取此对象的复制对象.
     *
     * @return 此对象实例的复制
     */
    public IQuery clone();

	/**
	 * 添加取值字段.
	 * 
	 * @param field
	 *            获取值的字段
	 * @return
	 */
	// public IQuery addField(String... field);

	/**
	 * 获取取值字段. 使用逗号分隔
	 * 
	 * @return
	 */
	// public String getField();

	/**
	 * 返回查询条件的sql语句.
	 * 
	 * @return 查询条件sql
	 */
	public String getWhereSql();

	/**
	 * 添加查询条件.
	 * 
	 * @param cond
	 *            一个查询条件
	 */
	public IQuery add(Condition cond);

	/**
	 * 添加排序字段.
	 * 
	 * @param field
	 *            被排序字段
	 * @param order
	 *            升序降序
	 */
	public IQuery orderBy(String field, Order order);

	/**
	 * 分页参数.
	 * 
	 * @param start
	 *            开始偏移量
	 * @param limit
	 *            页大小
	 */
	public IQuery limit(int start, int limit);

	/**
	 * 设置limit参数
	 * 
	 * @param limit
	 *            limit
	 */
	public IQuery limit(int limit);

}
