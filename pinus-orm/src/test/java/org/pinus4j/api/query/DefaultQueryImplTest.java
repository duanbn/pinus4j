package org.pinus4j.api.query;

import junit.framework.Assert;

import org.junit.Test;
import org.pinus4j.api.SQL;
import org.pinus4j.api.query.impl.Condition;
import org.pinus4j.api.query.impl.DefaultQueryImpl;

public class DefaultQueryImplTest {

    @Test
    public void test() throws Exception {
        DefaultQueryImpl<Object> query = new DefaultQueryImpl<Object>();
        query.and(Condition.and(Condition.eq("a", 1), Condition.eq("b", 1)));
        query.or(Condition.and(Condition.eq("c", 2), Condition.eq("d", 2)));
        query.or(Condition.or(Condition.noteq("e", 3), Condition.isNotNull("f")));
        SQL sql = query.getWhereSql();
        System.out.println(sql);
        System.out.println(sql.getParams());

        query.clean();
        Assert.assertTrue(query.getWhereSql().getSql().isEmpty());

        query.and(Condition.or(Condition.eq("a", 1), Condition.noteq("b", 1)));
        query.and(Condition.or(Condition.eq("c", 1), Condition.eq("d", 1)));
        query.and(Condition.or(Condition.eq("e", 1), Condition.eq("f", 1)));
        sql = query.getWhereSql();
        System.out.println(sql);
        System.out.println(sql.getParams());

        query.clean();
        Assert.assertTrue(query.getWhereSql().getSql().isEmpty());

        query.and(Condition.eq("a", 1)).and(Condition.eq("b", 1));
        query.limit(10, 10);
        sql = query.getWhereSql();
        
        System.out.println(sql);
        System.out.println(sql.getParams());

        query.clean();

        query.and(Condition.or(Condition.eq("a", 1), Condition.eq("b", 1))).and(
                Condition.and(Condition.eq("a", 2), Condition.eq("b", 2)));
        sql = query.getWhereSql();
        System.out.println(sql);
        System.out.println(sql.getParams());

        query.clean();
        query.and(Condition.in("a", true, false));
        sql = query.getWhereSql();
        System.out.println(sql);
        System.out.println(sql.getParams());

    }

}
