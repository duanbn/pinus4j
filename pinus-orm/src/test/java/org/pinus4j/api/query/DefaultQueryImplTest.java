package org.pinus4j.api.query;

import junit.framework.Assert;

import org.junit.Test;
import org.pinus4j.api.query.impl.Condition;
import org.pinus4j.api.query.impl.DefaultQueryImpl;

public class DefaultQueryImplTest {

    @Test
    public void test() throws Exception {
        DefaultQueryImpl query = new DefaultQueryImpl();
        query.and(Condition.and(Condition.eq("a", 1), Condition.eq("b", 1)));
        query.or(Condition.and(Condition.eq("c", 1), Condition.eq("d", 1)));
        query.or(Condition.or(Condition.noteq("e", 1), Condition.isNotNull("f")));
        Assert.assertEquals(" where (a = 1 and b = 1) or (c = 1 and d = 1) or (e <> 1 or f is not null)",
                query.getWhereSql());

        query.clean();
        Assert.assertTrue(query.getWhereSql().isEmpty());

        query.and(Condition.or(Condition.eq("a", 1), Condition.noteq("b", 1)));
        query.and(Condition.or(Condition.eq("c", 1), Condition.eq("d", 1)));
        query.and(Condition.or(Condition.eq("e", 1), Condition.eq("f", 1)));
        Assert.assertEquals(" where (a = 1 or b <> 1) and (c = 1 or d = 1) and (e = 1 or f = 1)", query.getWhereSql());

        query.clean();
        Assert.assertTrue(query.getWhereSql().isEmpty());

        query.and(Condition.eq("a", 1)).and(Condition.eq("b", 1));
        Assert.assertEquals(" where a = 1 and b = 1", query.getWhereSql());

    }

}
