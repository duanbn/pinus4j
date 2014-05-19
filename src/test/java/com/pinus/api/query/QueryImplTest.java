package com.pinus.api.query;

import org.junit.Test;

import com.pinus.api.query.Condition;
import com.pinus.api.query.IQuery;
import com.pinus.api.query.Order;
import com.pinus.api.query.QueryImpl;

public class QueryImplTest {

    @Test
    public void testAdd() throws Exception {
        IQuery query = new QueryImpl();
        Condition cond = Condition.eq("field", "test field");
        query.add(cond);
        System.out.println(query);

        query.add(Condition.noteq("field", "test field to"));
        System.out.println(query);
        query.add(Condition.in("field2", 2,3,4,5));
        System.out.println(query);

        query.add(Condition.or(Condition.like("likeField", "like%"), Condition.in("infield", "5", "test")));
        System.out.println(query);

        query.orderBy("field", Order.DESC).limit(0,10);
        System.out.println(query);
    }

}
