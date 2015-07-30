package org.pinus4j.utils;

import java.util.HashMap;

import org.junit.Assert;
import org.junit.Test;
import org.pinus4j.entity.meta.EntityPK;
import org.pinus4j.entity.meta.PKName;
import org.pinus4j.entity.meta.PKValue;

import com.google.common.collect.Maps;

public class IOUtilTest {

    @Test
    public void testEntityPK() throws Exception {
        PKName[] pkNames = new PKName[] { PKName.valueOf("name") };
        PKValue[] pkValues = new PKValue[] { PKValue.valueOf(100) };
        EntityPK pk1 = EntityPK.valueOf(pkNames, pkValues);
        EntityPK pk2 = EntityPK.valueOf(pkNames, pkValues);
        HashMap<EntityPK, String> map = Maps.newHashMap();
        map.put(pk1, "ok");
        System.out.println(map.get(pk2));
    }

    @Test
    public void testSerObject() {
        String a = "test object";
        byte[] b = IOUtil.getBytes(a);
        String c = IOUtil.getObject(b, String.class);
        Assert.assertEquals(a, c);
    }

}
