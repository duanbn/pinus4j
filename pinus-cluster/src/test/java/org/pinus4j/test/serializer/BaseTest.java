package org.pinus4j.test.serializer;

import java.sql.Timestamp;
import java.util.*;
import java.io.*;
import java.util.zip.*;

import org.codehaus.jackson.map.*;

public class BaseTest {
    protected char[] words = new char[] { '第', '提', '去', '额', '我', 'a', '速', '而', '的', '分', '平', '吗', '库', '你' };
    protected Random r     = new Random();

    protected String genWord(int count) {
        int length = words.length;
        char[] text = new char[count];
        for (int i = 0; i < count; i++) {
            text[i] = words[r.nextInt(length)];
        }

        return new String(text);
    }

    ObjectMapper mapper = new ObjectMapper();

    protected byte[] writeJson(Object o) throws Exception {
        return mapper.writeValueAsBytes(o);
    }

    protected <T> T readJson(byte[] b, Class<T> clazz) throws Exception {
        return (T) mapper.readValue(b, clazz);
    }

    protected byte[] writeObject(Object o) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);

        oos.writeObject(o);

        byte[] b = baos.toByteArray();
        baos.close();

        return b;
    }

    protected Object readObject(byte[] b) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bais = new ByteArrayInputStream(b);
        ObjectInputStream ois = new ObjectInputStream(bais);

        Object obj = ois.readObject();
        bais.close();

        return obj;
    }

    protected byte[] gzip(byte[] b) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        GZIPOutputStream gzip = new GZIPOutputStream(baos);
        gzip.write(b, 0, b.length);
        gzip.finish();
        byte[] gdata = baos.toByteArray();
        baos.close();

        return gdata;
    }

    protected void showLength(String desc, byte[] b) {
        System.out.println(desc + " serialize length: " + b.length);
    }

    protected void showLength(byte[] b) {
        System.out.println("serialize length: " + b.length);
    }

    protected Model createModel() {
        Model model = new Model();
        model.setBool(true);
        model.setBools(new boolean[] { true, false, true });
        model.setObool(new Boolean(false));
        model.setObools(new Boolean[] { new Boolean(true), new Boolean(false) });

        model.setCh('a');
        model.setChs(new char[] { '哦', '啊', '们' });
        model.setOch(new Character('a'));
        model.setOchs(new Character[] { new Character('a'), new Character('b') });

        model.setB((byte) 1);
        model.setBs(new byte[] { (byte) 1, (byte) 2 });
        model.setOb(new Byte((byte) 1));
        model.setObs(new Byte[] { new Byte((byte) 1), new Byte((byte) 2) });

        model.setSh((short) 1);
        model.setShs(new short[] { (short) 1, (short) 2 });
        model.setOsh(new Short((short) 1));
        model.setOshs(new Short[] { new Short((short) 1), new Short((short) 1) });

        model.setI(1);
        model.setIs(new int[] { 1, 2, 3, 4 });
        model.setOi(new Integer(1));
        model.setOis(new Integer[] { new Integer(1), new Integer(2) });

        model.setL(1l);
        model.setLs(new long[] { 1l, 2l });
        model.setOl(new Long(1l));
        model.setOls(new Long[] { new Long(1l), new Long(2l) });

        model.setF(1.1f);
        model.setFs(new float[] { 1.1f, 1.2f });
        model.setOf(new Float(1.2f));
        model.setOfs(new Float[] { new Float(1.2f), new Float(1.2f) });

        model.setD(1.1);
        model.setDs(new double[] { 1.1, 1.2 });
        model.setOd(new Double(1.1));
        model.setOds(new Double[] { new Double(1.1), new Double(1.1) });

        model.setS(genWord(20));
        model.setS1(genWord(25));
        model.setS2(genWord(30));
        model.setS3(genWord(40));
        model.setS4(genWord(80));
        model.setS5(genWord(160));

        model.setDate(new Date());
        model.setDates(new Date[] { new Date(), new Date(), new Date() });
        model.setSqlDate(new java.sql.Date(System.currentTimeMillis()));
        model.setSqlDates(new java.sql.Date[] { new java.sql.Date(System.currentTimeMillis()),
                new java.sql.Date(System.currentTimeMillis()) });
        model.setCal(Calendar.getInstance());
        model.setCals(new Calendar[] { Calendar.getInstance(), Calendar.getInstance(), Calendar.getInstance(),
                Calendar.getInstance() });
        model.setTime(new Timestamp(System.currentTimeMillis()));
        model.setTimes(new Timestamp[] { new Timestamp(System.currentTimeMillis()),
                new Timestamp(System.currentTimeMillis()) });

        model.setEt(EnumTest.A);
        model.setEts(new EnumTest[] { EnumTest.A, null, EnumTest.B });

        model.setSm(createInnerModel());
        model.setSms(new InnerModel[] { createInnerModel(), null, createInnerModel(), null });

        List<String> list1 = new ArrayList<String>();
        List<?> nlist = null;
        List<Object> list2 = new ArrayList<Object>();
        list1.add("aa");
        list1.add("bb");
        list2.add("hello world");
        list2.add(1);
        list2.add(createInnerModel());
        list2.add(createInnerModel());
        list2.add(createInnerModel());
        model.setList1(list1);
        model.setNlist(nlist);
        model.setList2(list2);

        Set<InnerModel> set1 = new HashSet<InnerModel>();
        Set<InnerModel> set2 = new TreeSet<InnerModel>();
        Set<InnerModel> set3 = new LinkedHashSet<InnerModel>();
        set1.add(createInnerModel());
        set1.add(createInnerModel());
        set2.add(createInnerModel());
        set2.add(createInnerModel());
        set3.add(createInnerModel());
        set3.add(createInnerModel());
        model.setSet1(set1);
        model.setSet2(set2);
        model.setSet3(set3);

        Map<InnerModel, InnerModel> map = new HashMap<InnerModel, InnerModel>();
        Map<String, Object> map1 = new HashMap<String, Object>();
        Map<Object, Object> map3 = new HashMap<Object, Object>();
        map.put(createInnerModel(), createInnerModel());
        map1.put("test server", createInnerModel());
        map3.put(createInnerModel(), createInnerModel());
        model.setMap(map);
        model.setMap1(map1);
        model.setMap3(map3);

        return model;
    }

    protected InnerModel createInnerModel() {
        InnerModel model = new InnerModel();
        model.setBool(true);
        model.setBools(new boolean[] { true, false, true });
        model.setObool(new Boolean(false));
        model.setObools(new Boolean[] { new Boolean(true), new Boolean(false) });

        model.setCh('a');
        model.setChs(new char[] { '哦', '啊', '们' });
        model.setOch(new Character('a'));
        model.setOchs(new Character[] { new Character('a'), new Character('b') });

        model.setB((byte) 1);
        model.setBs(new byte[] { (byte) 1, (byte) 2 });
        model.setOb(new Byte((byte) 1));
        model.setObs(new Byte[] { new Byte((byte) 1), new Byte((byte) 2) });

        model.setSh((short) 1);
        model.setShs(new short[] { (short) 1, (short) 2 });
        model.setOsh(new Short((short) 1));
        model.setOshs(new Short[] { new Short((short) 1), new Short((short) 1) });

        model.setI(1);
        model.setIs(new int[] { 1, 2, 3, 4 });
        model.setOi(new Integer(1));
        model.setOis(new Integer[] { new Integer(1), new Integer(2) });

        model.setL(1l);
        model.setLs(new long[] { 1l, 2l });
        model.setOl(new Long(1l));
        model.setOls(new Long[] { new Long(1l), new Long(2l) });

        model.setF(1.1f);
        model.setFs(new float[] { 1.1f, 1.2f });
        model.setOf(new Float(1.2f));
        model.setOfs(new Float[] { new Float(1.2f), new Float(1.2f) });

        model.setD(1.1);
        model.setDs(new double[] { 1.1, 1.2 });
        model.setOd(new Double(1.1));
        model.setOds(new Double[] { new Double(1.1), new Double(1.1) });

        model.setS(genWord(20));
        model.setS1(genWord(25));
        model.setS2(genWord(30));
        model.setS3(genWord(40));

        model.setDate(new Date());
        model.setDates(new Date[] { new Date(), new Date(), new Date() });
        model.setSqlDate(new java.sql.Date(System.currentTimeMillis()));
        model.setSqlDates(new java.sql.Date[] { new java.sql.Date(System.currentTimeMillis()),
                new java.sql.Date(System.currentTimeMillis()) });
        model.setCal(Calendar.getInstance());
        model.setCals(new Calendar[] { Calendar.getInstance(), Calendar.getInstance(), Calendar.getInstance(),
                Calendar.getInstance() });
        model.setTime(new Timestamp(System.currentTimeMillis()));
        model.setTimes(new Timestamp[] { new Timestamp(System.currentTimeMillis()),
                new Timestamp(System.currentTimeMillis()) });

        model.setEt(EnumTest.A);
        model.setEts(new EnumTest[] { EnumTest.A, null, EnumTest.B });

        return model;
    }

}
