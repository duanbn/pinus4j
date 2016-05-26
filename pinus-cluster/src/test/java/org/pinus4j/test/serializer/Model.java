package org.pinus4j.test.serializer;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = false)
public class Model extends BaseTest implements Serializable {

    private static final long       serialVersionUID = -6212501743856350593L;

    private boolean                 bool;
    private boolean                 nbool;
    private boolean[]               bools;
    private boolean[]               nbools;
    private Boolean                 obool;
    private Boolean                 nobool;
    private Boolean[]               obools;
    private Boolean[]               nobools;

    private char                    ch;
    private char                    nch;
    private char[]                  chs;
    private char[]                  nchs;
    private Character               och;
    private Character               noch;
    private Character[]             ochs;
    private Character[]             nochs;

    private byte                    b;
    private byte                    nb;
    private byte[]                  bs;
    private byte[]                  nbs;
    private Byte                    ob;
    private Byte[]                  nob;
    private Byte[]                  obs;
    private Byte[]                  nobs;

    private short                   sh;
    private short                   nsh;
    private short[]                 shs;
    private short[]                 nshs;
    private Short                   osh;
    private Short                   nosh;
    private Short[]                 oshs;
    private Short[]                 noshs;

    private int                     i;
    private int                     ni;
    private int[]                   is;
    private int[]                   nis;
    private Integer                 oi;
    private Integer                 noi;
    private Integer[]               ois;
    private Integer[]               nois;

    private long                    l;
    private long                    nl;
    private long[]                  ls;
    private long[]                  nls;
    private Long                    ol;
    private Long                    nol;
    private Long[]                  ols;
    private Long[]                  nols;

    private float                   f;
    private float                   nf;
    private float[]                 fs;
    private float[]                 nfs;
    private Float                   of;
    private Float                   nof;
    private Float[]                 ofs;
    private Float[]                 nofs;

    private double                  d;
    private double                  nd;
    private double[]                ds;
    private double[]                nds;
    private Double                  od;
    private Double                  nod;
    private Double[]                ods;
    private Double[]                nods;

    private String                  s;
    private String                  ns;
    private String[]                ss;
    private String[]                nss;

    private String                  s1;
    private String                  s2;
    private String                  s3;
    private String                  s4;
    private String                  s5;
    private String                  s6;
    private String                  s7;
    private String                  s8;

    private Date                    date;
    private Date[]                  dates;
    private java.sql.Date           sqlDate;
    private java.sql.Date[]         sqlDates;
    private Calendar                cal;
    private Calendar[]              cals;
    private Timestamp               time;
    private Timestamp[]             times;

    private EnumTest                et               = EnumTest.A;
    private EnumTest[]              ets              = new EnumTest[] { EnumTest.A, null, EnumTest.B };

    private SubModel                sm;
    private SubModel[]              sms;

    private List<String>            list1            = new ArrayList<String>();
    private List<?>                 nlist;
    private List<Object>            list2            = new ArrayList<Object>();

    private Set<SubModel>           set1             = new HashSet<SubModel>();
    private Set<SubModel>           set2             = new TreeSet<SubModel>();
    private Set<SubModel>           set3             = new LinkedHashSet<SubModel>();

    private Map<SubModel, SubModel> map              = new HashMap<SubModel, SubModel>();
    private Map<String, Object>     map1             = new HashMap<String, Object>();
    private Map<Object, Object>     map3             = new HashMap<Object, Object>();

    public Model() {
        bool = true;
        bools = new boolean[] { true, false, true };
        obool = new Boolean(false);
        obools = new Boolean[] { new Boolean(true), new Boolean(false) };

        ch = '我';
        chs = new char[] { '哦', '啊', '们' };
        och = new Character('a');
        ochs = new Character[] { new Character('a'), new Character('b') };

        b = (byte) 1;
        bs = new byte[] { (byte) 1, (byte) 2 };
        ob = new Byte((byte) 1);
        obs = new Byte[] { new Byte((byte) 1), new Byte((byte) 2) };

        sh = (short) 1;
        shs = new short[] { (short) 1, (short) 2 };
        osh = new Short((short) 1);
        oshs = new Short[] { new Short((short) 1), new Short((short) 1) };

        i = 1;
        is = new int[] { 1, 2 };
        oi = new Integer(1);
        ois = new Integer[] { new Integer(1), new Integer(1) };

        l = 1l;
        ls = new long[] { 1l, 2l };
        ol = new Long(1l);
        ols = new Long[] { new Long(1l), new Long(2l) };

        f = 1.1f;
        fs = new float[] { 1.1f, 1.2f };
        of = new Float(1.2f);
        ofs = new Float[] { new Float(1.2f), new Float(1.2f) };

        d = 1.1;
        ds = new double[] { 1.1, 1.2 };
        od = new Double(1.1);
        ods = new Double[] { new Double(1.1), new Double(1.1) };

        s = "alkjdflkjdsfkjdskfjsldkjflkdsjf";
        ss = new String[] { "alkjdflkjdsfkjdskfjsldkjflkdsjf", "alkjdflkjdsfkjdskfjsldkjflkdsjf" };

        s1 = genWord(50);
        s2 = genWord(10);
        s3 = genWord(50);
        s4 = genWord(100);
        s5 = genWord(50);
        s6 = genWord(500);
        s7 = genWord(30);
        s8 = genWord(20);

        date = new Date();
        dates = new Date[] { new Date(), new Date(), new Date() };
        sqlDate = new java.sql.Date(System.currentTimeMillis());
        sqlDates = new java.sql.Date[] { new java.sql.Date(System.currentTimeMillis()),
                new java.sql.Date(System.currentTimeMillis()) };
        cal = Calendar.getInstance();
        cals = new Calendar[] { Calendar.getInstance(), Calendar.getInstance(), Calendar.getInstance(),
                Calendar.getInstance() };
        time = new Timestamp(System.currentTimeMillis());
        times = new Timestamp[] { new Timestamp(System.currentTimeMillis()), new Timestamp(System.currentTimeMillis()) };

        sm = new SubModel();
        sms = new SubModel[] { new SubModel(), null, new SubModel(), null };

        list1.add("aa");
        list1.add("bb");
        list2.add("hello world");
        list2.add(1);
        list2.add(new SubModel());

        set1.add(new SubModel());
        set1.add(new SubModel());
        set2.add(new SubModel());
        set2.add(new SubModel());
        set3.add(new SubModel());
        set3.add(new SubModel());

        map.put(new SubModel(), new SubModel());
        map1.put("test server", new SubModel());
        map3.put(new SubModel(), new SubModel());
    }

}
