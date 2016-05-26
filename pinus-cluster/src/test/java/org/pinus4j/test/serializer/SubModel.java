package org.pinus4j.test.serializer;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.Data;
import lombok.EqualsAndHashCode;

import com.google.common.collect.Lists;

@Data
@EqualsAndHashCode(callSuper = false)
public class SubModel extends BaseTest implements Serializable, Comparable<SubModel> {

    private static final long  serialVersionUID = 7437492659713158374L;

    public boolean             bool             = true;
    public boolean[]           bools            = new boolean[] { true, false, true };
    public Boolean             obool            = new Boolean(false);
    public Boolean[]           obools           = new Boolean[] { new Boolean(true), new Boolean(false) };

    public char                ch               = '我';
    public char[]              chs              = new char[] { '哦', '啊', '们' };
    public Character           och              = new Character('a');
    public Character[]         ochs             = new Character[] { new Character('a'), new Character('b') };

    public byte                b                = (byte) 1;
    public byte[]              bs               = new byte[] { (byte) 1, (byte) 2 };
    public Byte                ob               = new Byte((byte) 1);
    public Byte[]              obs              = new Byte[] { new Byte((byte) 1), new Byte((byte) 2) };

    public short               sh               = (short) 1;
    public short[]             shs              = new short[] { (short) 1, (short) 2 };
    public Short               osh              = new Short((short) 1);
    public Short[]             oshs             = new Short[] { new Short((short) 1), new Short((short) 1) };

    public int                 i                = 1;
    public int[]               is               = new int[] { 1, 2 };
    public Integer             oi               = new Integer(1);
    public Integer[]           ois              = new Integer[] { new Integer(1), new Integer(1) };

    public long                l                = 1l;
    public long[]              ls               = new long[] { 1l, 2l };
    public Long                ol               = new Long(1l);
    public Long[]              ols              = new Long[] { new Long(1l), new Long(2l) };

    public float               f                = 1.1f;
    public float[]             fs               = new float[] { 1.1f, 1.2f };
    public Float               of               = new Float(1.2f);
    public Float[]             ofs              = new Float[] { new Float(1.2f), new Float(1.2f) };

    public double              d                = 1.1;
    public double[]            ds               = new double[] { 1.1, 1.2 };
    public Double              od               = new Double(1.1);
    public Double[]            ods              = new Double[] { new Double(1.1), new Double(1.1) };

    public String              s                = "alkjdflkjdsfkjdskfjsldkjflkdsjf";
    public String[]            ss               = new String[] { "alkjdflkjdsfkjdskfjsldkjflkdsjf",
            "alkjdflkjdsfkjdskfjsldkjflkdsjf"  };

    public String              s1               = genWord(100);
    public String              s2               = genWord(100);
    public String              s3               = genWord(100);
    public String              s4               = genWord(100);
    public String              s5               = genWord(100);
    public String              s6               = genWord(100);
    public String              s7               = genWord(100);
    public String              s8               = genWord(100);
    public String              s9               = genWord(100);
    public String              s10              = genWord(100);
    public String              s11              = genWord(100);
    public String              s12              = genWord(100);
    public String              s13              = genWord(100);
    public String              s14              = genWord(100);

    public Date                date             = new Date();
    public Date[]              dates            = new Date[] { new Date(), new Date(), new Date() };
    public java.sql.Date       sqlDate          = new java.sql.Date(System.currentTimeMillis());
    public java.sql.Date[]     sqlDates         = new java.sql.Date[] { new java.sql.Date(System.currentTimeMillis()),
            new java.sql.Date(System.currentTimeMillis()) };
    public Calendar            cal              = Calendar.getInstance();
    public Calendar[]          cals             = new Calendar[] { Calendar.getInstance(), Calendar.getInstance(),
            Calendar.getInstance(), Calendar.getInstance() };
    public Timestamp           time             = new Timestamp(System.currentTimeMillis());
    public Timestamp[]         times            = new Timestamp[] { new Timestamp(System.currentTimeMillis()),
            new Timestamp(System.currentTimeMillis()) };

    public EnumTest            et               = EnumTest.A;
    public EnumTest[]          ets              = new EnumTest[] { EnumTest.A, null, EnumTest.B };

    public List<String>        list;
    public List<String>        list1            = Lists.newArrayList(s1, s2, s3);

    public Map<String, Object> map              = new HashMap<String, Object>();

    @Override
    public int compareTo(SubModel o) {
        return 1 - 2;
    }

}
