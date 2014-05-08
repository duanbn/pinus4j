package com.pinus.test;

import java.io.*;
import java.util.*;

import com.pinus.core.ser.Serializable;

public class SubModel extends BaseTest implements Serializable, java.io.Serializable, Comparable<SubModel>
{

    public boolean bool = true;
    public boolean[] bools = new boolean[] {true, false, true};
    public Boolean obool = new Boolean(false);
    public Boolean[] obools = new Boolean[] {new Boolean(true), new Boolean(false)};

    public char ch = '我';
    public char[] chs = new char[] {'哦', '啊', '们'};
    public Character och = new Character('a');
    public Character[] ochs = new Character[] {new Character('a'), new Character('b')};

    public byte b = (byte)1;
    public byte[] bs = new byte[] {(byte)1, (byte)2};
    public Byte ob = new Byte((byte)1);
    public Byte[] obs = new Byte[] {new Byte((byte)1), new Byte((byte)2)};

    public short sh = (short)1;
    public short[] shs = new short[] {(short)1, (short)2};
    public Short osh = new Short((short)1);
    public Short[] oshs = new Short[] {new Short((short)1), new Short((short)1)};

    public int i = 1;
    public int[] is = new int[] {1,2};
    public Integer oi = new Integer(1);
    public Integer[] ois = new Integer[] {new Integer(1), new Integer(1)};

    public long l = 1l;
    public long[] ls = new long[] {1l, 2l};
    public Long ol = new Long(1l);
    public Long[] ols = new Long[] {new Long(1l), new Long(2l)};

    public float f = 1.1f;
    public float[] fs = new float[] {1.1f, 1.2f};
    public Float of = new Float(1.2f);
    public Float[] ofs = new Float[] {new Float(1.2f), new Float(1.2f)};

    public double d = 1.1;
    public double[] ds = new double[] {1.1, 1.2};
    public Double od = new Double(1.1);
    public Double[] ods = new Double[] {new Double(1.1), new Double(1.1)};

    public String s = "alkjdflkjdsfkjdskfjsldkjflkdsjf";
    public String[] ss = new String[] {"alkjdflkjdsfkjdskfjsldkjflkdsjf", "alkjdflkjdsfkjdskfjsldkjflkdsjf"};

    public String s1;
    public String s2;
    public String s3;
    public String s4;
    public String s5;
    public String s6;
    public String s7;
    public String s8;
    public String s9;
    public String s10;
    public String s11;
    public String s12;
    public String s13;
    public String s14;

    public EnumTest et = EnumTest.A;
    public EnumTest[] ets = new EnumTest[] {EnumTest.A, null, EnumTest.B};

    public List<String> list;
    public List<String> list1 = new ArrayList<String>();

    public SubModel()
    {
        s1 = genWord(100);
        s2 = genWord(100);
        s3 = genWord(100);
        s4 = genWord(100);
        s6 = genWord(500);
        s7 = genWord(500);
        s8 = genWord(500);
        s9 = genWord(500);
        s10 = genWord(100);
        s11 = genWord(100);
        s12 = genWord(100);
        s13 = genWord(100);
        s14 = genWord(100);

        for (int i=0; i<50; i++) {
            list1.add(s1);
        }
    }

    public String toString()
    {
        StringBuilder info = new StringBuilder("{");

        info.append("bool=").append(bool).append(",");
        info.append("bools=").append("[");
        if (bools != null) {
            for (int i=0; i<bools.length; i++)
                info.append(bools[i]).append(",");
        }
        info.append("]").append(",");
        info.append("obool=").append(obool).append(",");
        info.append("obools=").append("[");
        if (obools != null) {
            for (int i=0; i<obools.length; i++)
                info.append(obools[i]).append(",");
        }
        info.append("]").append(",");

        info.append("ch=").append(ch).append(",");
        info.append("chs=").append("[");
        if (chs != null) {
            for (int i=0; i<chs.length; i++)
                info.append(chs[i]).append(",");
        }
        info.append("]").append(",");
        info.append("och=").append(och).append(",");
        info.append("ochs=").append("[");
        if (ochs != null) {
            for (int i=0; i<ochs.length; i++)
                info.append(ochs[i]).append(",");
        }
        info.append("]").append(",");

        info.append("b=").append(b).append(",");
        info.append("bs=").append("[");
        if (bs != null) {
            for (int i=0; i<bs.length; i++)
                info.append(bs[i]).append(",");
        }
        info.append("]").append(",");
        info.append("ob=").append(ob).append(",");
        info.append("obs=").append("[");
        if (obs != null) {
            for (int i=0; i<obs.length; i++)
                info.append(obs[i]).append(",");
        }
        info.append("]").append(",");

        info.append("sh=").append(sh).append(",");
        info.append("shs=").append("[");
        if (shs != null) {
            for (int i=0; i<shs.length; i++)
                info.append(shs[i]).append(",");
        }
        info.append("]").append(",");
        info.append("osh=").append(osh).append(",");
        info.append("oshs=").append("[");
        if (oshs != null) {
            for (int i=0; i<oshs.length; i++)
                info.append(oshs[i]).append(",");
        }
        info.append("]").append(",");

        info.append("i=").append(i).append(",");
        info.append("is=").append("[");
        if (is != null) {
            for (int i=0; i<is.length; i++)
                info.append(is[i]).append(",");
        }
        info.append("]").append(",");
        info.append("oi=").append(oi).append(",");
        info.append("ois=").append("[");
        if (ois != null) {
            for (int i=0; i<ois.length; i++)
                info.append(ois[i]).append(",");
        }
        info.append("]").append(",");

        info.append("l=").append(l).append(",");
        info.append("ls=").append("[");
        if (ls != null) {
            for (int i=0; i<ls.length; i++)
                info.append(ls[i]).append(",");
        }
        info.append("]").append(",");
        info.append("ol=").append(ol).append(",");
        info.append("ols=").append("[");
        if (ols != null) {
            for (int i=0; i<ols.length; i++)
                info.append(ols[i]).append(",");
        }
        info.append("]").append(",");

        info.append("f=").append(f).append(",");
        info.append("fs=").append("[");
        if (fs != null) {
            for (int i=0; i<fs.length; i++)
                info.append(fs[i]).append(",");
        }
        info.append("]").append(",");
        info.append("of=").append(of).append(",");
        info.append("ofs=").append("[");
        if (ofs != null) {
            for (int i=0; i<ofs.length; i++)
                info.append(ofs[i]).append(",");
        }
        info.append("]").append(",");

        info.append("d=").append(d).append(",");
        info.append("ds=").append("[");
        if (ds != null) {
            for (int i=0; i<ds.length; i++)
                info.append(ds[i]).append(",");
        }
        info.append("]").append(",");
        info.append("od=").append(od).append(",");
        info.append("ods=").append("[");
        if (ods != null) {
            for (int i=0; i<ods.length; i++)
                info.append(ods[i]).append(",");
        }
        info.append("]").append(",");

        info.append("s=").append(s).append(",");
        info.append("ss=").append("[");
        if (ss != null) {
            for (int i=0; i<ss.length; i++)
                info.append(ss[i]).append(",");
        }
        info.append("]").append(",");
        info.append("et=").append(et).append(",");
        if (ets != null) {
            for (int i=0; i<ets.length; i++)
                info.append(ets[i]).append(",");
        }
        info.append("]");

        info.append("}");

        return info.toString();
    }

    public int compareTo(SubModel o) {
        return 1 - 2;
    }

}
