package com.pinus.test;

import java.io.*;
import java.util.*;

import com.pinus.core.ser.Serializable;

public class Model extends BaseTest implements Serializable, java.io.Serializable
{

    public boolean bool;
    public boolean nbool;
    public boolean[] bools;
    public boolean[] nbools;
    public Boolean obool;
    public Boolean nobool;
    public Boolean[] obools;
    public Boolean[] nobools;

    public char ch;
    public char nch;
    public char[] chs;
    public char[] nchs;
    public Character och;
    public Character noch;
    public Character[] ochs;
    public Character[] nochs;

    public byte b;
    public byte nb;
    public byte[] bs;
    public byte[] nbs;
    public Byte ob;
    public Byte[] nob;
    public Byte[] obs;
    public Byte[] nobs;

    public short sh;
    public short nsh;
    public short[] shs;
    public short[] nshs;
    public Short osh;
    public Short nosh;
    public Short[] oshs;
    public Short[] noshs;

    public int i;
    public int ni;
    public int[] is;
    public int[] nis;
    public Integer oi;
    public Integer noi;
    public Integer[] ois;
    public Integer[] nois;

    public long l;
    public long nl;
    public long[] ls;
    public long[] nls;
    public Long ol;
    public Long nol;
    public Long[] ols;
    public Long[] nols;

    public float f;
    public float nf;
    public float[] fs;
    public float[] nfs;
    public Float of;
    public Float nof;
    public Float[] ofs;
    public Float[] nofs;

    public double d;
    public double nd;
    public double[] ds;
    public double[] nds;
    public Double od;
    public Double nod;
    public Double[] ods;
    public Double[] nods;

    public String s;
    public String ns;
    public String[] ss;
    public String[] nss;

    public String s1;
    public String s2;
    public String s3;
    public String s4;
    public String s5;
    public String s6;
    public String s7;
    public String s8;

    public SubModel sm;
    public SubModel[] sms;

    public List<String> list1 = new ArrayList<String>();
    public List nlist;
    public List<SubModel> list2 = new ArrayList<SubModel>();

    public Set<SubModel> set1 = new HashSet<SubModel>();
    public Set<SubModel> set2 = new TreeSet<SubModel>();
    public Set<SubModel> set3 = new LinkedHashSet<SubModel>();

    public Map<SubModel, SubModel> map = new HashMap<SubModel, SubModel>();

    public Model()
    {
        bool = true;
        bools = new boolean[] {true, false, true};
        obool = new Boolean(false);
        obools = new Boolean[] {new Boolean(true), new Boolean(false)};

        ch = '我';
        chs = new char[] {'哦', '啊', '们'};
        och = new Character('a');
        ochs = new Character[] {new Character('a'), new Character('b')};

        b = (byte)1;
        bs = new byte[] {(byte)1, (byte)2};
        ob = new Byte((byte)1);
        obs = new Byte[] {new Byte((byte)1), new Byte((byte)2)};

        sh = (short)1;
        shs = new short[] {(short)1, (short)2};
        osh = new Short((short)1);
        oshs = new Short[] {new Short((short)1), new Short((short)1)};

        i = 1;
        is = new int[] {1,2};
        oi = new Integer(1);
        ois = new Integer[] {new Integer(1), new Integer(1)};

        l = 1l;
        ls = new long[] {1l, 2l};
        ol = new Long(1l);
        ols = new Long[] {new Long(1l), new Long(2l)};

        f = 1.1f;
        fs = new float[] {1.1f, 1.2f};
        of = new Float(1.2f);
        ofs = new Float[] {new Float(1.2f), new Float(1.2f)};

        d = 1.1;
        ds = new double[] {1.1, 1.2};
        od = new Double(1.1);
        ods = new Double[] {new Double(1.1), new Double(1.1)};

        s = "alkjdflkjdsfkjdskfjsldkjflkdsjf";
        ss = new String[] {"alkjdflkjdsfkjdskfjsldkjflkdsjf", "alkjdflkjdsfkjdskfjsldkjflkdsjf"};

        /*
        s1 = genWord(50);
        s2 = genWord(10);
        s3 = genWord(50);
        s4 = genWord(100);
        s5 = genWord(50);
        s6 = genWord(500);
        s7 = genWord(30);
        s8 = genWord(20);
        */
        
        sm = new SubModel();
        sms = new SubModel[] {new SubModel(), null, new SubModel(), null};

        list1.add("aa");
        list1.add("bb");
        list2.add(new SubModel());
        list2.add(new SubModel());

        set1.add(new SubModel());
        set1.add(new SubModel());
        set2.add(new SubModel());
        set2.add(new SubModel());
        set3.add(new SubModel());
        set3.add(new SubModel());

        map.put(new SubModel(), new SubModel());
    }

    public String toString()
    {
        StringBuilder info = new StringBuilder("{");

        info.append("bool=").append(bool).append(",");
        info.append("nbool=").append(nbool).append(",");
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
        info.append("nch=").append(nch).append(",");
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
        info.append("nb=").append(nb).append(",");
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
        info.append("nsh=").append(nsh).append(",");
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
        info.append("ni=").append(ni).append(",");
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
        info.append("nl=").append(nl).append(",");
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
        info.append("nf=").append(nf).append(",");
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
        info.append("nd=").append(nd).append(",");
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

        info.append("s1=").append(s1).append(",");
        info.append("s2=").append(s2).append(",");
        info.append("s3=").append(s3).append(",");
        info.append("s4=").append(s4).append(",");
        info.append("s5=").append(s5).append(",");
        info.append("s6=").append(s6).append(",");
        info.append("s7=").append(s7).append(",");
        info.append("s8=").append(s8).append(",");

        info.append("sm=").append(sm).append(",");

        info.append("list1=").append(list1).append(",");
        info.append("list2=").append(list2).append(",");
        info.append("set1=").append(set1).append(",");
        info.append("set2=").append(set2).append(",");
        info.append("set3=").append(set3).append(",");
        
        info.append("map=").append(map).append(",");
        
        info.append("}");

        return info.toString();
    }
}
