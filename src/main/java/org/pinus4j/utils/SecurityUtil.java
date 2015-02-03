package org.pinus4j.utils;

import java.security.*;

/**
 * security util.
 *
 * @author duanbn
 */
public class SecurityUtil {

    public static final String md5(String value) {
        byte[] bytes = value.getBytes();

        MessageDigest md = null;

        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }

        md.update(bytes);

        byte[] newByte = md.digest();

        StringBuilder str = new StringBuilder();

        for (int i=0; i<newByte.length; i++) {
            if ((newByte[i] & 0xff) < 0x10) { 
                str.append("0"); 
            } 
            str.append(Long.toString(newByte[i] & 0xff, 16));
        }

        return str.toString();

    }

}
