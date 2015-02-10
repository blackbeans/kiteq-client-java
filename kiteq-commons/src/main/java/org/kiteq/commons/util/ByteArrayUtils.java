package org.kiteq.commons.util;

/**
 * @author gaofeihang
 * @since Jan 4, 2015
 */
public class ByteArrayUtils {
    
    private static final char[] HEX_CHAR =  new char[] {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

    public static String prettyPrint(byte[] bytes) {
        
        if (bytes == null) {
            return null;
        }
        
        StringBuffer sb = new StringBuffer();
        
        for (byte b : bytes) {
            if (b > 31 && b < 127 || b == 10 || b == 13) {
                sb.append((char) b);
            } else {
                if (sb.length() != 0 && sb.charAt(sb.length() - 1) != ' ') {
                    sb.append(' ');
                }
                sb.append(getHexString(b)).append(' ');;
            }
        }

        return sb.toString();
    }
    
    private static String getHexString(byte b) {
        int i = b < 0 ? b + 256 : b;
        return "0x" + HEX_CHAR[i / 16] + HEX_CHAR[i % 16];
    }

}
