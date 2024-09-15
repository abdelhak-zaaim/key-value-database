package com.zaaim.kv.db.parser;

public class LZF {
    private static final int MAX_LITERAL = 32;

    public static void expand(byte[] in, int inPos, byte[] out, int outPos, int outLen) {
        if (inPos < 0 || outPos < 0 || outLen < 0) {
            throw new IllegalArgumentException();
        }
        do {
            int ctrl = in[inPos++] & 255;
            if (ctrl < MAX_LITERAL) {
                // literal run of length = ctrl + 1,
                ctrl++;
                // copy to output and move forward this many bytes
                System.arraycopy(in, inPos, out, outPos, ctrl);
                outPos += ctrl;
                inPos += ctrl;
            } else {
                // back reference
                // the highest 3 bits are the match length
                int len = ctrl >> 5;
                // if the length is maxed, add the next byte to the length
                if (len == 7) {
                    len += in[inPos++] & 255;
                }
                // minimum back-reference is 3 bytes,
                // so 2 was subtracted before storing size
                len += 2;

                // ctrl is now the offset for a back-reference...
                // the logical AND operation removes the length bits
                ctrl = -((ctrl & 0x1f) << 8) - 1;

                // the next byte augments/increases the offset
                ctrl -= in[inPos++] & 255;

                // copy the back-reference bytes from the given
                // location in output to current position
                ctrl += outPos;
                if (outPos + len > outLen) {
                    // reduce array bounds checking
                    throw new ArrayIndexOutOfBoundsException();
                }
                for (int i = 0; i < len; i++) {
                    out[outPos++] = out[ctrl++];
                }
            }
        } while (outPos < outLen);
    }
}