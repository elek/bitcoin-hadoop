package org.bitcoinj.script;

/**
 * Created by melek on 9/27/16.
 */
public class Util {

    public static String parseToString(byte[] raw) {
        try {
            StringBuilder b = new StringBuilder();
            Script script = new Script(raw);
            for (ScriptChunk chunk : script.getChunks()) {
                if (chunk.isOpCode()) {
                    b.append(chunk + " ");
                }
            }
            return b.toString();
        } catch (Exception ex) {
            return "ERROR";
        }
    }
}
