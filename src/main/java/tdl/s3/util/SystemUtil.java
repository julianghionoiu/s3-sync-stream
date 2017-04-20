package tdl.s3.util;


import org.apache.commons.lang3.SystemUtils;

import java.io.*;

public class SystemUtil {

    public static boolean isProcessAlive(int pid) throws IOException {
        Process process = getProcess(pid);
        InputStream processOut = process.getInputStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(processOut));
        for (String line = reader.readLine(); line != null; line = reader.readLine()) {
            if (line.contains("" + pid)) return true;
        }
        return false;
    }

    private static Process getProcess(int pid) throws IOException {
        if (SystemUtils.IS_OS_WINDOWS) {
            return getWindowsProcess(pid);
        } else if (SystemUtils.IS_OS_UNIX) {
            return getUnixProcess(pid);
        } else {
            throw new RuntimeException("Undefined system " + System.getProperty("os.name"));
        }
    }

    private static Process getWindowsProcess(int pid) throws IOException {
        return Runtime.getRuntime().exec("tasklist /FI \"PID eq" + pid +"\"");
    }

    private static Process getUnixProcess(int pid) throws IOException {
        return Runtime.getRuntime().exec("ps --pid " + pid);
    }
}
