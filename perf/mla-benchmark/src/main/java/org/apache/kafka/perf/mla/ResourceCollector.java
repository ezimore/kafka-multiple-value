/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.perf.mla;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

/**
 * Collects resource usage metrics for a running process.
 *
 * Two modes:
 *
 *   snapshot: Take a single snapshot of a process and write to file.
 *     java ResourceCollector snapshot <pid> <label> <outputFile>
 *
 *   wait-and-snapshot: Wait for process to exit, then write final stats.
 *     java ResourceCollector wait <pid> <label> <outputFile>
 *
 *   disk: Measure disk usage of a directory.
 *     java ResourceCollector disk <directory> <outputFile>
 */
public class ResourceCollector {

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage:");
            System.err.println("  ResourceCollector snapshot <pid> <label> <outputFile>");
            System.err.println("  ResourceCollector wait <pid> <label> <outputFile>");
            System.err.println("  ResourceCollector disk <directory> <outputFile>");
            System.exit(1);
        }

        String mode = args[0];

        switch (mode) {
            case "disk":
                collectDiskUsage(args[1], args[2]);
                break;
            case "snapshot":
                takeSnapshot(Integer.parseInt(args[1]), args[2], args[3]);
                break;
            case "wait":
                waitAndCollect(Integer.parseInt(args[1]), args[2], args[3]);
                break;
            default:
                // Legacy mode: treat first arg as pid
                // java ResourceCollector <pid> <label> <outputFile> <intervalMs> <durationSec>
                waitAndCollect(Integer.parseInt(args[0]), args[1], args[2]);
                break;
        }
    }

    /**
     * Take a single snapshot of a process's resource usage.
     */
    private static void takeSnapshot(int pid, String label, String outputFile) throws Exception {
        Path procStat = Paths.get("/proc", String.valueOf(pid), "stat");
        Path procStatus = Paths.get("/proc", String.valueOf(pid), "status");
        Path procIo = Paths.get("/proc", String.valueOf(pid), "io");

        if (!Files.exists(procStat)) {
            writeZeros(label, pid, outputFile);
            return;
        }

        long cpuTicks = getCpuTicks(procStat);
        double cpuSeconds = cpuTicks / 100.0;
        long rssKb = getRssKb(procStatus);
        long[] io = getIoBytes(procIo);

        writeResults(label, pid, cpuSeconds, rssKb / 1024.0,
                     io[0] / (1024.0 * 1024.0), io[1] / (1024.0 * 1024.0), outputFile);
    }

    /**
     * Wait for a process to exit, polling its peak RSS along the way,
     * then write the final resource usage.
     */
    private static void waitAndCollect(int pid, String label, String outputFile) throws Exception {
        Path procStat = Paths.get("/proc", String.valueOf(pid), "stat");
        Path procStatus = Paths.get("/proc", String.valueOf(pid), "status");
        Path procIo = Paths.get("/proc", String.valueOf(pid), "io");

        // Wait for process to appear
        for (int i = 0; i < 30; i++) {
            if (Files.exists(procStat)) break;
            Thread.sleep(1000);
        }
        if (!Files.exists(procStat)) {
            writeZeros(label, pid, outputFile);
            return;
        }

        // Initial snapshot
        long startCpuTicks = getCpuTicks(procStat);
        long[] startIo = getIoBytes(procIo);
        long peakRssKb = getRssKb(procStatus);

        // Track last known values (updated every poll while process is alive)
        long lastCpuTicks = startCpuTicks;
        long lastIoRead = startIo[0];
        long lastIoWrite = startIo[1];

        // Poll until process exits, capturing latest values each iteration
        while (Files.exists(procStat)) {
            Thread.sleep(1000);
            if (!Files.exists(procStat)) break;

            long rss = getRssKb(procStatus);
            if (rss > peakRssKb) peakRssKb = rss;

            long cpu = getCpuTicks(procStat);
            if (cpu > 0) lastCpuTicks = cpu;

            long[] io = getIoBytes(procIo);
            if (io[0] > 0) lastIoRead = io[0];
            if (io[1] > 0) lastIoWrite = io[1];
        }

        double cpuSeconds = Math.max(0, lastCpuTicks - startCpuTicks) / 100.0;
        double ioReadMb = Math.max(0, lastIoRead - startIo[0]) / (1024.0 * 1024.0);
        double ioWriteMb = Math.max(0, lastIoWrite - startIo[1]) / (1024.0 * 1024.0);

        writeResults(label, pid, cpuSeconds, peakRssKb / 1024.0, ioReadMb, ioWriteMb, outputFile);
    }

    private static void writeResults(String label, int pid, double cpuSeconds,
                                      double peakRssMb, double ioReadMb, double ioWriteMb,
                                      String outputFile) throws Exception {
        try (PrintWriter pw = new PrintWriter(outputFile)) {
            pw.printf("label=%s%n", label);
            pw.printf("pid=%d%n", pid);
            pw.printf("cpu_seconds=%.2f%n", cpuSeconds);
            pw.printf("peak_rss_mb=%.1f%n", peakRssMb);
            pw.printf("io_read_mb=%.1f%n", ioReadMb);
            pw.printf("io_write_mb=%.1f%n", ioWriteMb);
        }
        System.out.printf("[RESOURCE] %s (pid %d): CPU=%.1fs, RSS=%.0fMB, IO_R=%.0fMB, IO_W=%.0fMB%n",
                label, pid, cpuSeconds, peakRssMb, ioReadMb, ioWriteMb);
    }

    private static void writeZeros(String label, int pid, String outputFile) throws Exception {
        writeResults(label, pid, 0, 0, 0, 0, outputFile);
        System.err.println("Process " + pid + " not found, writing zeros.");
    }

    private static void collectDiskUsage(String directory, String outputFile) throws Exception {
        long totalBytes = 0;
        File dir = new File(directory);
        if (dir.exists() && dir.isDirectory()) {
            try (Stream<Path> paths = Files.walk(dir.toPath())) {
                totalBytes = paths.filter(Files::isRegularFile)
                    .mapToLong(p -> p.toFile().length())
                    .sum();
            }
        }

        try (PrintWriter pw = new PrintWriter(outputFile)) {
            pw.printf("directory=%s%n", directory);
            pw.printf("total_bytes=%d%n", totalBytes);
            pw.printf("total_mb=%.1f%n", totalBytes / (1024.0 * 1024.0));
        }
        System.out.printf("[DISK] %s: %.1f MB%n", directory, totalBytes / (1024.0 * 1024.0));
    }

    /** Read user + system CPU ticks from /proc/pid/stat. */
    private static long getCpuTicks(Path procStat) {
        try {
            if (!Files.exists(procStat)) return 0;
            String line = Files.readString(procStat).trim();
            int closeParenIdx = line.lastIndexOf(')');
            if (closeParenIdx < 0) return 0;
            String[] fields = line.substring(closeParenIdx + 2).split("\\s+");
            if (fields.length < 13) return 0;
            long utime = Long.parseLong(fields[11]);
            long stime = Long.parseLong(fields[12]);
            return utime + stime;
        } catch (Exception e) {
            return 0;
        }
    }

    /** Read VmRSS from /proc/pid/status. */
    private static long getRssKb(Path procStatus) {
        try (BufferedReader br = new BufferedReader(new FileReader(procStatus.toFile()))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.startsWith("VmRSS:")) {
                    return Long.parseLong(line.split("\\s+")[1]);
                }
            }
        } catch (Exception e) { }
        return 0;
    }

    /** Read read_bytes and write_bytes from /proc/pid/io. */
    private static long[] getIoBytes(Path procIo) {
        long readBytes = 0, writeBytes = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(procIo.toFile()))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.startsWith("read_bytes:"))
                    readBytes = Long.parseLong(line.split("\\s+")[1]);
                else if (line.startsWith("write_bytes:"))
                    writeBytes = Long.parseLong(line.split("\\s+")[1]);
            }
        } catch (Exception e) { }
        return new long[]{readBytes, writeBytes};
    }
}
