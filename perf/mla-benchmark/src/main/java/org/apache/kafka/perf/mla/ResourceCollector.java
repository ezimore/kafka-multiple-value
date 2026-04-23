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
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

/**
 * Collects resource usage metrics for a running process.
 *
 * Usage:
 *   java ResourceCollector <pid> <label> <outputFile> <intervalMs> <durationSec>
 *
 * Collects:
 *   - CPU time (user + system) from /proc/<pid>/stat
 *   - RSS memory from /proc/<pid>/status
 *   - I/O bytes read/written from /proc/<pid>/io
 *
 * Also collects disk usage for a given directory:
 *   java ResourceCollector disk <directory> <outputFile>
 */
public class ResourceCollector {

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage:");
            System.err.println("  ResourceCollector <pid> <label> <outputFile> <intervalMs> <durationSec>");
            System.err.println("  ResourceCollector disk <directory> <outputFile>");
            System.exit(1);
        }

        if ("disk".equals(args[0])) {
            collectDiskUsage(args[1], args[2]);
        } else {
            int pid = Integer.parseInt(args[0]);
            String label = args[1];
            String outputFile = args[2];
            long intervalMs = Long.parseLong(args[3]);
            int durationSec = Integer.parseInt(args[4]);
            collectProcessMetrics(pid, label, outputFile, intervalMs, durationSec);
        }
    }

    private static void collectProcessMetrics(int pid, String label, String outputFile,
                                               long intervalMs, int durationSec) throws Exception {
        Path procStat = Paths.get("/proc", String.valueOf(pid), "stat");
        Path procStatus = Paths.get("/proc", String.valueOf(pid), "status");
        Path procIo = Paths.get("/proc", String.valueOf(pid), "io");

        long iterations = (durationSec * 1000L) / intervalMs;

        long peakRssKb = 0;
        long startCpuTicks = 0;
        long endCpuTicks = 0;
        long startIoRead = 0;
        long startIoWrite = 0;
        long endIoRead = 0;
        long endIoWrite = 0;

        // Initial snapshot
        startCpuTicks = getCpuTicks(procStat);
        long[] startIo = getIoBytes(procIo);
        startIoRead = startIo[0];
        startIoWrite = startIo[1];

        for (long i = 0; i < iterations; i++) {
            Thread.sleep(intervalMs);

            if (!Files.exists(procStat)) {
                System.err.println("Process " + pid + " no longer exists.");
                break;
            }

            long rssKb = getRssKb(procStatus);
            if (rssKb > peakRssKb) {
                peakRssKb = rssKb;
            }
        }

        // Final snapshot
        if (Files.exists(procStat)) {
            endCpuTicks = getCpuTicks(procStat);
            long[] endIo = getIoBytes(procIo);
            endIoRead = endIo[0];
            endIoWrite = endIo[1];
        }

        long cpuTicksDelta = endCpuTicks - startCpuTicks;
        double cpuSeconds = cpuTicksDelta / 100.0; // Linux uses 100 Hz clock ticks
        long ioReadBytes = endIoRead - startIoRead;
        long ioWriteBytes = endIoWrite - startIoWrite;

        try (PrintWriter pw = new PrintWriter(outputFile)) {
            pw.printf("label=%s%n", label);
            pw.printf("pid=%d%n", pid);
            pw.printf("cpu_seconds=%.2f%n", cpuSeconds);
            pw.printf("peak_rss_mb=%.1f%n", peakRssKb / 1024.0);
            pw.printf("io_read_mb=%.1f%n", ioReadBytes / (1024.0 * 1024.0));
            pw.printf("io_write_mb=%.1f%n", ioWriteBytes / (1024.0 * 1024.0));
        }

        System.out.printf("[RESOURCE] %s (pid %d): CPU=%.1fs, RSS=%.0fMB, IO_R=%.0fMB, IO_W=%.0fMB%n",
            label, pid, cpuSeconds, peakRssKb / 1024.0,
            ioReadBytes / (1024.0 * 1024.0), ioWriteBytes / (1024.0 * 1024.0));
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

    /** Read user + system CPU ticks from /proc/<pid>/stat (fields 14 and 15, 0-indexed 13 and 14). */
    private static long getCpuTicks(Path procStat) {
        try {
            String line = Files.readString(procStat).trim();
            // Skip past the comm field (which may contain spaces and parentheses)
            int closeParenIdx = line.lastIndexOf(')');
            String[] fields = line.substring(closeParenIdx + 2).split("\\s+");
            // fields[11] = utime, fields[12] = stime (0-indexed from after the ')' + state)
            long utime = Long.parseLong(fields[11]);
            long stime = Long.parseLong(fields[12]);
            return utime + stime;
        } catch (Exception e) {
            return 0;
        }
    }

    /** Read VmRSS from /proc/<pid>/status. */
    private static long getRssKb(Path procStatus) {
        try (BufferedReader br = new BufferedReader(new FileReader(procStatus.toFile()))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.startsWith("VmRSS:")) {
                    String[] parts = line.split("\\s+");
                    return Long.parseLong(parts[1]); // in kB
                }
            }
        } catch (Exception e) {
            // ignore
        }
        return 0;
    }

    /** Read read_bytes and write_bytes from /proc/<pid>/io. */
    private static long[] getIoBytes(Path procIo) {
        long readBytes = 0;
        long writeBytes = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(procIo.toFile()))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.startsWith("read_bytes:")) {
                    readBytes = Long.parseLong(line.split("\\s+")[1]);
                } else if (line.startsWith("write_bytes:")) {
                    writeBytes = Long.parseLong(line.split("\\s+")[1]);
                }
            }
        } catch (Exception e) {
            // ignore — /proc/<pid>/io may not be readable
        }
        return new long[]{readBytes, writeBytes};
    }
}
