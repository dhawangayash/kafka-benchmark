package com.datavisor.infra;


import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.TimeoutException;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.util.*;

import static net.sourceforge.argparse4j.impl.Arguments.store;

public class ProducerBenchmark {

    public static void main(String[] args) throws Exception {
        ArgumentParser parser = argParser();

        try {
            Namespace res = parser.parseArgs(args);

            /* parse args */
/**

            String topicName = res.getString("topic");
            long numRecords = res.getLong("numRecords");
            Integer recordSize = res.getInt("recordSize");
            int throughput = res.getInt("throughput");
            List<String> producerProps = res.getList("producerConfig");
            String producerConfig = res.getString("producerConfigFile");
            String payloadFilePath = res.getString("payloadFile");
*/


            String topicName = res.getString("topic");
            int recordPerMessage = res.getInt("recordPerMessage");
            int sampling = res.getInt("sampling");
            int readBufferSize = res.getInt("readBufferSize") * 1024;
            List<String> producerProps = res.getList("producerConfig");

            Properties props = new Properties();
            if (producerProps != null)
                for (String prop : producerProps) {
                    String[] pieces = prop.split("=");
                    if (pieces.length != 2)
                        throw new IllegalArgumentException("Invalid property: " + prop);
                    props.put(pieces[0], pieces[1]);
                }

            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
            KafkaProducer<byte[], byte[]> producer = new KafkaProducer<byte[], byte[]>(props);

            long batchSize = props.getProperty("batch.size") == null ? 16384L : Long.parseLong(props.getProperty("batch.size"));

            DataInputStream is = new DataInputStream(new BufferedInputStream(System.in, readBufferSize));
            int n = 120;
            Stats stats = new Stats(sampling, 5000, batchSize);
            while (n < sampling + 120) {
                Callback cb = stats.nextCompletion(System.currentTimeMillis(), ("hello_world:'" + n + "'").length(), stats);
                producer.send(new ProducerRecord<byte[], byte[]>(topicName, ("hello_world:'" + n++ + "'").getBytes()), cb);
            }
//            List<ByteArrayString> objs = new ArrayList<>(recordPerMessage);
//            try {
//                while (true) {
//                    is.readInt();
//                    is.readInt();
//                    int len = is.readInt();
//                    byte[] msg = new byte[len];
//                    is.readFully(msg);
//                    n++;
//
//                    objs.add(new ByteArrayString(msg));
//                    if (n == recordPerMessage) {
//                        byte[] payload = n == 1 ? msg : ByteArrayString.join(objs, (byte) 0x1e).getData();
//                        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topicName, payload);
//                        long sendStartMs = System.currentTimeMillis();
//                        Callback cb = stats.nextCompletion(sendStartMs, payload.length, stats);
//                        producer.send(record, cb);
//
//                        n = 0;
//                        objs.clear();
//                    }
//                }
//            } catch (EOFException e) {
//            }

            producer.close();
            stats.printTotal();
        } catch (ArgumentParserException e) {
            if (args.length == 0) {
                parser.printHelp();
                System.exit(0);
            } else {
                parser.handleError(e);
                System.exit(1);
            }
        }
    }

    /** Get the command-line argument parser. */
    private static ArgumentParser argParser() {
        ArgumentParser parser = ArgumentParsers
                .newArgumentParser("producer-performance")
                .defaultHelp(true)
                .description("This tool is used to verify the producer performance.");

        parser.addArgument("--topic")
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("TOPIC")
                .help("produce messages to this topic");

        parser.addArgument("--records-per-message")
                .action(store())
                .required(false)
                .type(Integer.class)
                .metavar("RECORD-PER-MESSAGE")
                .dest("recordPerMessage")
                .setDefault(1)
                .help("number of CAL records per message");

        parser.addArgument("--sampling")
                .action(store())
                .required(false)
                .type(Integer.class)
                .metavar("SAMPLING")
                .dest("sampling")
                .setDefault(100)
                .help("sampling interval");

        parser.addArgument("--read-buffer-in-kb")
                .action(store())
                .required(false)
                .type(Integer.class)
                .metavar("READ-BUFFER-SIZE")
                .dest("readBufferSize")
                .setDefault(64)
                .help("read buffer size");

        parser.addArgument("--producer-props")
                .nargs("+")
                .required(true)
                .metavar("PROP-NAME=PROP-VALUE")
                .type(String.class)
                .dest("producerConfig")
                .help("kafka producer related configuaration properties like bootstrap.servers,client.id etc..");

        return parser;
    }

    private static class Stats {
        private long start;
        private long windowStart;
        private int[] latencies;
        private int sampling;
        private int iteration;
        private int index;
        private long count;
        private long expired, windowExpired;
        private long bytes;
        private int maxLatency;
        private long totalLatency;
        private long windowCount;
        private int windowMaxLatency;
        private long windowTotalLatency;
        private long windowBytes;
        private long reportingInterval;
        private long batchSize;


        public Stats(int sampling, int reportingInterval, long batchSize) {
            this.start = System.currentTimeMillis();
            this.windowStart = System.currentTimeMillis();
            this.index = 0;
            this.iteration = 0;
            this.sampling = sampling;
            this.latencies = new int[100];
            this.index = 0;
            this.maxLatency = 0;
            this.totalLatency = 0;
            this.windowCount = 0;
            this.windowMaxLatency = 0;
            this.windowTotalLatency = 0;
            this.windowBytes = 0;
            this.totalLatency = 0;
            this.reportingInterval = reportingInterval;
            this.batchSize = batchSize;
        }

        public void record(int iter, int latency, int bytes, long time) {
            this.count++;
            this.bytes += bytes;
            this.totalLatency += latency;
            this.maxLatency = Math.max(this.maxLatency, latency);
            this.windowCount++;
            this.windowBytes += bytes;
            this.windowTotalLatency += latency;
            this.windowMaxLatency = Math.max(windowMaxLatency, latency);
            if (iter % this.sampling == 0) {
                this.latencies[index] = latency;
                this.index++;
            }
            /* maybe report the recent perf */
            if (time - windowStart >= reportingInterval) {
                printWindow();
                newWindow();
            }
        }

        public void recordExpired() {
            windowExpired++;
            expired++;
        }

        public Callback nextCompletion(long start, int bytes, Stats stats) {
            Callback cb = new PerfCallback(this.iteration, start, bytes, stats);
            this.iteration++;
            return cb;
        }

        public void printWindow() {
            long ellapsed = System.currentTimeMillis() - windowStart;
            double recsPerSec = 1000.0 * windowCount / (double) ellapsed;
            double mbPerSec = 1000.0 * this.windowBytes / (double) ellapsed / (1024.0 * 1024.0);
            System.out.printf("%d records sent, %.1f records/sec (%.2f MB/sec), %.1f ms avg latency, %.1f max latency. %d errors %.2f %% errors\n",
                    windowCount,
                    recsPerSec,
                    mbPerSec,
                    windowTotalLatency / (double) windowCount,
                    (double) windowMaxLatency,
                    windowExpired,
                    (((windowExpired)/ (double) windowCount) * 100));
        }

        public void newWindow() {
            this.windowStart = System.currentTimeMillis();
            this.windowCount = 0;
            this.windowMaxLatency = 0;
            this.windowTotalLatency = 0;
            this.windowBytes = 0;
            this.windowExpired = 0;
        }

        public void printTotal() {
            long elapsed = System.currentTimeMillis() - start;
            double recsPerSec = 1000.0 * count / (double) elapsed;
            double mbPerSec = 1000.0 * this.bytes / (double) elapsed / (1024.0 * 1024.0);
            int[] percs = percentiles(this.latencies, index, 0.5, 0.95, 0.99, 0.999);
            System.out.printf("%d records sent, %f records/sec (%.2f MB/sec), %.2f ms avg latency, %.2f ms max latency, %d ms 50th, %d ms 95th, %d ms 99th, %d ms 99.9th. %d total-errors(%.2f %% loss)\n",
                    count,
                    recsPerSec,
                    mbPerSec,
                    totalLatency / (double) count,
                    (double) maxLatency,
                    percs[0],
                    percs[1],
                    percs[2],
                    percs[3],
                    expired,
                    (((expired)/ (double) count) * 100));
        }

        private static int[] percentiles(int[] latencies, int count, double... percentiles) {
            int size = Math.min(count, latencies.length);
            Arrays.sort(latencies, 0, size);
            int[] values = new int[percentiles.length];
            for (int i = 0; i < percentiles.length; i++) {
                int index = (int) (percentiles[i] * size);
                values[i] = latencies[index];
            }
            return values;
        }
    }

    private static final class PerfCallback implements Callback {
        private final long start;
        private final int iteration;
        private final int bytes;
        private final Stats stats;

        public PerfCallback(int iter, long start, int bytes, Stats stats) {
            this.start = start;
            this.stats = stats;
            this.iteration = iter;
            this.bytes = bytes;
        }

        public void onCompletion(RecordMetadata metadata, Exception exception) {
            long now = System.currentTimeMillis();
            int latency = (int) (now - start);
            this.stats.record(iteration, latency, bytes, now);

            if (exception != null) {
                if (exception instanceof TimeoutException) {
                    this.stats.recordExpired();
                } else {
                    exception.printStackTrace();
                }
            }
        }
    }


}
