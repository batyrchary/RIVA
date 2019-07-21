import java.io.FileInputStream;
import java.io.IOException;
import java.net.Socket;
import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.*;
import javax.xml.bind.annotation.adapters.HexBinaryAdapter;


public class FiverSender {
    private Socket s;

    static boolean allFileTransfersCompleted = false;
    static String destIp;

    public static volatile List<FiverFile> files;
    public static volatile LinkedBlockingQueue<FiverFile> checksumFiles;

    static long totalTransferredBytes = 0;
    static long totalChecksumBytes = 0;
    long INTEGRITY_VERIFICATION_BLOCK_SIZE = 256 * 1024 * 1024;
    long startTime;
    boolean debug = true;

    static String fileOrdering = "shuffle";

    public static volatile boolean checksum_done=false;

    static LinkedBlockingQueue<Item> items = new LinkedBlockingQueue<>(10000);

    public class FiverFile {
        public FiverFile(File file, long offset, long length) {
            this.file = file;
            this.offset = offset;
            this.length = length;
        }
        File file;
        Long offset;
        Long length;
    }

    class Item {
        byte[] buffer;
        int length;

        public Item(byte[] buffer, int length){
            this.buffer =  Arrays.copyOf(buffer, length);
            this.length = length;
        }
    }

    public FiverSender(String host, int port) {
        try {
            s = new Socket(host, port);
            s.setSoTimeout(10000);
        } catch (Exception e) {
            System.out.println(e);
        }
    }





    public void sendFile(String path) throws IOException {
        //new MonitorThread().start();
        startTime = System.currentTimeMillis();
        DataOutputStream dos = new DataOutputStream(s.getOutputStream());

        File file =new File(path);
        files = new LinkedList<>();
        checksumFiles = new LinkedBlockingQueue<>();
        if(file.isDirectory()) {
            for (File f : file.listFiles()) {
                files.add(new FiverFile(f, 0, f.length()));
            }
        } else {
            files.add(new FiverFile(file, 0, file.length()));
        }
        System.out.println("Will transfer " + files.size()+ " files");

        if (fileOrdering.compareTo("shuffle") == 0){
            Collections.shuffle(files);
        } else if (fileOrdering.compareTo("sort") == 0) {
            Collections.sort(files, new Comparator<FiverFile>() {
                public int compare(FiverFile f1, FiverFile f2) {
                    try {
                        int i1 = Integer.parseInt(f1.file.getName());
                        int i2 = Integer.parseInt(f2.file.getName());
                        return i1 - i2;
                    } catch(NumberFormatException e) {
                        throw new AssertionError(e);
                    }
                }
            });
        } else {
            System.out.println("Undefined file ordering:" + fileOrdering);
            System.exit(-1);
        }

        ChecksumThread worker = new ChecksumThread();
        Thread thread = new Thread(worker);
        thread.start();

        dos.writeLong(INTEGRITY_VERIFICATION_BLOCK_SIZE);

        byte[] buffer = new byte[128 * 1024];
        int n;
        while (!checksum_done) {
            FiverFile currentFile = null;
            synchronized (files) {
                if (!files.isEmpty()) {
                    currentFile = files.remove(0);
                }
            }
            if (currentFile == null) {
                
                continue;
            }

            checksumFiles.offer(currentFile);
            //send file metadata
            dos.writeUTF(currentFile.file.getName());
            dos.writeLong(currentFile.offset);
            dos.writeLong(currentFile.length);
            if (debug) {
                System.out.println("Transfer START file " + currentFile.file.getName() + "offset" + currentFile.offset +
                        " size:" + humanReadableByteCount(currentFile.file.length(), false) + " time:" +
                        (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
            }
            long fileTransferStartTime = System.currentTimeMillis();

            FileInputStream fis = new FileInputStream(currentFile.file);
            if (currentFile.offset > 0) {
                fis.getChannel().position(currentFile.offset);
            }
            Long remaining = currentFile.length;
            try {
                while ((n = fis.read(buffer, 0, (int) Math.min((long)buffer.length, remaining))) > 0) {
                    remaining -= n;
                    totalTransferredBytes += n;
                    items.offer(new Item(buffer, n), Long.MAX_VALUE, TimeUnit.MILLISECONDS);
                    dos.write(buffer, 0, n);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (debug) {
                System.out.println("Transfer END file " + currentFile.file.getName() + "\t duration:" +
                        (System.currentTimeMillis() - fileTransferStartTime) / 1000.0 + " time:" +
                        (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
            }
            System.out.println("Total transferred bytes:" + totalTransferredBytes);
            fis.close();
        }
    }


    public static String humanReadableByteCount(long bytes, boolean si) {
        int unit = si ? 1000 : 1024;
        if (bytes < unit) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp-1) + (si ? "" : "i");
        return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }

    public static void main(String[] args) {
        destIp = args[0];
        String path = args[1];
        if (args.length > 2) {
            fileOrdering = args[2];
        }
        FiverSender fc = new FiverSender(destIp, 2008);
        try {
            fc.sendFile(path);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }




    public class ChecksumThread implements Runnable {
        MessageDigest md = null;


        public void reset () {
            md.reset();
        }

        @Override
        public void run() {
            System.out.println("MyThread - START " + Thread.currentThread().getName());
            DataInputStream dataInputStream = null;
            while (true) {
                try {
                    s = new Socket(destIp, 20180);
                    dataInputStream = new DataInputStream(s.getInputStream());
                    break;
                } catch (IOException e) {
                    System.out.println("Trying to connect to checksum thread");
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    }
                }
            }

            try {
                md = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            md.reset();
            int totalIntegrityFailures = 0;
            try {
                while (!files.isEmpty() || !checksumFiles.isEmpty()) {
System.out.println(files.size() +"\t"+checksumFiles.size());

                    //FiverFile currentFile = checksumFiles.poll(1000, TimeUnit.SECONDS);
                    FiverFile currentFile=null; 
                    currentFile = checksumFiles.poll();

                    if(currentFile==null)
                    {
                        try{Thread.sleep(500);}catch(Exception e){}
                        continue;
                    }
                    long fileSize = currentFile.length;
                    long remaining = fileSize;
                    if (debug) {
                        System.out.println("Checksum START file:" + currentFile.file.getName() +  " size:" +
                                humanReadableByteCount(fileSize, false) + "  time:" +
                                (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
                    }
                    long init = System.currentTimeMillis();
                    long processedBytes = 0;
                    Item item;
                    //INTEGRITY_VERIFICATION_BLOCK_SIZE = fileSize;
                    while (remaining > 0) {
                        long currentBlockSize = Math.min(INTEGRITY_VERIFICATION_BLOCK_SIZE, remaining + processedBytes);
                        System.out.println("Block size:" + currentBlockSize + " -- " + processedBytes);
                        byte[] leftOverBytes = null;
                        while (processedBytes < currentBlockSize) {
                            item = items.poll(100, TimeUnit.MILLISECONDS);
                            if (item == null) {
                                try{Thread.sleep(200);}catch(Exception e){}

                                continue;
                            }
                            int usedBytes = item.length;
                            if (item.length + processedBytes > currentBlockSize) {
                                usedBytes = (int)(currentBlockSize - processedBytes);
                                md.update(item.buffer, 0, usedBytes);
                                leftOverBytes = Arrays.copyOfRange(item.buffer, usedBytes, item.length);
                            } else {
                                md.update(item.buffer, 0, item.length);
                            }
                            remaining -= usedBytes;
                            totalChecksumBytes += usedBytes;
                            processedBytes += usedBytes;
                        }
                        long offset = currentFile.length - remaining - processedBytes;
                        byte[] digest = md.digest();
                        String hex = (new HexBinaryAdapter()).marshal(digest);
                        String destinationHex = dataInputStream.readUTF();
                        if (hex.compareTo(destinationHex) != 0 ) { // Checksums dont match
                            totalIntegrityFailures++;
                            System.out.print("Checksum of file " + currentFile.file.getName()  + " offset:" + offset +
                                            " length:" + processedBytes);
                            System.out.print(" Source-hex:"+hex + " destination hex:" + destinationHex);
                            System.out.println("----FAILURE total failures:" + totalIntegrityFailures);
                            //
                            synchronized (files) {
                                files.add(new FiverFile(currentFile.file, offset, processedBytes));
                                System.out.println("File added " + files.size() + " files left");
                            }
                        }
                        md.reset();
                        processedBytes = 0L;
                        if (leftOverBytes != null) {
                            md.update(leftOverBytes);
                            remaining -= leftOverBytes.length;
                            totalChecksumBytes += leftOverBytes.length;
                            processedBytes += leftOverBytes.length;
                        }
                    }
                    double duration = (System.currentTimeMillis() - init) / 1000.0;
                    /*
                    if (debug) {
                        System.out.println("Checksum  END " + hex + " file:" + currentFile.file.getName() +
                                "  duration:" + duration + " time :" +
                                (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
                    }
                    */
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }

            checksum_done=true;
            System.out.println("MyThread - END " + Thread.currentThread().getName());
            double totalDuration = (System.currentTimeMillis() - startTime) / 1000.0;
            System.out.println("Total duration: " + totalDuration);

        }

    }


    public class MonitorThread extends Thread {
        long lastTransferredBytes = 0;
        long lastChecksumBytes = 0;

        @Override
        public void run() {
            try {
                while (!checksum_done) {
                    double transferThrInMbps = 8 * (totalTransferredBytes-lastTransferredBytes)/(1000*1000);
                    double checksumThrInMbps = 8 * (totalChecksumBytes-lastChecksumBytes)/(1024*1024);
                    System.out.println("Network thr:" + transferThrInMbps + "Mb/s I/O thr:" + checksumThrInMbps + " Mb/s" +
                            " items:" + items.size());
                    lastTransferredBytes = totalTransferredBytes;
                    lastChecksumBytes = totalChecksumBytes;
                    Thread.sleep(1000);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }
}