import javax.xml.bind.annotation.adapters.HexBinaryAdapter;
import java.io.*;
import java.net.Socket;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.Semaphore;

public class BlockLevelPipeliningSender {
    private Socket s;

    static boolean allFileTransfersCompleted = false;

    static Semaphore checksumLock = new Semaphore(0);
    static Semaphore transferLock = new Semaphore(0);

    static long totalTransferredBytes = 0;
    static long totalChecksumBytes = 0;

    static String fileOrdering = "shuffle";

    boolean debug = false;

    List<Block> blocks;

    public class Block {
        String path;
        String fileName;
        long length;
        long offset;
        boolean insertFault = false;
        public Block (String path, String fileName, long offset, long length) {
            this.path = path;
            this.fileName = fileName;
            this.length = length;
            this.offset = offset;
        }
    }

    public BlockLevelPipeliningSender(String host, int port) {
        try {
            s = new Socket(host, port);
        //    s.setSoTimeout(10000);
            //s.setSendBufferSize(33554432);
        } catch (Exception e) {
            System.out.println(e);
        }
    }


    public void sendFile(String path, long blockSize) throws IOException {
        //new MonitorThread().start();
        checksumLock.release(2);
        long startTime = System.currentTimeMillis();
        DataOutputStream dos = new DataOutputStream(s.getOutputStream());

        File file =new File(path);
        File[] files;
        if(file.isDirectory()) {
            files = file.listFiles();
        } else {
            files = new File[] {file};
        }
        System.out.println("Using " + fileOrdering + "to arrange files");
        if (fileOrdering.compareTo("shuffle") == 0){
            List<File> fileList = Arrays.asList(files);
            Collections.shuffle(fileList);
            files = fileList.toArray(new File[fileList.size()]);
        } else if (fileOrdering.compareTo("sort") == 0) {
            Arrays.sort(files, new Comparator<File>() {
                public int compare(File f1, File f2) {
                    try {
                        int i1 = Integer.parseInt(f1.getName());
                        int i2 = Integer.parseInt(f2.getName());
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

        for (int i = 0; i <files.length ; i++) {
            System.out.println("File " + files[i].getName() + " size:" +
                    humanReadableByteCount(files[i].length(), false));
        }

        System.out.println("Will transfer " + files.length+ " files. Block size:" +
                humanReadableByteCount(blockSize, false));

        byte[] buffer = new byte[128 * 1024];
        blocks = new LinkedList<>();
        for (int i = 0; i <files.length ; i++) {
            String filePath = files[i].getAbsolutePath();
            long fileSize = files[i].length();
            long remainingFileSize = fileSize;
            long currentOffset = 0;
            long sizeOfBlock;

            while (remainingFileSize > 0) {
                sizeOfBlock = remainingFileSize < 1.1 * blockSize ? remainingFileSize : blockSize;
                Block block = new Block(filePath, files[i].getName(), currentOffset, sizeOfBlock);
                synchronized (blocks) {
                    blocks.add(block);
                }
                remainingFileSize -= sizeOfBlock;
                currentOffset += sizeOfBlock;
            }

        }
        System.out.println("Total blocks:" + blocks.size());
        ChecksumThread checksumThread = new ChecksumThread();
        Thread thread = new Thread(checksumThread);
        thread.start();
        double totalTransferTime = 0.0;
        FileInputStream fis = null;
        while (true) {
            //send file metadata
            while (blocks.isEmpty()) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            Block currentBlock = blocks.remove(0);

            dos.writeUTF(currentBlock.fileName);
            dos.writeLong(currentBlock.offset);
            dos.writeLong(currentBlock.length);
            try {
                checksumLock.acquire();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            long transferStart = System.currentTimeMillis();
            if (debug) {
                System.out.println("Transfer START file:" + currentBlock.fileName +
                        " offset:" + humanReadableByteCount(currentBlock.offset, false) +
                        " length:" + humanReadableByteCount(currentBlock.length, false) +
                        " time:" + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
            }

            fis = new FileInputStream(currentBlock.path);
            if (currentBlock.offset > 0) {
                fis.getChannel().position(currentBlock.offset);
            }

            long remaining = currentBlock.length;
            int n;
            while ((n = fis.read(buffer, 0, (int) Math.min((long)buffer.length, remaining))) > 0) {
                dos.write(buffer, 0, n);
                remaining -= n;
            }
            totalTransferTime += (System.currentTimeMillis() - transferStart)/1000.0;
            if (debug) {
                System.out.println("Transfer END file:" + currentBlock.fileName + "\t" +
                        " offset:" + humanReadableByteCount(currentBlock.offset, false) +
                        " length:" + humanReadableByteCount(currentBlock.length, false) +
                        " duration:" + (System.currentTimeMillis() - transferStart) / 1000.0 +
                        " seconds transferTime:" + totalTransferTime + "seconds time:" +
                        (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
            }

            fis.close();
            checksumThread.setNextBlock(currentBlock);
            transferLock.release();
        }
        //dos.close();
        //allFileTransfersCompleted = true;
    }


    public static String humanReadableByteCount(long bytes, boolean si) {
        int unit = si ? 1000 : 1024;
        if (bytes < unit) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp-1) + (si ? "" : "i");
        return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }

    public static void main(String[] args) {
        String destIp = args[0];
        String path = args[1];
        long blockSize = 256 * 1024 * 1024; // default block size is 256 MB
        if (args.length > 2) {
            blockSize = Long.parseLong(args[2]);
        }
        if (args.length > 3) {
            fileOrdering = args[3];
        }
        BlockLevelPipeliningSender blockLevelPipeliningSender = new BlockLevelPipeliningSender(destIp, 2038);
        try {
            blockLevelPipeliningSender.sendFile(path, blockSize);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public class ChecksumThread implements Runnable {
        Block block;
        int checksumComputedBlocks = 0;
        MessageDigest md = null;
        DataInputStream dataInputStream;

        public void setNextBlock (Block block) {
            this.block = block;
        }

        public void reset () {
            md.reset();
        }

        @Override
        public void run() {
            System.out.println("MyThread - START " + Thread.currentThread().getName());
            try {
                dataInputStream = new DataInputStream(s.getInputStream());
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                md = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            long startTime = System.currentTimeMillis();
            while (true) {
                try {
                    transferLock.acquire();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if (debug) {
                    System.out.println("Checksum START of block:" + block.fileName +
                            " offset:" + humanReadableByteCount(block.offset, false) +
                            " length:" + humanReadableByteCount(block.length, false) +
                            " time:" + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
                }
                Block currentBlock = block;
                long checksumStart = System.currentTimeMillis();
                byte[] buffer = new byte[128*1024];
                long transferredBytes = 0;
                int n;
                try {
                    FileInputStream fis = new FileInputStream(currentBlock.path);
                    if (block.offset > 0) {
                        fis.getChannel().position(block.offset);
                    }
                    DigestInputStream dis = new DigestInputStream(fis, md);
                    long remaining = currentBlock.length;
                    while ((n = dis.read(buffer, 0, (int)Math.min(buffer.length, remaining))) > 0) {
                        transferredBytes +=n;
                        remaining -= n;
                    }
                    byte[] digest = md.digest();
                    String hex = (new HexBinaryAdapter()).marshal(digest);
                    System.out.print("Checksum of file " + currentBlock.fileName  + " offset:" + currentBlock.offset +
                            " length:" + currentBlock.length + " hex:"+hex);
                    String destHex = dataInputStream.readUTF();
                    if (hex.compareTo(destHex) != 0) {
                        System.out.println("---FAILURE");
                        synchronized (blocks) {
                            blocks.add(currentBlock);
                        }
                    } else {
                        System.out.println("---success");
                    }
                    md.reset();

                    if (debug) {
                        System.out.println("Checksum END File" + currentBlock.fileName +
                                " offset:" + humanReadableByteCount(currentBlock.offset, false) +
                                " length:" + humanReadableByteCount(currentBlock.length, false) +
                                " Checksum  " + hex + " time " +
                                " duration:" + (System.currentTimeMillis() - checksumStart) / 1000.0 + " seconds" +
                                " time:" + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
                    }

                    dis.close();
                    fis.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                checksumComputedBlocks ++;
		System.out.println("checksumComputedBlocks="+checksumComputedBlocks);
                checksumLock.release();

            }
            //System.out.println("MyThread - END " + Thread.currentThread().getName());
            //double totalDuration = (System.currentTimeMillis() - startTime) / 1000.0;
            //System.out.println("Total duration: " + totalDuration);

        }

    }


    public class MonitorThread extends Thread {
        long lastTransferredBytes = 0;
        long lastChecksumBytes = 0;

        @Override
        public void run() {
            try {
                while (!allFileTransfersCompleted) {
                    double transferThrInMbps = 8 * (totalTransferredBytes-lastTransferredBytes)/(1000*1000);
                    double checksumThrInMbps = 8 * (totalChecksumBytes-lastChecksumBytes)/(1024*1024);
                    System.out.println("Network thr:" + transferThrInMbps + "Mb/s I/O thr:" + checksumThrInMbps + " Mb/s");
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
