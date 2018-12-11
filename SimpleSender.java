import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.Socket;
import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class SimpleSender {

    private Socket s;

    static boolean allFileTransfersCompleted = false;
    static String fileOrdering = "sort";

    static long totalTransferredBytes = 0;

    public SimpleSender(String host, int port, String file) {
        try {
            s = new Socket(host, port);
            s.setSoTimeout(1000000);
            //s.setSendBufferSize(134217728);
            sendFile(file);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void sendFile(String filePath) throws IOException {

        new MonitorThread().start();

        long start = System.currentTimeMillis();
        DataOutputStream dos = new DataOutputStream(s.getOutputStream());
        File file =new File(filePath);
        File[] files;
        if(file.isDirectory()) {
            files = file.listFiles();
        } else {
            files = new File[] {file};
        }

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

        System.out.println("Will transfer " + files.length+ " files");
        dos.writeInt(files.length);
        byte[] buffer = new byte[128 * 1024];
        int n;
        for (int i = 0; i <files.length ; i++) {
            File currentFile =files[i];
            dos.writeUTF(currentFile.getName());
            dos.writeLong(currentFile.length());
            //System.out.println("Starting to transfer  " + currentFile.getName() + "\t" +
            //        humanReadableByteCount(currentFile.length(), false));
            FileInputStream fis = new FileInputStream(currentFile);
            //long init = System.currentTimeMillis();
            long remaining = currentFile.length();

            while (remaining > 0)
            {
                int howmuchtoRead=(int) Math.min(buffer.length, remaining);

                n=0;
                while(n!=howmuchtoRead)
                {
                    int r= fis.read(buffer, n,howmuchtoRead-n);
                    n=n+r;
                }
                //n = fis.read(buffer, 0, (int) Math.min((long)buffer.length, remaining));

                remaining -= n;
                totalTransferredBytes += n;

                //items.offer(new Item(currentFileName, n));
                dos.write(buffer, 0, n);
            }
            //System.out.println("Finished to transfer  " + currentFile.getName() + "\t" +
            //       (System.currentTimeMillis() - init)/1000.0 + " s");
            fis.close();
        }
        allFileTransfersCompleted = true;
        dos.close();
        System.out.println("Time " + (System.currentTimeMillis() - start) + " ms");
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
        String fileName = args[1];
        if (args.length > 2) {
            fileOrdering = args[2];
        }
        SimpleSender fc = new SimpleSender(destIp, 2008, fileName);
    }

    public class MonitorThread extends Thread {
        long lastTransferredBytes = 0;
        @Override
        public void run() {
            try {
                while (!allFileTransfersCompleted) {
                    double transferThrInMbps = 8 * (totalTransferredBytes-lastTransferredBytes)/(1000*1000);
                    System.out.println("Network thr:" + transferThrInMbps + "Mb/s");
                    lastTransferredBytes = totalTransferredBytes;
                    Thread.sleep(1000);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }

}