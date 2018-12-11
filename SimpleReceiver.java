import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class SimpleReceiver{

    private ServerSocket ss;
    static String basedir = "/storage/data1/earslan/mixed/";


    public SimpleReceiver(int port) {
        try {
            ss = new ServerSocket( port);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void listen() {
        while (true) {
            try {
                Socket clientSock = ss.accept();
                clientSock.setSoTimeout(100000);
                System.out.println("Connection established from  " + clientSock.getInetAddress());
                saveFile(clientSock);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void saveFile(Socket clientSock) throws IOException {
        long startTime = System.currentTimeMillis();
        DataInputStream dataInputStream  = new DataInputStream(clientSock.getInputStream());
        byte[] buffer = new byte[128*1024];
        int fileCount = dataInputStream.readInt();
        System.out.println("Will receive " + fileCount + " files");
        for (int i = 0; i <fileCount ; i++) {
            String fileName = dataInputStream.readUTF();
            long fileSize = dataInputStream.readLong();

            //System.out.println("Starting to receive  " + fileName + "\t" + humanReadableByteCount(fileSize, false));
            //OutputStream fos = new FileOutputStream(basedir + "/" +fileName);
            RandomAccessFile randomAccessFile = new RandomAccessFile(basedir +"/" + fileName, "rw");
            long remaining = fileSize;
            long init = System.currentTimeMillis();
            while(remaining > 0) {
                int read = dataInputStream.read(buffer, 0, (int)Math.min((long)buffer.length, remaining));
                remaining -= read;
                randomAccessFile.write(buffer, 0, read);
                //fos.write(buffer, 0, read);
            }
            //System.out.println("Finished file: " + fileName + (System.currentTimeMillis() - init) + " ms");
            //fos.close();
            randomAccessFile.close();
        }
        dataInputStream.close();
        System.out.println("Time " + (System.currentTimeMillis() - startTime) + " ms");

    }

    public static String humanReadableByteCount(long bytes, boolean si) {
        int unit = si ? 1000 : 1024;
        if (bytes < unit) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp-1) + (si ? "" : "i");
        return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }

    public static void main (String[] args) {
        if (args.length > 0) {
            basedir = args[0];
        }
        SimpleReceiver fs = new SimpleReceiver(2008);
        fs.listen();
    }
}