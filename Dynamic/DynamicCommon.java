import java.io.File;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;

public class DynamicCommon {



    public static void sleeper(int howmuch)
    {
        try {
            Thread.sleep(howmuch);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static class FiverFile {
        public FiverFile(File file, long offset, long endoffset, byte[] buffer, long id) {
            this.file = file;
            this.offset = offset;
            this.endoffset = endoffset;
            this.length = endoffset-offset;
            this.buffer=buffer;
            this.isEvicted=false;
            this.id=id;
        }
        public FiverFile(FiverFile f) {
            this.file = f.file;
            this.offset = f.offset;
            this.endoffset = f.endoffset;
            this.length=f.length;
            this.buffer =  f.buffer;
            this.isEvicted=f.isEvicted;
            this.id=f.id;
        }
        File file;
        Long offset;
        Long endoffset;
        Long length;
        byte[] buffer;
        boolean isEvicted = false;
        long id;
    }

    /*
    public static class Item {
        byte[] buffer;
        int length;

        public Item(byte[] buffer, int length){
            this.buffer =  Arrays.copyOf(buffer, length);
            this.length = length;
        }
    }
    */


/*
    public static class server implements Runnable
    {
        Thread t;

        public server() {
            t = new Thread(this);
            t.start();
        }
        @Override
        public void run() {
            try {

                ServerSocket ss;
                ss = new ServerSocket(2008);

                while(true) {

                    Socket clientSock = ss.accept();
                    System.out.println("Connection established from  " + clientSock.getInetAddress());
                }

            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("cant create server");
            }
            System.out.println("server ends");
        }
    }
*/

}
