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
        public FiverFile(File file, long offset, long length, long id) {
            this.file = file;
            this.offset = offset;
            this.endoffset = offset+length;
            this.length = length;

            this.isEvicted=false;
            this.id=id;
        }
        public FiverFile(FiverFile f) {
            this.file = f.file;
            this.offset = f.offset;
            this.endoffset = f.endoffset;
            this.length=f.length;

            this.isEvicted=f.isEvicted;
            this.id=f.id;
        }
        File file;
        Long offset;
        Long endoffset;
        Long length;
        boolean isEvicted = false;
        long id;
    }
}
