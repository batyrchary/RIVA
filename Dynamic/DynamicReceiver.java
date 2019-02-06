import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

public class DynamicReceiver {


    static String baseDir = "/Users/batyrchary/Desktop/journalFIVER/receiver/";
    static AtomicBoolean allTransfersCompleted = new AtomicBoolean(false);






    public static void main (String[] args)
    {
        if (args.length > 0)
        {
            baseDir = args[0];
        }
        FiverReceiverU fs = new FiverReceiverU(2008);
        fs.start();
    }


/*
    public static class individualFitnessRunnable implements Runnable {
        Chromosome individual;
        String name;
        Thread t;
        public individualFitnessRunnable(Chromosome individual, String nameOfThread)
        {
            this.individual = individual;
            this.name=nameOfThread;

            t=new Thread(this, name);
            t.start();
        }
        @Override
        public void run() {
            individual.hfitnes=individualFitness(individual);
        }
    }



    public class MonitorThread extends Thread
    {
        long lastReceivedBytes = 0;
        long lastReadBytes = 0;
        String currentFile;
        public void setCurrentFile(String currentFile) {
            this.currentFile = currentFile;
        }


        ArrayList<individualFitnessRunnable> threads=new ArrayList<>();

        individualFitnessRunnable fit1 = new individualFitnessRunnable(kid1, "kid1");
        //new Thread(fit1).start();

        individualFitnessRunnable fit2 = new individualFitnessRunnable(kid2, "kid2");
        //new Thread(fit2).start();

            threads.add(fit1);
            threads.add(fit2);

        for(int i=0; i<threads.size(); i++)
        {
            try {
                threads.get(i).t.join();
                newGeneration.add(threads.get(i).individual);
            }catch (Exception e) { }
        }


        @Override
        public void run() {
            try
            {

                while (!allTransfersCompleted.get())
                {



*/

                /*
                    double thrInMbps = 8 * (totalTransferredBytes - lastReceivedBytes) / (1000*1000);
                    double readThrInMbps = 8 * (totalChecksumBytes - lastReadBytes) / (1000*1000);
                    System.out.println("Transfer Thr:" + thrInMbps + " Mb/s Checksum thr:" + readThrInMbps + "items:" + items.size());
                    lastReceivedBytes = totalTransferredBytes;
                    lastReadBytes = totalChecksumBytes;
                    Thread.sleep(1000);
                */

/*                }



            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
    }*/

}








