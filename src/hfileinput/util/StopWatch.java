package hfileinput.util;

public class StopWatch {

    private String name;

    private long elapseNano = 0L;

    private long startNano = -1L;

    private int count = 0;


    public StopWatch(String name) {
        this.name = name;
    }

    public void start() {
        if (startNano < 0L) {
            startNano = System.nanoTime();
            ++count;
        } else {
            throw new IllegalStateException("Tryed to start when not stopped.");
        }
    }

    public void stop() {
        if (startNano >= 0L) {
            long endNano = System.nanoTime();
            elapseNano += (endNano - startNano);
            startNano = -1L;
        } else {
            throw new IllegalStateException("Tryed to stop when not started.");
        }
    }

    public String getName() {
        return name;
    }

    public long getElapseNano() {
        return elapseNano;
    }

    public int getCount() {
        return count;
    }

    @Override
    public String toString() {
        return String.format("%s - called %,d times, total %,.2f ms.",
                name, count, (elapseNano / 1000.0d / 1000.0d));
    }

}
