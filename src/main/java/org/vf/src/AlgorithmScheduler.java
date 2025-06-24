package org.vf.src;

import java.util.ArrayList;

@FunctionalInterface
public interface AlgorithmScheduler<T extends Task, S extends SchedulingAlgorithm> {
    ArrayList<VM> schedule(ArrayList<? extends Task> tasks, ArrayList<VM> vms, int tau, int deadline);
}