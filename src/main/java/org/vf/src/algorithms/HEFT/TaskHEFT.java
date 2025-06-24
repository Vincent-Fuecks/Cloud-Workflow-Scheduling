package org.vf.src.algorithms.HEFT;

import org.vf.src.HardwareType;
import org.vf.src.Task;

import java.util.List;

public class TaskHEFT extends Task {
    private double upperRank;

    public TaskHEFT(int id, double din, double dout, double mi, List<Integer> parents, List<Integer> children, HardwareType typ) {
        super(id, din, dout, mi, parents, children, typ);
        this.upperRank = -1f;
    }

    public double getUpperRank() {
        return upperRank;
    }

    public void setUpperRank(double upperRank) {
        this.upperRank = upperRank;
    }
}
