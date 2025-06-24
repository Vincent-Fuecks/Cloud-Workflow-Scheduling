package org.vf.src.algorithms.CETSS;

import org.vf.src.HardwareType;
import org.vf.src.Task;

import java.util.List;

public class TaskCETSS extends Task {
    private int groupLevel;
    private double earliestStartTime;

    public TaskCETSS(int id, double din, double dout, double mi, List<Integer> parents, List<Integer> children, HardwareType typ) {
        super(id, din, dout, mi, parents, children, typ);
        this.groupLevel = -1;
        this.earliestStartTime = 0.0;
    }

    public void reset() {
        this.resetInDegree();
        this.setStatus(Status.UNSCHEDULED);
        this.earliestStartTime = 0.0;
    }

    public double getEarliestStartTime(){
        return earliestStartTime;
    }

    public void setEarliestStartTime(double earliestStartTime) {
        this.earliestStartTime = earliestStartTime;
    }

    public int getGroupLevel() { return groupLevel;}

    public void setGroupLevel(int groupLevel) {
        this.groupLevel = groupLevel;
    }
}