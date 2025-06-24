package org.vf.src;

import java.util.List;

public class Task {

    public enum Status { UNSCHEDULED, SCHEDULED }

    private int id;
    private double din;
    private double dout;
    private double mi;
    private int inDegree;
    private List<Integer> parents;
    private List<Integer> children;
    private Status status = Status.UNSCHEDULED;
    private HardwareType typ;

    protected Task(int id, double din, double dout, double mi, List<Integer> parents, List<Integer> children, HardwareType typ) {
        this.id = id;
        this.din = din;
        this.dout = dout;
        this.mi = mi;
        this.parents = parents;
        this.children = children;
        this.typ = typ;
        this.inDegree = parents.size();
    }

    public int getId() {
        return id;
    }

    public double getDin() {
        return din;
    }

    public double getDout() {
        return dout;
    }

    public double getMi() {
        return mi;
    }

    public List<Integer> getParents() {
        return parents;
    }

    public List<Integer> getChildren() {
        return children;
    }

    public int getInDegree() {return this.inDegree;}

    public Status getStatus() { return status; }

    public HardwareType getTyp() {
        return typ;
    }

    public void setStatus(Status status) { this.status = status; }

    public void reduceInDegree() {
        this.inDegree--;
    }

    public void resetInDegree() {
        this.inDegree = parents.size();
    }
}
