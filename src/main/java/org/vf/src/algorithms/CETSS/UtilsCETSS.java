package org.vf.src.algorithms.CETSS;

import org.vf.src.HardwareType;
import org.vf.src.Task;
import org.vf.src.VM;

import java.util.*;
import java.util.function.ToDoubleFunction;

public class UtilsCETSS {

    private static double[] te;
    private static double[] tl;

    public static class TaskVmPair {

        public final TaskCETSS task;
        public final VM vm;
        public final double financialCost;
        public final double est;
        public final double eft;
        public final double et;

        public TaskVmPair(TaskCETSS task, VM vm, double financialCost, double est, double eft, double et) {
            this.task = task;
            this.vm   = vm;
            this.financialCost = financialCost;
            this.est = est;
            this.eft = eft;
            this.et = et;
        }

        public TaskCETSS getTask() {return task;}
        public VM getVm() {return vm;}
        public double getFinancialCost() { return financialCost; }
        public double getEft() { return eft; }
        public double getEst() { return est; }
        public double getEt() {
            return et;
        }
    }

    public static class LevelGroup {
        public final int level;
        public double subdeadline;
        public TaskCETSS criticalTask;
        public double eft;
        public ArrayList<TaskCETSS> levelGroupTasks;

        public LevelGroup(int level, double subdeadline, TaskCETSS criticalTask, double eft) {
            this.level = level;
            this.subdeadline = subdeadline;
            this.criticalTask = criticalTask;
            this.levelGroupTasks = new ArrayList<>();
            if (criticalTask != null) {
                this.levelGroupTasks.add(criticalTask);
            }
            this.eft = eft;
        }

        public void addLevelGroupTask (TaskCETSS task) {
            if (!this.levelGroupTasks.contains(task)) {
                this.levelGroupTasks.add(task);
            }
        }

        public double getSubdeadline() { return subdeadline; }
        public TaskCETSS getCriticalTask() { return criticalTask; }
        public double getEft(){ return eft; }
        public ArrayList<TaskCETSS> getLevelGroupTasks() { return levelGroupTasks; }
        public void setSubdeadline(double subdeadline) { this.subdeadline = subdeadline; }
        public void setEft(double eft) { this.eft = eft; }
        public void setCriticalTask(TaskCETSS task) {
            this.criticalTask = task;
        }
    }

    public static double computeStandardMetric(double phi, ArrayList<VM> vms, ToDoubleFunction<VM> metricExtractor) {
        if (vms.isEmpty()) return 0.0;
        double minVal = Double.POSITIVE_INFINITY;
        double maxVal = Double.NEGATIVE_INFINITY;
        for (VM vm : vms) {
            double val = metricExtractor.applyAsDouble(vm);
            if (val < minVal) minVal = val;
            if (val > maxVal) maxVal = val;
        }
        return phi * minVal + (1.0 - phi) * maxVal;
    }

    /**
     * Calculates the financial cost for scheduling a single task on a given VM.
     *
     * Three scenarios:
     * 1. The task is scheduled in new, un-rented periods.
     * 2. The task utilizes one or more periods that are already rented, thus reducing its own cost.
     * 3. The task's start time alignment requires renting an additional period at the beginning that would otherwise be unused.
     */
    public static double calculateFinancialCost(VM vm, double taskStartTime, double taskFinishTime, int tau) {
        Set<Integer> rentedPeriods = new HashSet<>();
        List<VM.TimeSlot> schedule = vm.getSchedule();

        // Identify all billing periods that are already rented by existing tasks on the VM.
        for (VM.TimeSlot existingSlot : schedule) {
            int startPeriod = (int) Math.floor(existingSlot.start / tau);
            // The ceiling of the end time gives the end of the last billing period.
            int endPeriodIndex = (int) Math.ceil(existingSlot.end / tau);

            for (int i = startPeriod; i < endPeriodIndex; i++) {
                rentedPeriods.add(i);
            }
        }
        // Determine the billing periods required by the new task.
        int newStartPeriod = (int) Math.floor(taskStartTime / tau);
        int newEndPeriodIndex = (int) Math.ceil(taskFinishTime / tau);

        // Calculate the number of *new* periods that must be rented.
        int additionalPeriodsToRent = 0;
        for (int i = newStartPeriod; i < newEndPeriodIndex; i++) {
            if (!rentedPeriods.contains(i)) {
                additionalPeriodsToRent++;
            }
        }

        return additionalPeriodsToRent * vm.getC();
    }

    public static double getSubdeadline(double te_i, double te_exit, double d){
        if (te_exit == 0) return 0;
        return (te_i/te_exit) * d;
    }

    public static double getPHI(ArrayList<VM> vms, double[] vmResourceWeights) {
        int m = vms.size();
        if (m == 0) return 0.0;

        double sumW = 0, sumGsr = 0, sumGsw = 0, sumC = 0;
        double maxW = Double.MIN_VALUE, maxGsr = Double.MIN_VALUE, maxGsw = Double.MIN_VALUE, maxC = Double.MIN_VALUE;
        for (VM vm : vms) {
            double w = vm.getW();
            double gsr = vm.getGsr();
            double gsw = vm.getGsw();
            double c   = vm.getC();

            sumW  += w;
            sumGsr += gsr;
            sumGsw += gsw;
            sumC   += c;

            maxW  = Math.max(maxW, w);
            maxGsr = Math.max(maxGsr, gsr);
            maxGsw = Math.max(maxGsw, gsw);
            maxC   = Math.max(maxC, c);
        }

        double avgW  = sumW  / m;
        double avgGsr = sumGsr / m;
        double avgGsw = sumGsw / m;
        double avgC   = sumC   / m;

        double sqW = 0, sqGsr = 0, sqGsw = 0, sqC = 0;
        for (VM vm : vms) {
            double dw  = vm.getW()  - avgW;
            double dgs = vm.getGsr() - avgGsr;
            double dgw = vm.getGsw() - avgGsw;
            double dc  = vm.getC()   - avgC;
            sqW  += dw * dw;
            sqGsr += dgs * dgs;
            sqGsw += dgw * dgw;
            sqC   += dc * dc;
        }

        double termW  = maxW  > 0 ? Math.sqrt(sqW)  / (m * maxW)  : 0;
        double termGsr = maxGsr > 0 ? Math.sqrt(sqGsr) / (m * maxGsr) : 0;
        double termGsw = maxGsw > 0 ? Math.sqrt(sqGsw) / (m * maxGsw) : 0;
        double termC   = maxC   > 0 ? Math.sqrt(sqC)   / (m * maxC)   : 0;

        return vmResourceWeights[0]   * termW
                + vmResourceWeights[1]  * termGsr
                + vmResourceWeights[2]    * termGsw
                + vmResourceWeights[3]    * termC;
    }

    public static double[] getEarliestFinishTimes(List<TaskCETSS> tasks, HashMap<HardwareType, VM> vmstMap) {
        if (tasks == null || tasks.isEmpty()) {
            return new double[0];
        }
        te = new double[tasks.size()];
        Arrays.fill(te, -1.0);

        for (TaskCETSS task : tasks) {
            computeEFT(tasks, task, vmstMap);
        }
        return te;
    }

    /**
     * Computes the Earliest Finish Time (EFT) for a given task.
     */
    private static double computeEFT(List<TaskCETSS> tasks, TaskCETSS current, HashMap<HardwareType, VM> vmstMap) {
        int id = current.getId();
        if (te[id] >= 0) {
            return te[id];
        }

        double maxParentEFT = 0.0;
        // The start time of the current task is determined by the latest finish time among all its parents.
        if (!current.getParents().isEmpty()) {
            for (Integer parentId : current.getParents()) {
                TaskCETSS parent = tasks.get(parentId);
                maxParentEFT = Math.max(maxParentEFT, computeEFT(tasks, parent, vmstMap));
            }
        }

        // EFT is the finish time of the last parent + own execution time.
        double eft = maxParentEFT + getExecutionTime(current, vmstMap.get(current.getTyp()));
        te[id] = eft;
        return eft;
    }

    /**
     *  Compute Latest Finish Times (LFT) for all tasks.
     */
    public static double[] getLatestFinishTimes(List<TaskCETSS> tasks, HashMap<HardwareType, VM> vmst) {
        if (tasks == null || tasks.isEmpty()) {
            return new double[0];
        }
        int n = tasks.size();
        tl = new double[n];
        Arrays.fill(tl, -1.0);

        double makespan = 0.0;
        if (te == null || te.length != n) {
            getEarliestFinishTimes(tasks, vmst);
        }
        for (TaskCETSS task : tasks) {
            if (task.getChildren().isEmpty()) {
                makespan = Math.max(makespan, te[task.getId()]);
            }
        }

        for (TaskCETSS task : tasks) {
            computeLFT(tasks, task, vmst, makespan);
        }

        return tl;
    }

    /**
     * Computes the Latest Finish Time (LFT) for a given task.
     */
    private static double computeLFT(List<TaskCETSS> tasks, TaskCETSS current, HashMap<HardwareType, VM> vmstMap, double makespan) {
        int id = current.getId();
        if (tl[id] >= 0) {
            return tl[id];
        }

        double lft;
        if (current.getChildren().isEmpty()) {
            lft = makespan;
        } else {
            double minChildLftMinusExec = Double.POSITIVE_INFINITY;
            for (Integer childId : current.getChildren()) {
                TaskCETSS child = tasks.get(childId);
                double childLFT = computeLFT(tasks, child, vmstMap, makespan);
                minChildLftMinusExec = Math.min(minChildLftMinusExec, childLFT - getExecutionTime(child, vmstMap.get(child.getTyp())));
            }
            lft = minChildLftMinusExec;
        }

        tl[id] = lft;
        return lft;
    }

    /**
     * Calculates the total execution time of a task on a specific VM.
     */
    public static double getExecutionTime(Task task, VM vm) {
        if (vm == null) return Double.POSITIVE_INFINITY;
        return (task.getDin() / vm.getGsr()) +
                (task.getMi() / vm.getW()) +
                (task.getDout() / vm.getGsw());
    }
}