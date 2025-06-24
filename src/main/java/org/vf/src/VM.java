package org.vf.src;


import java.util.*;
import java.util.stream.Collectors;

public class VM {
    private HardwareType typ;
    private final int id;
    private final String name;
    private final double c;  // Billing Cost per period
    private final double w;  // Computation Capacity
    private final double gsr;// Read Storage Speed
    private final double gsw;// Write Storage Speed
    private final List<TimeSlot> schedule;
    private static int nextVmId = 1;

    public VM(int id, double c, double w, double gsr, double gsw) {
        this.id = id;
        this.name = "Custom";
        this.c = c;
        this.w = w;
        this.gsr = gsr;
        this.gsw = gsw;
        this.typ = HardwareType.CPU;
        this.schedule = new ArrayList<>();
    }

    public VM(int id, String name, double c, double w, double gsr, double gsw, HardwareType typ) {
        this.id = id;
        this.name = name;
        this.c = c;
        this.w = w;
        this.gsr = gsr;
        this.gsw = gsw;
        this.schedule = new ArrayList<>();
        this.typ = typ;
    }

    // Constructor for deep copying
    public VM(VM source) {
        this.id = source.id;
        this.name = source.name;
        this.c = source.c;
        this.w = source.w;
        this.gsr = source.gsr;
        this.gsw = source.gsw;
        this.schedule = source.schedule.stream()
                .map(TimeSlot::new)
                .collect(Collectors.toList());
    }

    /**
     * Represents a scheduled task on the VM's timeline.
     */
    public static class TimeSlot {
        public double start;
        public double end;
        public Task task;
        public double deadline;
        public double financialCost;

        public TimeSlot(double start, double end, Task task, double deadline, double financialCost) {
            this.start = start;
            this.end = end;
            this.task = task;
            this.deadline = deadline;
            this.financialCost = financialCost;
        }

        // Constructor for deep copying
        public TimeSlot(TimeSlot source) {
            this.start = source.start;
            this.end = source.end;
            this.task = source.task;
            this.deadline = source.deadline;
            this.financialCost = source.financialCost;
        }

        public double getEnd() { return end; }
        public double getStart() { return start; }
        public Task getTask() { return task; }
        public double getFinancialCost() { return financialCost; }
        public double getDeadline() { return deadline; }
        public double getEt() {
            return this.getEnd() - getStart();
        }

    }

    // --- VM Getters ---
    public int getId() { return id; }
    public double getC() { return c; }
    public double getW() { return w; }
    public double getGsr() { return gsr; }
    public double getGsw() { return gsw; }
    public HardwareType getTyp() {
        return typ;
    }
    public List<TimeSlot> getSchedule() { return schedule; }

    // --- Custom VMs ---
    public static VM createE2MicroVM() {
        double costPerHour = 0.0092215;
        double fp64TFLOPS = 0.024;
        double readSpeed = 42;
        double writeSpeed = 42;
        return new VM(nextVmId++, "CPU-E2-MICRO", costPerHour, fp64TFLOPS, readSpeed, writeSpeed, HardwareType.CPU);
    }

    public static VM createE2SmallVM() {
        double costPerHour = 0.01844301;
        double fp64TFLOPS = 0.048;
        double readSpeed = 42;
        double writeSpeed = 42;
        return new VM(nextVmId++, "CPU-E2-SMALL", costPerHour, fp64TFLOPS, readSpeed, writeSpeed, HardwareType.CPU);
    }

    public static VM createIntelCascadeLakeVM() {
        double costPerHour = 0.033982;
        double fp64TFLOPS = 2.42;
        double readSpeed = 140.78;
        double writeSpeed = 140.78;
        return new VM(nextVmId++, "CPU-C2", costPerHour, fp64TFLOPS, readSpeed, writeSpeed, HardwareType.CPU);
    }

    public static VM createIntelEmeraldRapidsVM() {
        double costPerHour = 0.03938;
        double fp64TFLOPS = 4.10;
        double readSpeed = 307.2;
        double writeSpeed = 307.2;
        return new VM(nextVmId++, "CPU-C4", costPerHour, fp64TFLOPS, readSpeed, writeSpeed, HardwareType.CPU);
    }

    public static VM createNvidiaP100VM() {
        double costPerHour = 1.6;
        double fp64TFLOPS = 4.763;
        double readSpeed = 732.2;
        double writeSpeed = 732.2;
        return new VM(nextVmId++, "GPU-P100", costPerHour, fp64TFLOPS, readSpeed, writeSpeed, HardwareType.GPU);
    }

    public static VM createNvidiaV100VM() {
        double costPerHour = 2.55;
        double fp64TFLOPS = 7.066;
        double readSpeed = 897.0;
        double writeSpeed = 897.0;
        return new VM(nextVmId++, "GPU-V100", costPerHour, fp64TFLOPS, readSpeed, writeSpeed, HardwareType.GPU);
    }

    public static VM createNvidiaH200VM() {
        double costPerHour = 3.72;
        double fp64TFLOPS = 30.16;
        double readSpeed = 4890.0;
        double writeSpeed = 4890.0;
        return new VM(nextVmId++, "GPU-H200", costPerHour, fp64TFLOPS, readSpeed, writeSpeed, HardwareType.GPU);
    }

    public void clearSchedule() {
        this.schedule.clear();
    }

    /**
     * Calculates the total cost of all tasks scheduled on this VM.
     * It calculates the total number of unique billing periods rented.
     */
    public double calculateTotalCost(int tau) {
        if (schedule.isEmpty()) {
            return 0.0;
        }

        Set<Integer> rentedPeriods = new HashSet<>();
        for (TimeSlot slot : schedule) {
            int startPeriod = (int) Math.floor(slot.start / tau);
            int endPeriodIndex = (int) Math.ceil(slot.end / tau);
            for (int i = startPeriod; i < endPeriodIndex; i++) {
                rentedPeriods.add(i);
            }
        }
        return rentedPeriods.size() * this.c;
    }

    /**
     * Calculates the financial cost of scheduling a task at a specific time.
     */
    public double vmCalculateFinancialCost(double taskStartTime, double taskFinishTime, double tau) {
        Set<Integer> rentedPeriods = new HashSet<>();
        for (TimeSlot existingSlot : this.schedule) {
            int startPeriod = (int) Math.floor(existingSlot.start / tau);
            int endPeriodIndex = (int) Math.ceil(existingSlot.end / tau);
            for (int i = startPeriod; i < endPeriodIndex; i++) {
                rentedPeriods.add(i);
            }
        }

        int newStartPeriod = (int) Math.floor(taskStartTime / tau);
        int newEndPeriodIndex = (int) Math.ceil(taskFinishTime / tau);
        int additionalPeriodsToRent = 0;
        for (int i = newStartPeriod; i < newEndPeriodIndex; i++) {
            if (!rentedPeriods.contains(i)) {
                additionalPeriodsToRent++;
            }
        }
        return additionalPeriodsToRent * this.getC();
    }

    /**
     * Finds the most cost-effective placement for a task on this VM by checking all valid idle gaps.
     */
    public TimeSlot findBestSlotForTask(Task task, double executionTime, double earliestStartTime, double deadline, double tau) {
        TimeSlot bestSlot = null;
        double minCost = Double.POSITIVE_INFINITY;

        // Generate all possible idle time windows where the task could fit.
        List<double[]> idleWindows = new ArrayList<>();
        double lastFinishTime = 0.0;
        schedule.sort(Comparator.comparingDouble(TimeSlot::getStart));

        for (TimeSlot scheduledSlot : schedule) {
            if (scheduledSlot.start > lastFinishTime) {
                idleWindows.add(new double[]{lastFinishTime, scheduledSlot.start});
            }
            lastFinishTime = scheduledSlot.end;
        }
        idleWindows.add(new double[]{lastFinishTime, Double.MAX_VALUE}); // The gap after the last task

        // Check every idle window for the best placement.
        for (double[] window : idleWindows) {
            double windowStart = window[0];
            double windowEnd = window[1];

            // The task cannot start before its dependencies are met OR before the window opens.
            double effectiveStartTime = Math.max(earliestStartTime, windowStart);

            // Potential start time 1: Immediately when possible.
            double actualStart1 = effectiveStartTime;
            double finishTime1 = actualStart1 + executionTime;

            if (finishTime1 <= deadline && finishTime1 <= windowEnd) {
                double cost = vmCalculateFinancialCost(actualStart1, finishTime1, tau);
                if (cost < minCost) {
                    minCost = cost;
                    bestSlot = new TimeSlot(actualStart1, finishTime1, task, deadline, cost);
                }
            }

            // Potential start time 2: The beginning of the next billing period after effectiveStartTime.
            // This can sometimes be cheaper if it avoids renting a partial period.
            double nextBillingPeriodStart = Math.ceil(effectiveStartTime / tau) * tau;
            if (nextBillingPeriodStart > effectiveStartTime && nextBillingPeriodStart < windowEnd) {
                double actualStart2 = nextBillingPeriodStart;
                double finishTime2 = actualStart2 + executionTime;

                if (finishTime2 <= deadline && finishTime2 <= windowEnd) {
                    double cost = vmCalculateFinancialCost(actualStart2, finishTime2, tau);
                    if (cost < minCost) {
                        minCost = cost;
                        bestSlot = new TimeSlot(actualStart2, finishTime2, task, deadline, cost);
                    }
                }
            }
        }
        return bestSlot;
    }

    /**
     * Schedules a task by finding the earliest possible start time and adding it to the schedule.
     */
    public TimeSlot scheduleTask(Task task, double executionTime, double earliestStartTime, double deadline, double financialCost) {
        double actualStartTime = findEarliestAvailableStartTime(executionTime, earliestStartTime);
        double finishTime = actualStartTime + executionTime;

        TimeSlot newSlot = new TimeSlot(actualStartTime, finishTime, task, deadline, financialCost);
        addSlotToSchedule(newSlot);
        return newSlot;
    }

    /**
     * Finds the first available start time for a new task.
     */
    public double findEarliestAvailableStartTime(double duration, double earliestStartTime) {
        schedule.sort(Comparator.comparingDouble(TimeSlot::getStart));
        double lastFinishTime = 0.0;

        // Check gap before the first task
        double potentialStart = Math.max(earliestStartTime, lastFinishTime);
        if (schedule.isEmpty()) {
            return potentialStart;
        }
        if (potentialStart + duration <= schedule.get(0).start) {
            return potentialStart;
        }
        lastFinishTime = schedule.get(0).end;

        // Check gaps between existing tasks
        for (int i = 1; i < schedule.size(); i++) {
            potentialStart = Math.max(earliestStartTime, lastFinishTime);
            if (potentialStart + duration <= schedule.get(i).start) {
                return potentialStart;
            }
            lastFinishTime = schedule.get(i).end;
        }

        // If no gap found, schedule it after the very last task
        return Math.max(earliestStartTime, lastFinishTime);
    }

    public void addSlotToSchedule(TimeSlot newSlot) {
        this.schedule.add(newSlot);
        this.schedule.sort(Comparator.comparingDouble(slot -> slot.start));
    }
}