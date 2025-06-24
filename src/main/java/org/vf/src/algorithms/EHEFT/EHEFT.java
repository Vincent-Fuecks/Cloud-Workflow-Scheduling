package org.vf.src.algorithms.EHEFT;

import org.vf.src.HardwareType;
import org.vf.src.algorithms.HEFT.TaskHEFT;
import org.vf.src.SchedulingAlgorithm;
import org.vf.src.Task;
import org.vf.src.VM;

import java.util.*;
import java.util.stream.Collectors;

import static org.vf.src.algorithms.Utils.*;

public class EHEFT implements SchedulingAlgorithm {

    private final ArrayList<TaskHEFT> tasks;
    private final ArrayList<VM> vms;
    private final Map<Integer, TaskHEFT> taskMap;

    private final Map<Task, Double> actualStartTimes = new HashMap<>();
    private final Map<Task, Double> actualFinishTimes = new HashMap<>();
    private final Map<Task, VM> taskAssignments = new HashMap<>();

    private Map<VM, Double> vmThresholds;
    private Map<VM, Double> assignedWorkload;
    private double totalDagWorkload;

    public EHEFT(ArrayList<TaskHEFT> tasks, ArrayList<VM> vms) {
        this.tasks = tasks;
        this.vms = vms;
        this.taskMap = new HashMap<>();
        for (TaskHEFT task : tasks) {
            this.taskMap.put(task.getId(), task);
        }
    }

    /**
     * E-HEFT scheduling algorithm.
     * 0. VM Threshold Attribution: Assigns a load threshold to each VM for balancing purposes.
     * 1. Task Prioritization: Calculates the upward rank for each task and sorts them in a priority queue.
     * 2. Processor Selection: Assigns each task, in order of priority, to the VM that offers the earliest finish time,
     * while respecting the load balancing thresholds.
     */
    @Override
    public ArrayList<VM> scheduler() {
        for (VM vm : vms) {
            vm.clearSchedule();
        }

        actualStartTimes.clear();
        actualFinishTimes.clear();
        taskAssignments.clear();

        this.assignedWorkload = new HashMap<>();
        for (VM vm : vms) {
            this.assignedWorkload.put(vm, 0.0);
        }

        this.totalDagWorkload = tasks.stream().mapToDouble(Task::getMi).sum();

        // Phase 0: VM Threshold Attribution
        attributeVmThresholds();

        // Phase 1: Task Prioritization (Ranking) like in the HEFT algorithm
        for (TaskHEFT task : tasks) {
            calculateUpwardRank(task, vms, taskMap);
        }

        PriorityQueue<TaskHEFT> taskQueue = new PriorityQueue<>(Comparator.comparingDouble(TaskHEFT::getUpperRank).reversed());
        taskQueue.addAll(tasks);

        // Scheduling
        while (!taskQueue.isEmpty()) {
            TaskHEFT task = taskQueue.poll();

            double minEFT = Double.POSITIVE_INFINITY;
            VM bestVM = null;

            for (VM vm : vms) {
                if (task.getTyp() == vm.getTyp()){
                    double currentLoadPercentage = 0;
                    if (totalDagWorkload > 0) {
                        currentLoadPercentage = this.assignedWorkload.get(vm) / totalDagWorkload;
                    }
                    double vmThreshold = this.vmThresholds.get(vm);

                    if (currentLoadPercentage < vmThreshold) {
                        double eft = calculateEFT(task, vm, taskMap, taskAssignments, actualFinishTimes);
                        if (eft < minEFT) {
                            minEFT = eft;
                            bestVM = vm;
                        }
                    }
                }
            }

            // Fallback: If no VM was found because all are over their threshold,
            // just pick the VM with the best EFT
            if (bestVM == null) {
                minEFT = Double.POSITIVE_INFINITY;
                for (VM vm : vms) {
                    if (task.getTyp() == vm.getTyp()) {
                        double eft = calculateEFT(task, vm, taskMap, taskAssignments, actualFinishTimes);
                        if (eft < minEFT) {
                            minEFT = eft;
                            bestVM = vm;
                        }
                    }
                }
            }

            double executionTime = getExecutionTime(task, bestVM);
            double actualStartTime = minEFT - executionTime;
            actualStartTimes.put(task, actualStartTime);
            actualFinishTimes.put(task, minEFT);
            taskAssignments.put(task, bestVM);
            this.assignedWorkload.put(bestVM, this.assignedWorkload.get(bestVM) + task.getMi());
            bestVM.addSlotToSchedule(new VM.TimeSlot(actualStartTime, minEFT, task, 0, 0));
        }

        return vms;
    }

    /**
     *  I used the TFLOPS of the VM as relative performance metric, as a proxy for a Linpack benchmark score.
     *  The sum of the threshold over all VMs is 1.0.
     */
    private void attributeVmThresholds() {
        this.vmThresholds = new HashMap<>();
        if (vms.isEmpty()) {
            return;
        }

        // Calculate the total computational performance of the entire cluster.
        double totalClusterPerformance = vms.stream().mapToDouble(VM::getW).sum();

        // Calculate the total computational performance of GPU/CPU VMs
        Map<HardwareType, Double> totalClusterPerformanceByHardwareType = vms.stream()
                .collect(Collectors.groupingBy(
                        VM::getTyp,
                        Collectors.summingDouble(VM::getW)
                ));

        // If total performance is zero, distribute load evenly to avoid division by zero.
        if (totalClusterPerformance == 0) {
            for (VM vm : vms) {
                vmThresholds.put(vm, 1.0 / vms.size());
            }
            return;
        }

        // The threshold for each VM is its share of the total power of GPU/CPU VMs.
        for (VM vm : vms) {
            double threshold = vm.getW() / totalClusterPerformanceByHardwareType.get(vm.getTyp());
            vmThresholds.put(vm, threshold);
        }
    }
}