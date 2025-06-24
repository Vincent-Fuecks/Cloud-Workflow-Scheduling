package org.vf.src.algorithms.CETSS;


import org.vf.src.HardwareType;
import org.vf.src.SchedulingAlgorithm;
import org.vf.src.Task;
import org.vf.src.VM;
import org.vf.src.algorithms.CETSS.UtilsCETSS.TaskVmPair;
import org.vf.src.algorithms.CETSS.UtilsCETSS.LevelGroup;

import java.util.*;
import java.util.stream.Collectors;

import static org.vf.src.algorithms.CETSS.UtilsCETSS.*;
import static org.vf.src.Task.Status.SCHEDULED;

public class CETSS implements SchedulingAlgorithm {

    private final ArrayList<TaskCETSS> tasks;
    private final ArrayList<VM> vms;
    private final int tau;
    private final int workflowDeadline;

    private final double phiCPU;
    private final double phiGPU;
    private final double [] vmResourceWeights = new double [] {0.4, 0.25, 0.25, 0.1};
    private final VM vmstCPU;
    private final VM vmstGPU;

    private final double [] te;
    private final double [] tl;

    public CETSS(ArrayList<TaskCETSS> tasks, ArrayList<VM> vms, int tau, int workflowDeadline) {
        this.tasks = tasks;
        this.vms = vms;
        this.tau = tau;
        this.workflowDeadline = workflowDeadline;

        // Initialize standard VM for CPU and GPU
        HashMap<HardwareType, ArrayList<VM>> vmsByHardwareTypMap = getVMSeparatedByHardwareTyp(vms);
        ArrayList<VM> cpus = vmsByHardwareTypMap.get(HardwareType.CPU);
        this.phiCPU = getPHI(cpus, vmResourceWeights);
        double wVMstCPU = computeStandardMetric(phiCPU, cpus, VM::getW);
        double gsrVMstCPU = computeStandardMetric(phiCPU, cpus, VM::getGsr);
        double gswVMstCPU = computeStandardMetric(phiCPU, cpus, VM::getGsw);
        this.vmstCPU = new VM(-1,"vmst-CPU", -1, wVMstCPU, gsrVMstCPU, gswVMstCPU, HardwareType.CPU);

        ArrayList<VM> gpus = vmsByHardwareTypMap.get(HardwareType.GPU);
        this.phiGPU = getPHI(gpus, vmResourceWeights);
        double wVMstGPU = computeStandardMetric(phiGPU, gpus, VM::getW);
        double gsrVMstGPU = computeStandardMetric(phiGPU, gpus, VM::getGsr);
        double gswVMstGPU = computeStandardMetric(phiGPU, gpus, VM::getGsw);
        this.vmstGPU = new VM(-1, "vmst-GPU", -1, wVMstGPU, gsrVMstGPU, gswVMstGPU, HardwareType.GPU);
        HashMap<HardwareType, VM> vmstMap = new HashMap<>();
        vmstMap.put(HardwareType.CPU, vmstCPU);
        vmstMap.put(HardwareType.GPU, vmstGPU);

        this.te = getEarliestFinishTimes(this.tasks, vmstMap);
        this.tl = getLatestFinishTimes(this.tasks, vmstMap);
    }

    public HashMap<HardwareType, ArrayList<VM>> getVMSeparatedByHardwareTyp(ArrayList<VM> vms){
        HashMap<HardwareType, ArrayList<VM>> vmsSeparatedByHardwareTyp = new HashMap<>();

        ArrayList<VM> gpus = new ArrayList<>();
        ArrayList<VM> cpus = new ArrayList<>();

        for (VM vm : vms) {
            if (vm.getTyp() == HardwareType.CPU) {
                cpus.add(vm);
            } else {
                gpus.add(vm);
            }
        }
        vmsSeparatedByHardwareTyp.put(HardwareType.CPU, cpus);
        vmsSeparatedByHardwareTyp.put(HardwareType.GPU, gpus);
        return vmsSeparatedByHardwareTyp;
    }

    @Override
    public ArrayList<VM> scheduler() {
        return performTaskAdjustment();
    }

    /**
     * Executes the greedy workflow scheduling algorithm based on the CETSS paper.
     */
    public ArrayList<VM> getGreedyWorkflowScheduling() {
        for (TaskCETSS task : tasks) {
            task.reset();
        }

        for (VM vm : vms) {
            vm.clearSchedule();
        }

        Map<Integer, LevelGroup> levelGroups = getLevelGroups(tasks);
        Queue<TaskCETSS> schedulableTasks = new LinkedList<>();

        // Initialize the queue with entry tasks (in-degree == 0).
        for (TaskCETSS task : tasks) {
            if (task.getInDegree() == 0) {
                schedulableTasks.add(task);
                task.setEarliestStartTime(0.0);
            }
        }

        while (!schedulableTasks.isEmpty()) {
            TaskVmPair bestChoice = null;

            // Phase 1: Find the cheapest task-VM assignment that MEETS the subdeadline.
            for (TaskCETSS task : schedulableTasks) {
                LevelGroup levelGroup = levelGroups.get(task.getGroupLevel());
                if (levelGroup == null) continue;
                double subdeadline = levelGroup.getSubdeadline();

                for (VM vm : vms) {
                    if (vm.getTyp() == task.getTyp()){
                        double executionTime = getExecutionTime(task, vm);
                        double actualEST = vm.findEarliestAvailableStartTime(executionTime, task.getEarliestStartTime());
                        double actualEFT = actualEST + executionTime;

                        if (actualEFT <= subdeadline) {
                            double financialCost = vm.vmCalculateFinancialCost(actualEST, actualEFT, this.tau);
                            TaskVmPair candidate = new TaskVmPair(task, vm, financialCost, actualEST, actualEFT, executionTime);

                            // If this candidate is cheaper, it's the new best choice.
                            if (bestChoice == null || candidate.getFinancialCost() < bestChoice.getFinancialCost()) {
                                bestChoice = candidate;
                            } else if (candidate.getFinancialCost() == bestChoice.getFinancialCost()) {
                                // Tie-breaking rule: if costs are equal, choose the one that finishes earlier.
                                if (candidate.getEft() < bestChoice.getEft()) {
                                    bestChoice = candidate;
                                }
                            }
                        }
                    }

                }
            }

            // Phase 2: If no assignment could meet its deadline (bestChoice is null),
            // we must violate the deadline. Choose the assignment that finishes the earliest
            // to minimize the extent of the violation.
            if (bestChoice == null) {
                TaskVmPair earliestFinishChoice = null;
                for (TaskCETSS task : schedulableTasks) {
                    for (VM vm : vms) {
                        if (vm.getTyp() == task.getTyp()){
                            double executionTime = getExecutionTime(task, vm);
                            double actualEST = vm.findEarliestAvailableStartTime(executionTime, task.getEarliestStartTime());
                            double actualEFT = actualEST + executionTime;
                            double financialCost = vm.vmCalculateFinancialCost(actualEST, actualEFT, this.tau);
                            TaskVmPair candidate = new TaskVmPair(task, vm, financialCost, actualEST, actualEFT, executionTime);

                            if (earliestFinishChoice == null || candidate.getEft() < earliestFinishChoice.getEft()) {
                                earliestFinishChoice = candidate;
                            }
                        }
                    }
                }
                bestChoice = earliestFinishChoice;
            }

            if (bestChoice == null) {
                System.err.println("Error: Could not determine a best choice for scheduling. Workflow might be stuck.");
                return vms;
            }

            // --- Schedule the chosen task ---
            TaskCETSS taskToSchedule = bestChoice.getTask();
            VM vmToScheduleOn = bestChoice.getVm();
            LevelGroup currentLevelGroup = levelGroups.get(taskToSchedule.getGroupLevel());
            double finalFinishTime = bestChoice.getEft();

            VM.TimeSlot scheduledSlot = vmToScheduleOn.scheduleTask(taskToSchedule, bestChoice.getEt(), bestChoice.getEst(), currentLevelGroup.getSubdeadline(), bestChoice.getFinancialCost());
            taskToSchedule.setStatus(SCHEDULED);
            schedulableTasks.remove(taskToSchedule);

            // --- Subdeadline Relaxation ---
            // If the actual finish time is later than the level's estimated finish time,
            // it may delay subsequent tasks. This logic updates the estimates.
            if (finalFinishTime > currentLevelGroup.getEft()) {
                currentLevelGroup.setEft(finalFinishTime);
                double newSubdeadline = getSubdeadline(finalFinishTime, te[tasks.size() - 1], this.workflowDeadline);
                currentLevelGroup.setSubdeadline(newSubdeadline);
            }

            // --- Update Children and Add to Queue ---
            for (Integer childId : taskToSchedule.getChildren()) {
                TaskCETSS child = tasks.get(childId);
                // A child's earliest start time is determined by the latest finishing parent.
                child.setEarliestStartTime(Math.max(child.getEarliestStartTime(), scheduledSlot.getEnd()));
                child.reduceInDegree();
                if (child.getInDegree() == 0 && child.getStatus() == Task.Status.UNSCHEDULED) {
                    schedulableTasks.add(child);
                }
            }
        }
        return vms;
    }

    public ArrayList<VM> performTaskAdjustment() {
        ArrayList<VM> scheduledVMs = getGreedyWorkflowScheduling();

        Map<Integer, VM> taskToVmMap = new HashMap<>();
        Map<Integer, VM.TimeSlot> taskToSlotMap = new HashMap<>();
        for (VM vm : scheduledVMs) {
            for (VM.TimeSlot slot : vm.getSchedule()) {
                taskToVmMap.put(slot.getTask().getId(), vm);
                taskToSlotMap.put(slot.getTask().getId(), slot);
            }
        }

        List<TaskCETSS> revTopOrder = getReversedTopologicalOrder(this.tasks);
        Map<Integer, LevelGroup> levelGroups = getLevelGroups(this.tasks);

        // Iterate through each task and try to find a cheaper VM placement.
        for (TaskCETSS taskToAdjust : revTopOrder) {
            VM sourceVm = taskToVmMap.get(taskToAdjust.getId());
            VM.TimeSlot sourceSlot = taskToSlotMap.get(taskToAdjust.getId());

            if (sourceVm == null || sourceSlot == null) continue;

            double currentCost = sourceSlot.getFinancialCost();
            if (currentCost == 0) continue; // Cannot be improved if it's already free.

            VM.TimeSlot bestReplacementSlot = null;
            VM bestReplacementVm = null;

            // Calculate time constraints from the CURRENT schedule.
            double parentsLatestFT = 0.0;
            for (int parentId : taskToAdjust.getParents()) {
                VM.TimeSlot parentSlot = taskToSlotMap.get(parentId);
                if (parentSlot != null) {
                    parentsLatestFT = Math.max(parentsLatestFT, parentSlot.getEnd());
                }
            }

            double childrenEarliestST = Double.POSITIVE_INFINITY;
            for (int childId : taskToAdjust.getChildren()) {
                VM.TimeSlot childSlot = taskToSlotMap.get(childId);
                if (childSlot != null) {
                    childrenEarliestST = Math.min(childrenEarliestST, childSlot.getStart());
                }
            }

            double taskSubdeadline = levelGroups.get(taskToAdjust.getGroupLevel()).getSubdeadline();
            double finalDeadline = Math.min(childrenEarliestST, taskSubdeadline);

            // Check every other VM as a potential new host.
            for (VM targetVm : this.vms) {
                if (targetVm.getTyp() == taskToAdjust.getTyp()){
                    if (targetVm.getId() == sourceVm.getId()) continue;

                    double executionTimeOnTarget = getExecutionTime(taskToAdjust, targetVm);

                    VM.TimeSlot candidateSlot = targetVm.findBestSlotForTask(taskToAdjust, executionTimeOnTarget, parentsLatestFT, finalDeadline, this.tau);

                    // If a valid, cheaper slot is found
                    if (candidateSlot != null && candidateSlot.getFinancialCost() < currentCost) {
                        // Check if it's the best one found so far
                        if (bestReplacementSlot == null || candidateSlot.getFinancialCost() < bestReplacementSlot.getFinancialCost()) {
                            bestReplacementSlot = candidateSlot;
                            bestReplacementVm = targetVm;
                        }
                    }
                }
            }

            // If a better placement was found, perform the adjustment.
            if (bestReplacementSlot != null) {
                // Remove from old VM
                sourceVm.getSchedule().remove(sourceSlot);

                // Add to new VM
                bestReplacementVm.getSchedule().add(bestReplacementSlot);
                // Keep the schedule sorted by start time
                bestReplacementVm.getSchedule().sort(Comparator.comparingDouble(VM.TimeSlot::getStart));

                // Update maps to reflect the change for subsequent tasks
                taskToVmMap.put(taskToAdjust.getId(), bestReplacementVm);
                taskToSlotMap.put(taskToAdjust.getId(), bestReplacementSlot);
            }
        }

        return scheduledVMs;
    }

    private static List<TaskCETSS> getReversedTopologicalOrder(List<TaskCETSS> tasks) {
        List<TaskCETSS> topologicalOrder = new ArrayList<>();
        Map<Integer, Integer> inDegree = new HashMap<>();
        Map<Integer, TaskCETSS> taskMap = tasks.stream().collect(Collectors.toMap(Task::getId, task -> task));

        for (TaskCETSS task : tasks) {
            inDegree.put(task.getId(), task.getInDegree());
        }

        Queue<TaskCETSS> queue = new LinkedList<>();
        for (TaskCETSS task : tasks) {
            if (inDegree.get(task.getId()) == 0) {
                queue.add(task);
            }
        }

        while (!queue.isEmpty()) {
            TaskCETSS u = queue.poll();
            topologicalOrder.add(u);

            for (Integer vId : u.getChildren()) {
                int newInDegree = inDegree.get(vId) - 1;
                inDegree.put(vId, newInDegree);
                if (newInDegree == 0) {
                    queue.add(taskMap.get(vId));
                }
            }
        }

        Collections.reverse(topologicalOrder);
        return topologicalOrder;
    }

    /**
     * Groups tasks into levels based on their depth (longest path from an entry task).
     * It then identifies the critical task within each level to set the level's subdeadline.
     */
    public Map<Integer, LevelGroup> getLevelGroups(List<TaskCETSS> tasks) {
        Map<Integer, LevelGroup> levelGroupsMap = new HashMap<>();
        if (tasks.isEmpty()) return levelGroupsMap;

        // Calculate task depths (levels) using a BFS-like approach.
        Map<TaskCETSS, Integer> taskDepths = new HashMap<>();
        Queue<TaskCETSS> queue = new LinkedList<>();
        Map<TaskCETSS, Integer> inDegrees = new HashMap<>();

        for (TaskCETSS task : tasks) {
            inDegrees.put(task, task.getInDegree());
            taskDepths.put(task, 0);
            if (task.getInDegree() == 0) {
                queue.add(task);
            }
        }

        while (!queue.isEmpty()) {
            TaskCETSS u = queue.poll();
            for (Integer vId : u.getChildren()) {
                TaskCETSS v = tasks.get(vId);
                taskDepths.put(v, Math.max(taskDepths.get(v), taskDepths.get(u) + 1));
                inDegrees.put(v, inDegrees.get(v) - 1);
                if (inDegrees.get(v) == 0) {
                    queue.add(v);
                }
            }
        }

        // Group tasks into LevelGroup objects based on calculated depth.
        for (TaskCETSS task : tasks) {
            int depth = taskDepths.get(task);
            task.setGroupLevel(depth);
            levelGroupsMap.computeIfAbsent(depth, k -> new LevelGroup(k, 0.0, null, 0.0))
                    .addLevelGroupTask(task);
        }

        // Assign subdeadlines to each level group.
        double te_exit = te[tasks.size() - 1];
        for (LevelGroup levelGroup : levelGroupsMap.values()) {
            TaskCETSS criticalTaskInLevel = null;
            double maxEftInLevel = -1.0;

            // Find the critical path task within this level.
            for (TaskCETSS task : levelGroup.getLevelGroupTasks()) {
                int id = task.getId();
                if (Math.abs(this.te[id] - this.tl[id]) < 1e-6) {
                    if (this.te[id] > maxEftInLevel) {
                        maxEftInLevel = this.te[id];
                        criticalTaskInLevel = task;
                    }
                }
            }

            if (criticalTaskInLevel != null) {
                // If a critical task exists, the subdeadline is based on its TE.
                double subdeadline = getSubdeadline(te[criticalTaskInLevel.getId()], te_exit, this.workflowDeadline);
                levelGroup.setEft(maxEftInLevel);
                levelGroup.setSubdeadline(subdeadline);
                levelGroup.setCriticalTask(criticalTaskInLevel);
            } else {
                // If no critical task is in this level, tasks have slack.
                // The subdeadline can be based on the latest finish time (TL) of tasks in the group.
                double maxTlInLevel = 0.0;
                for (TaskCETSS task : levelGroup.getLevelGroupTasks()) {
                    maxTlInLevel = Math.max(maxTlInLevel, this.tl[task.getId()]);
                }
                // Fallback to a very large number if no valid TL is found.
                levelGroup.setSubdeadline(maxTlInLevel > 0 ? maxTlInLevel : Double.MAX_VALUE);
            }
        }
        return levelGroupsMap;
    }
}