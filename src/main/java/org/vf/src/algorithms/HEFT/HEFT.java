package org.vf.src.algorithms.HEFT;

import org.vf.src.SchedulingAlgorithm;
import org.vf.src.Task;
import org.vf.src.VM;

import java.util.*;

import static org.vf.src.algorithms.Utils.*;

public class HEFT implements SchedulingAlgorithm {

    private final ArrayList<TaskHEFT> tasks;
    private final ArrayList<VM> vms;
    private final Map<Integer, TaskHEFT> taskMap;

    private final Map<Task, Double> actualStartTimes = new HashMap<>();
    private final Map<Task, Double> actualFinishTimes = new HashMap<>();
    private final Map<Task, VM> taskAssignments = new HashMap<>();

    public HEFT(ArrayList<TaskHEFT> tasks, ArrayList<VM> vms) {
        this.tasks = tasks;
        this.vms = vms;
        this.taskMap = new HashMap<>();

        for (TaskHEFT task : tasks) {
            this.taskMap.put(task.getId(), task);
        }
    }

    @Override
    public ArrayList<VM>  scheduler() {
        for (VM vm : vms) {
            vm.clearSchedule();
        }

        for (TaskHEFT task : tasks) {
            calculateUpwardRank(task, vms, taskMap);
        }

        // Scheduling list sorted by decreasing order of upward rank.
        PriorityQueue<TaskHEFT> taskQueue = new PriorityQueue<>(Comparator.comparingDouble(TaskHEFT::getUpperRank).reversed());
        taskQueue.addAll(tasks);

        while (!taskQueue.isEmpty()) {
            TaskHEFT task = taskQueue.poll();

            double minEFT = Double.POSITIVE_INFINITY;
            VM bestVM = null;

            for (VM vm : vms) {
                if (vm.getTyp() == task.getTyp()) {
                    double eft = calculateEFT(task, vm, taskMap, taskAssignments, actualFinishTimes);
                    if (eft < minEFT) {
                        minEFT = eft;
                        bestVM = vm;
                    }
                }
            }

            // Assign the task to the best VM found.
            double executionTime = getExecutionTime(task, bestVM);
            double actualStartTime = minEFT - executionTime;

            // Store scheduling decision
            actualStartTimes.put(task, actualStartTime);
            actualFinishTimes.put(task, minEFT);
            taskAssignments.put(task, bestVM);

            bestVM.addSlotToSchedule(new VM.TimeSlot(actualStartTime, minEFT, task, 0, 0));
        }
        return vms;
    }
}
