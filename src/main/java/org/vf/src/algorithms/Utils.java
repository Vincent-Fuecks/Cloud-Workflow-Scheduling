package org.vf.src.algorithms;

import org.vf.src.Task;
import org.vf.src.VM;
import org.vf.src.algorithms.HEFT.TaskHEFT;

import java.util.ArrayList;
import java.util.Map;

public class Utils {

    public static double calculateEFT(TaskHEFT task, VM vm, Map<Integer, TaskHEFT> taskMap, Map<Task, VM> taskAssignments, Map<Task, Double> actualFinishTimes) {
        double executionTime = getExecutionTime(task, vm);
        double earliestStartTime = calculateEST(task, vm, taskMap, taskAssignments, actualFinishTimes);
        return earliestStartTime + executionTime;
    }

    public static double calculateEST(TaskHEFT task, VM vm, Map<Integer, TaskHEFT> taskMap, Map<Task, VM> taskAssignments, Map<Task, Double> actualFinishTimes) {
        double vmReadyTime = vm.findEarliestAvailableStartTime(0, 0);

        double maxParentFinishTime = 0.0;
        for (Integer parentId : task.getParents()) {
            TaskHEFT parentTask = taskMap.get(parentId);
            VM parentVM = taskAssignments.get(parentTask);
            double parentFinishTime = actualFinishTimes.get(parentTask);

            double communicationCost = 0.0;
            if (parentVM != null && parentVM.getId() != vm.getId()) {
                communicationCost = parentTask.getDout() / vm.getGsr();
            }
            maxParentFinishTime = Math.max(maxParentFinishTime, parentFinishTime + communicationCost);
        }

        return Math.max(vmReadyTime, maxParentFinishTime);
    }

    public static double getExecutionTime(Task task, VM vm) {
        return task.getDin() / vm.getGsr() +
                task.getMi() / vm.getW() +
                task.getDout() / vm.getGsw();
    }

    public static double getAvgExecutionTime(Task task, ArrayList<VM> vms) {
        double totalExecutionTime = 0;
        for (VM vm : vms) {
            totalExecutionTime += getExecutionTime(task, vm);
        }

        return totalExecutionTime / vms.size();
    }

    public static double getAvgCommunicationCost(Task task, ArrayList<VM> vms) {
        double totalCommunicationCost = 0;
        for (VM vm : vms) {
            totalCommunicationCost += task.getDin() / vm.getGsr() + task.getDout() / vm.getGsw();
        }
        return totalCommunicationCost / vms.size();
    }

    public static double calculateUpwardRank(TaskHEFT task, ArrayList<VM> vms, Map<Integer, TaskHEFT> taskMap) {
        if (task.getUpperRank() != -1) {
            return task.getUpperRank();
        }

        double maxSuccPath = 0.0;
        for (Integer childId : task.getChildren()) {
            TaskHEFT childTask = taskMap.get(childId);
            double avgCommCost = getAvgCommunicationCost(task, vms);
            double childRank = calculateUpwardRank(childTask, vms, taskMap);
            maxSuccPath = Math.max(maxSuccPath, avgCommCost + childRank);
        }

        double avgExecTime = getAvgExecutionTime(task, vms);
        double rank = avgExecTime + maxSuccPath;
        task.setUpperRank(rank);

        return rank;
    }
}
