package org.vf.src.evaluation;

import org.vf.src.VM;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class Utils {

    public static ArrayList<Double> getEvaluationResults(ArrayList<VM> scheduledVMs, int totalNumOfTasks, int tau, double workflowDeadline){

        double totalWorkflowCost = 0.0;
        double overallMakespan = 0.0;
        double overallResourceUtilization;
        double avgCostPerTask;
        double avgMakespanPerTask;
        double isWorkflowCompletedInDeadline;

        System.out.println("Scheduled Task running under VM: \n");

        for (VM vm : scheduledVMs) {
            System.out.println("VM " + vm.getId() + " (Cost/Period: " + vm.getC() + "):");
            if (vm.getSchedule().isEmpty()) {
                System.out.println("  No tasks scheduled.");
            } else {
                vm.getSchedule().sort(java.util.Comparator.comparingDouble(VM.TimeSlot::getStart));
                for (VM.TimeSlot slot : vm.getSchedule()) {
                    System.out.printf("  Task %d: Start=%.2f (%s), End=%.2f (%s), Cost=%.2f, Deadline=%.2f (%s)\n",
                            slot.getTask().getId(),
                            slot.getStart(), convertSecondsToHMS(slot.getStart()),
                            slot.getEnd(), convertSecondsToHMS(slot.getEnd()),
                            slot.getFinancialCost(),
                            slot.getDeadline(), convertSecondsToHMS(slot.getDeadline()));
                    overallMakespan = Math.max(overallMakespan, slot.getEnd());
                }
                totalWorkflowCost += vm.calculateTotalCost(tau);
            }
            System.out.println("  Total Cost for VM " + vm.getId() + ": " + vm.calculateTotalCost(tau));
        }

        overallResourceUtilization = calculateOverallResourceUtilization(scheduledVMs);
        avgCostPerTask = calculateAverageCostPerTask(totalWorkflowCost, totalNumOfTasks);
        avgMakespanPerTask = calculateAverageMakespanPerTask(overallMakespan, totalNumOfTasks);


        System.out.println("\n--------------------------");
        System.out.printf("Overall Workflow Makespan: %.2f seconds (%s)\n", overallMakespan, convertSecondsToHMS(overallMakespan));
        System.out.printf("Total Workflow Financial Cost: %.2f\n", totalWorkflowCost);
        System.out.printf("Overall Resource Utilization: %.2f\n", overallResourceUtilization);
        System.out.printf("Average Cost Per Task: %.2f\n", avgCostPerTask);
        System.out.printf("Average Makespan Per Task: %.2f\n", avgMakespanPerTask);
        System.out.printf("Defined Workflow Deadline: %.2f seconds (%s)\n", workflowDeadline, convertSecondsToHMS(workflowDeadline));

        if (overallMakespan <= workflowDeadline) {
            isWorkflowCompletedInDeadline = 1.0;
            System.out.println("Workflow completed within the deadline!");
        } else {
            isWorkflowCompletedInDeadline = 0.0;
            System.out.println("Workflow exceeded the deadline!");
        }

        return new ArrayList<>(Arrays.asList(workflowDeadline, isWorkflowCompletedInDeadline, overallMakespan, totalWorkflowCost, overallResourceUtilization, avgCostPerTask, avgMakespanPerTask));
    }

    public static String convertSecondsToHMS(double seconds) {
        int hours = (int) (seconds / 3600);
        int minutes = (int) ((seconds % 3600) / 60);
        int secs = (int) (seconds % 60);
        return String.format("%d:%02d:%02d", hours, minutes, secs);
    }

    public static double calculateOverallResourceUtilization(List<VM> scheduledVMs) {
        double totalTaskExecutionTime = 0.0;
        double totalVmActivePeriod = 0.0;

        for (VM vm : scheduledVMs) {
            if (!vm.getSchedule().isEmpty()) {
                List<VM.TimeSlot> sortedSchedule = vm.getSchedule().stream()
                        .sorted(Comparator.comparingDouble(VM.TimeSlot::getStart))
                        .toList();

                double vmFirstTaskStart = sortedSchedule.get(0).getStart();
                double vmLastTaskEnd = 0.0;
                double vmIndividualActiveExecutionTime = 0.0;

                for (VM.TimeSlot slot : sortedSchedule) {
                    vmLastTaskEnd = Math.max(vmLastTaskEnd, slot.getEnd());
                    vmIndividualActiveExecutionTime += slot.getEt();
                }
                totalTaskExecutionTime += vmIndividualActiveExecutionTime;
                totalVmActivePeriod += (vmLastTaskEnd - vmFirstTaskStart);
            }
        }

        if (totalVmActivePeriod > 0) {
            return (totalTaskExecutionTime / totalVmActivePeriod) * 100.0;
        } else {
            return 0.0;
        }
    }

    public static double calculateAverageCostPerTask(double totalWorkflowCost, int numberOfTasks) {
        if (numberOfTasks == 0) {
            return 0.0;
        }
        return totalWorkflowCost / numberOfTasks;
    }

    public static double calculateAverageMakespanPerTask(double overallMakespan, int numberOfTasks) {
        if (numberOfTasks == 0) {
            return 0.0;
        }
        return overallMakespan / numberOfTasks;
    }

    public static void printGanttChart(ArrayList<VM> scheduledVMs) {
        System.out.println("\n--- Gantt Chart ---");
        if (scheduledVMs == null || scheduledVMs.isEmpty()) {
            System.out.println("No schedule to display.");
            return;
        }

        double makespan = 0;
        for (VM vm : scheduledVMs) {
            for(VM.TimeSlot slot : vm.getSchedule()) {
                makespan = Math.max(makespan, slot.getEnd());
            }
        }

        if (makespan == 0) {
            System.out.println("No tasks were scheduled.");
            return;
        }

        int chartWidth = 100;

        System.out.println("Timescale: 0 to " + String.format("%.2f", makespan) + "s");

        for (VM vm : scheduledVMs) {
            StringBuilder chart = new StringBuilder();
            chart.append(String.format("VM %-2d |", vm.getId()));

            double lastEnd = 0;
            for (VM.TimeSlot slot : vm.getSchedule()) {
                int idleLen = (int) (((slot.getStart() - lastEnd) / makespan) * chartWidth);
                int taskLen = (int) (((slot.getEnd() - slot.getStart()) / makespan) * chartWidth);
                if (taskLen == 0) taskLen = 1; // Ensure task is at least 1 char wide

                for (int i = 0; i < idleLen; i++) chart.append(" ");

                String taskLabel = String.format("T%d", slot.getTask().getId());
                chart.append("[");
                int labelStart = (taskLen - 2 - taskLabel.length()) / 2;
                for(int i = 0; i < taskLen - 2; i++) {
                    if (i >= labelStart && i < labelStart + taskLabel.length()) {
                        chart.append(taskLabel.charAt(i - labelStart));
                    } else {
                        chart.append("=");
                    }
                }
                chart.append("]");
                lastEnd = slot.getEnd();
            }
            System.out.println(chart.toString());
        }
        System.out.println("-------------------\n");
    }
}