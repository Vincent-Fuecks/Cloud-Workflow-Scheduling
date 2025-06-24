package org.vf;

import org.vf.src.*;
import org.vf.src.algorithms.CETSS.CETSS;
import org.vf.src.algorithms.CETSS.TaskCETSS;
import org.vf.src.algorithms.EHEFT.EHEFT;
import org.vf.src.evaluation.CsvFile;
import org.vf.src.algorithms.HEFT.HEFT;
import org.vf.src.algorithms.HEFT.TaskHEFT;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.vf.src.evaluation.EvaluationSetup.*;
import static org.vf.src.evaluation.Utils.getEvaluationResults;

public class Main {

    static ArrayList<TaskCETSS> cetssTasks;
    static ArrayList<TaskHEFT> heftTasks;
    static ArrayList<TaskHEFT> eheftTasks;
    static ArrayList<VM> vmsOneOfEach;
    static ArrayList<VM> vmsWithScheduledTasks;
    static ArrayList<Double> evaluationResults;
    static ArrayList<? extends Task> currentTasks;

    private static CETSS cetss;
    private static HEFT heft;
    private static EHEFT eheft;

    static int tau = 3600; // One Hour
    static double workflowDeadline;
    static double minDeadlineEpigenomicsWorkflowWithHugePipDifferences = 500000000;
    static double minDeadlineEpigenomicsWorkflowWithBalancedPipLoad = 500000000;

    static List<String> evaluationHeader = Arrays.asList(
            "Workflow Deadline",
            "Was Workflow within Deadline executed?",
            "Overall Makespan",
            "Total Workflow Cost",
            "Overall Resource Utilization",
            "Avarage Cost Per Task",
            "Avarage Makespan Per Task");

    public static <T extends Task, S extends SchedulingAlgorithm> CsvFile evaluateWorkflowWithDeadlineConstrain(
            CsvFile csvWriter,
            TaskFactory<T> taskFactory,
            String algorithmName,
            AlgorithmScheduler<T, S> scheduler) throws IOException {

        csvWriter.addRow(Arrays.asList("Algorithm", "Workflow", "Workflow Kind", "VM Configuration"));
        csvWriter.addRow(Arrays.asList(algorithmName, "Epigenomics", "Huge Pipline Differences", "One of Each Kind"));
        csvWriter.addNewLine();
        csvWriter.addRow(evaluationHeader);

        for (double relaxDeadlineConstraint = 1; relaxDeadlineConstraint <= 3; relaxDeadlineConstraint += 0.25) {
            currentTasks = getTaskForEpigenomicsWorkflowWithHugePipDifferenceSize(taskFactory);
            vmsOneOfEach = getVMConfig(1);

            workflowDeadline = Math.floor(minDeadlineEpigenomicsWorkflowWithHugePipDifferences * relaxDeadlineConstraint);
            vmsWithScheduledTasks = scheduler.schedule(currentTasks, vmsOneOfEach, tau, (int) Math.floor(workflowDeadline));
            evaluationResults = getEvaluationResults(vmsWithScheduledTasks, currentTasks.size(), tau, workflowDeadline);
            csvWriter.addRow(evaluationResults);
        }

        csvWriter.addNewLine();
        csvWriter.addRow(Arrays.asList("Algorithm", "Workflow", "Workflow Kind", "VM Configuration"));
        csvWriter.addRow(Arrays.asList(algorithmName, "Epigenomics", "Huge Pipline Differences", "One of Each Kind"));
        csvWriter.addNewLine();
        csvWriter.addRow(evaluationHeader);


        for (double relaxDeadlineConstraint = 1; relaxDeadlineConstraint <= 3; relaxDeadlineConstraint += 0.25) {
            currentTasks = getTaskForEpigenomicsWorkflowWithBalancedPipLoad(taskFactory);
            vmsOneOfEach = getVMConfig(1);

            workflowDeadline = Math.floor(minDeadlineEpigenomicsWorkflowWithBalancedPipLoad * relaxDeadlineConstraint);
            vmsWithScheduledTasks = scheduler.schedule(currentTasks, vmsOneOfEach, tau, (int) Math.floor(workflowDeadline));
            evaluationResults = getEvaluationResults(vmsWithScheduledTasks, currentTasks.size(), tau, workflowDeadline);
            csvWriter.addRow(evaluationResults);
        }

        return csvWriter;
    }


    public static void main(String[] args) {
        String evaluationResultsFilePath = "/home/vincent/Desktop/EvaluationResults/evaluationResults.csv";

        try {
            CsvFile csvWriter = new CsvFile(evaluationResultsFilePath);

            // TaskFactory for HEFT
            TaskFactory<TaskHEFT> heftTaskFactory = (id, din, dout, mi, parents, children, hardwareType) ->
                    new TaskHEFT(id, din, dout, mi, parents, children, hardwareType);

            // Scheduler for HEFT
            AlgorithmScheduler<TaskHEFT, HEFT> heftScheduler = (tasks, vms, tau, deadline) -> {
                HEFT heft = new HEFT((ArrayList<TaskHEFT>) tasks, vms);
                return heft.scheduler();
            };
            csvWriter = evaluateWorkflowWithDeadlineConstrain(csvWriter, heftTaskFactory, "HEFT", heftScheduler);



            // TaskFactory for EHEFT
            TaskFactory<TaskHEFT> eheftTaskFactory = (id, din, dout, mi,parents, children, hardwareType) ->
                    new TaskHEFT(id, din, dout, mi, parents, children, hardwareType);

            // Scheduler for HEFT
            AlgorithmScheduler<TaskHEFT, EHEFT> eheftScheduler = (tasks, vms, tau, deadline) -> {
                HEFT eheft = new HEFT((ArrayList<TaskHEFT>) tasks, vms);
                return eheft.scheduler();
            };
            csvWriter = evaluateWorkflowWithDeadlineConstrain(csvWriter, eheftTaskFactory, "E-HEFT", eheftScheduler);



            // TaskFactory for CETSS
            TaskFactory<TaskCETSS> cetssTaskFactory = (id, din, dout, mi, parents, children, hardwareType) ->
                    new TaskCETSS(id, din, dout, mi, parents, children, hardwareType);

            // Scheduler for CETSS
            AlgorithmScheduler<TaskCETSS, CETSS> cetssScheduler = (tasks, vms, tau, deadline) -> {
                CETSS cetss = new CETSS((ArrayList<TaskCETSS>) tasks, vms, tau, deadline);
                return cetss.scheduler();
            };
            csvWriter = evaluateWorkflowWithDeadlineConstrain(csvWriter, cetssTaskFactory, "CETSS", cetssScheduler);
            csvWriter.close();

        } catch (IOException e) {
            System.err.println("Error during writing to CSV file: " + e.getMessage());
            e.printStackTrace();
        }
    }
}