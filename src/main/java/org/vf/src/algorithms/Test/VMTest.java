package org.vf.src.algorithms.Test;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.vf.src.algorithms.CETSS.TaskCETSS;
import org.vf.src.HardwareType;
import org.vf.src.VM;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;

class VMTest {

    private VM vm;
    private TaskCETSS task1, task2, task3;
    private int tau;

    @BeforeEach
    void setUp() {
        vm = new VM(0, 10.0, 1000, 100, 100);
        tau = 3600;

        task1 = new TaskCETSS(1, 100, 100, 1000 * 1000, new ArrayList<>(), new ArrayList<>(), HardwareType.GPU);
        task2 = new TaskCETSS(2, 100, 100, 500 * 1000, new ArrayList<>(), new ArrayList<>(), HardwareType.GPU);
        task3 = new TaskCETSS(3, 100, 100, 500 * 1000, new ArrayList<>(), new ArrayList<>(), HardwareType.GPU);

    }

    @Test
    @DisplayName("findBestSlotForTask should find optimal gap, not just end of schedule")
    void testFindBestSlotForTask_FindsGap() {
        // Schedule two tasks with a large idle gap between them
        // Slot 1: [0, 1000]. Rents period 0. Paid 0 to 3600
        // Slot 2: [8000, 9000]. Rents period 2. Paid 7200 to 10800
        // There is a free idle period [1000, 8000]. Period 1 ([3600, 7200]) is completely free.
        vm.scheduleTask(task1, 1000, 0, 10000, 100.0);
        vm.scheduleTask(task1, 1000, 8000, 10000, 100.0);

        double execTime = 500;
        double deadline = 7000;
        double earliestStart = 1000;

        VM.TimeSlot bestSlotTask2 = vm.findBestSlotForTask(task2, execTime, earliestStart, deadline, tau);
        assertNotNull(bestSlotTask2, "A valid slot should be found.");
        assertEquals(1000, bestSlotTask2.getStart(), "Should start exactly at the beginning of the free billing period to be optimal.");
        assertEquals(1500, bestSlotTask2.getEnd(), "End time should be start + execution time.");
        assertEquals(0, bestSlotTask2.getFinancialCost(), "Cost should be for one new billing period.");


        execTime = 500;
        deadline = 3000;
        earliestStart = 1500;
        VM.TimeSlot bestSlotTask3 = vm.findBestSlotForTask(task2, execTime, earliestStart, deadline, tau);
        assertNotNull(bestSlotTask3, "A valid slot should be found.");
        assertEquals(1500, bestSlotTask3.getStart(), "Should start exactly at the beginning of the free billing period to be optimal.");
        assertEquals(2000, bestSlotTask3.getEnd(), "End time should be start + execution time.");
        assertEquals(0, bestSlotTask3.getFinancialCost(), "Cost should be for one new billing period.");


        execTime = 500;
        deadline = 9000;
        earliestStart = 7350;
        VM.TimeSlot bestSlotTask4 = vm.findBestSlotForTask(task2, execTime, earliestStart, deadline, tau);
        assertNotNull(bestSlotTask4, "A valid slot should be found.");
        assertEquals(7350, bestSlotTask4.getStart(), "Should start exactly at the beginning of the free billing period to be optimal.");
        assertEquals(7850, bestSlotTask4.getEnd(), "End time should be start + execution time.");
        assertEquals(0, bestSlotTask4.getFinancialCost(), "Cost should be for one new billing period.");


        execTime = 3800;
        deadline = 15000;
        earliestStart = 10000;
        VM.TimeSlot bestSlotTask5 = vm.findBestSlotForTask(task2, execTime, earliestStart, deadline, tau);
        assertNotNull(bestSlotTask4, "A valid slot should be found.");
        assertEquals(10000, bestSlotTask5.getStart(), "Should start exactly at the beginning of the free billing period to be optimal.");
        assertEquals(13800, bestSlotTask5.getEnd(), "End time should be start + execution time.");
        assertEquals(10.0, bestSlotTask5.getFinancialCost(), "Cost should be for one new billing period.");
    }

}