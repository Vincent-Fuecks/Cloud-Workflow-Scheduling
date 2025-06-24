package org.vf.src.evaluation;

import org.vf.src.HardwareType;
import org.vf.src.Task;
import org.vf.src.TaskFactory;
import org.vf.src.VM;

import java.util.ArrayList;
import java.util.Arrays;

public class EvaluationSetup {

    public static <T extends Task> ArrayList<T>  getTaskForEpigenomicsWorkflowWithHugePipDifferenceSize(TaskFactory<T> factory){

        // In GB
        double lowComputation = 0.1;
        double midiumComputation = 0.5;
        double largeComputation = 5;
        double UltraComputation = 100;

        // FP64 TFLOPS
        double lowDataTransfer = 10;
        double midiumDataTransfer = 50;
        double largeDataTransfer = 500;
        double UltraDataTransfer = 10000;

        ArrayList<T> tasks = new ArrayList<>();
        // Task 0: t1 (Entry Task)
        tasks.add(factory.create(0, UltraDataTransfer, UltraDataTransfer, midiumComputation, new ArrayList<>(), Arrays.asList(1, 2, 3, 4), HardwareType.CPU)); // t1 -> t2, t3, t4, t5

        // Level 1: t2, t3, t4, t5
        tasks.add(factory.create(1, largeDataTransfer, midiumDataTransfer, lowComputation, Arrays.asList(0), Arrays.asList(5), HardwareType.GPU));  // t2 -> t6
        tasks.add(factory.create(2, largeDataTransfer, midiumDataTransfer, midiumComputation, Arrays.asList(0), Arrays.asList(6), HardwareType.GPU));  // t3 -> t7
        tasks.add(factory.create(3, largeDataTransfer, midiumDataTransfer, largeComputation, Arrays.asList(0), Arrays.asList(7), HardwareType.GPU));  // t4 -> t8
        tasks.add(factory.create(4, largeDataTransfer, midiumDataTransfer, UltraComputation, Arrays.asList(0), Arrays.asList(8), HardwareType.GPU));  // t5 -> t9

        // Level 2: t6, t7, t8, t9
        tasks.add(factory.create(5, midiumDataTransfer, lowDataTransfer, lowComputation, Arrays.asList(1), Arrays.asList(9), HardwareType.CPU));  // t6 -> t10
        tasks.add(factory.create(6, midiumDataTransfer, lowDataTransfer, midiumComputation, Arrays.asList(2), Arrays.asList(10), HardwareType.CPU)); // t7 -> t11
        tasks.add(factory.create(7, midiumDataTransfer, lowDataTransfer, largeComputation, Arrays.asList(3), Arrays.asList(11), HardwareType.CPU)); // t8 -> t12
        tasks.add(factory.create(8, midiumDataTransfer, lowDataTransfer, UltraComputation, Arrays.asList(4), Arrays.asList(12), HardwareType.CPU)); // t9 -> t13

        // Level 3: t10, t11, t12, t13
        tasks.add(factory.create(9, lowDataTransfer, lowDataTransfer, lowComputation, Arrays.asList(5), Arrays.asList(13), HardwareType.GPU)); // t10 -> t14
        tasks.add(factory.create(10, lowDataTransfer, lowDataTransfer, midiumComputation, Arrays.asList(6), Arrays.asList(14), HardwareType.GPU));// t11 -> t15
        tasks.add(factory.create(11, lowDataTransfer, lowDataTransfer, largeComputation, Arrays.asList(7), Arrays.asList(15), HardwareType.GPU));// t12 -> t16
        tasks.add(factory.create(12, lowDataTransfer, lowDataTransfer, UltraComputation, Arrays.asList(8), Arrays.asList(16), HardwareType.GPU));// t13 -> t17

        // Level 4: t14, t15, t16, t17
        tasks.add(factory.create(13, midiumDataTransfer, midiumDataTransfer, lowComputation, Arrays.asList(9), Arrays.asList(17), HardwareType.CPU)); // t14 -> t18
        tasks.add(factory.create(14, midiumDataTransfer, midiumDataTransfer, midiumComputation, Arrays.asList(10), Arrays.asList(17), HardwareType.CPU));// t15 -> t18
        tasks.add(factory.create(15, midiumDataTransfer, midiumDataTransfer, largeComputation, Arrays.asList(11), Arrays.asList(17), HardwareType.CPU));// t16 -> t18
        tasks.add(factory.create(16, midiumDataTransfer, midiumDataTransfer, UltraComputation, Arrays.asList(12), Arrays.asList(17), HardwareType.CPU));// t17 -> t18

        // Level 5: t18
        tasks.add(factory.create(17, UltraDataTransfer, largeDataTransfer, UltraComputation, Arrays.asList(13, 14, 15, 16), Arrays.asList(18), HardwareType.CPU));// t18 -> t19

        // Level 6: t19 (Exit Task)
        tasks.add(factory.create(18, largeDataTransfer, largeDataTransfer, UltraComputation, Arrays.asList(17), new ArrayList<>(), HardwareType.CPU));// t19

        return tasks;
    }

    public static <T extends Task> ArrayList<T> getTaskForEpigenomicsWorkflowWithBalancedPipLoad(TaskFactory<T> factory){
        // In GB
        double lowComputation = 100;
        double midiumComputation = 500;
        double largeComputation = 500;
        double UltraComputation = 10000;

        // FP64 TFLOPS
        double lowDataTransfer = 1000;
        double midiumDataTransfer = 5000;
        double largeDataTransfer = 50000;
        double UltraDataTransfer = 1000000;

        ArrayList<T> tasks = new ArrayList<>();
        // Task 0: t1 (Entry Task)
        tasks.add(factory.create(0, UltraDataTransfer, UltraDataTransfer, midiumComputation, new ArrayList<>(), Arrays.asList(1, 2, 3, 4), HardwareType.CPU)); // t1 -> t2, t3, t4, t5

        // Level 1: t2, t3, t4, t5
        tasks.add(factory.create(1, lowDataTransfer, lowDataTransfer, lowComputation, Arrays.asList(0), Arrays.asList(5), HardwareType.GPU));  // t2 -> t6
        tasks.add(factory.create(2, midiumDataTransfer, midiumDataTransfer, midiumComputation, Arrays.asList(0), Arrays.asList(6), HardwareType.GPU));  // t3 -> t7
        tasks.add(factory.create(3, largeDataTransfer, largeDataTransfer, largeComputation, Arrays.asList(0), Arrays.asList(7), HardwareType.GPU));  // t4 -> t8
        tasks.add(factory.create(4, UltraDataTransfer, UltraDataTransfer, UltraComputation, Arrays.asList(0), Arrays.asList(8), HardwareType.GPU));  // t5 -> t9

        // Level 2: t6, t7, t8, t9
        tasks.add(factory.create(5, UltraDataTransfer, UltraDataTransfer, UltraComputation, Arrays.asList(1), Arrays.asList(9), HardwareType.CPU));  // t6 -> t10
        tasks.add(factory.create(6, lowDataTransfer, lowDataTransfer, lowComputation, Arrays.asList(2), Arrays.asList(10), HardwareType.CPU)); // t7 -> t11
        tasks.add(factory.create(7, midiumDataTransfer, midiumDataTransfer, midiumComputation, Arrays.asList(3), Arrays.asList(11), HardwareType.CPU)); // t8 -> t12
        tasks.add(factory.create(8, largeDataTransfer, largeDataTransfer, largeComputation, Arrays.asList(4), Arrays.asList(12), HardwareType.CPU)); // t9 -> t13

        // Level 3: t10, t11, t12, t13
        tasks.add(factory.create(9, largeDataTransfer, largeDataTransfer, largeComputation, Arrays.asList(5), Arrays.asList(13), HardwareType.GPU)); // t10 -> t14
        tasks.add(factory.create(10, UltraDataTransfer, UltraDataTransfer, UltraComputation, Arrays.asList(6), Arrays.asList(14), HardwareType.GPU));// t11 -> t15
        tasks.add(factory.create(11, lowDataTransfer, lowDataTransfer, lowComputation, Arrays.asList(7), Arrays.asList(15), HardwareType.GPU));// t12 -> t16
        tasks.add(factory.create(12, midiumDataTransfer, midiumDataTransfer, midiumComputation, Arrays.asList(8), Arrays.asList(16), HardwareType.GPU));// t13 -> t17

        // Level 4: t14, t15, t16, t17
        tasks.add(factory.create(13, midiumDataTransfer, midiumDataTransfer, midiumComputation, Arrays.asList(9), Arrays.asList(17), HardwareType.CPU)); // t14 -> t18
        tasks.add(factory.create(14, largeDataTransfer, largeDataTransfer, largeComputation, Arrays.asList(10), Arrays.asList(17), HardwareType.CPU));// t15 -> t18
        tasks.add(factory.create(15, UltraDataTransfer, UltraDataTransfer, UltraComputation, Arrays.asList(11), Arrays.asList(17), HardwareType.CPU));// t16 -> t18
        tasks.add(factory.create(16, lowDataTransfer, lowDataTransfer, lowComputation, Arrays.asList(12), Arrays.asList(17), HardwareType.CPU));// t17 -> t18

        // Level 5: t18
        tasks.add(factory.create(17, UltraDataTransfer, midiumDataTransfer, lowComputation, Arrays.asList(13, 14, 15, 16), Arrays.asList(18), HardwareType.CPU));// t18 -> t19

        // Level 6: t19 (Exit Task)
        tasks.add(factory.create(18, midiumDataTransfer, lowDataTransfer, lowComputation, Arrays.asList(17), new ArrayList<>(), HardwareType.CPU));// t19

        return tasks;
    }

    public static ArrayList<VM> getVMConfig(int numOfVMsOfEachTyp){
        ArrayList<VM> vms = new ArrayList<>();

        for (int i = 0; i < numOfVMsOfEachTyp; i ++) {
            vms.add(VM.createE2MicroVM());
            vms.add(VM.createE2SmallVM());
            vms.add(VM.createIntelCascadeLakeVM());
            vms.add(VM.createIntelEmeraldRapidsVM());
            vms.add(VM.createNvidiaP100VM());
            vms.add(VM.createNvidiaV100VM());
            vms.add(VM.createNvidiaH200VM());
        }

        return vms;
    }

}
