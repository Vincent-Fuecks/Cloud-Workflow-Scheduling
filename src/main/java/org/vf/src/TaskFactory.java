package org.vf.src;

import java.util.List;

@FunctionalInterface
public interface TaskFactory<T extends Task> {
    T create(int id,
             double din,
             double dout,
             double mi,
             List<Integer> parents,
             List<Integer> children,
             HardwareType type
    );
}
