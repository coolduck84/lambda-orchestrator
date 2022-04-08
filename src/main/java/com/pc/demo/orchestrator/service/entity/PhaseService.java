package com.pc.demo.orchestrator.service.entity;

import java.util.List;

import com.pc.demo.orchestrator.entity.Phase;


/**
 * Service for managing all aspects of phases in the system.
 * @author SACHPAL
 */
public interface PhaseService {
    /**
     * Creates a phase.
     * @param phase the entity representation of a phase.
     */
    void create(Phase phase);

    /**
     * Modify a phase.
     * @param phase the entity representation of a phase.
     */
    void modify(Phase phase);

    /**
     * Deactivates a phase.
     * @param phase the entity representation of a phase.
     */
    void delete(Phase phase);

    /**
     * Returns the list of phases in the system.
     * @return the list of phases in the system.
     */
    List<Phase> findAll();

    /**
     * Returns the list of active phases in the system.
     * @return the list of active phases in the system.
     */
    List<Phase> findActive();

    /**
     * Returns the phase based on ID.
     * @param id The phase ID.
     * @return a phase.
     */
    Phase find(Integer phase);

    /**
     * Returns the phase based on name.
     * @param name The phase name.
     * @return a phase.
     */
    Phase find(String name);
}
