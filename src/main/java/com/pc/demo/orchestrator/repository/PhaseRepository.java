package com.pc.demo.orchestrator.repository;

import java.util.List;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.pc.demo.orchestrator.entity.Phase;

@Repository
public interface PhaseRepository extends CrudRepository<Phase, Long> {
    /**
     * Find by phase.
     * @param phase the phase
     * @return entities
     */
    @Query("SELECT p FROM Phase p WHERE p.phase = :phase")
    Phase findByPhase(@Param("phase") Integer phase);

    /**
     * Find a phase based on name.
     * @param name the name
     * @return a phase
     */
    @Query("SELECT p FROM Phase p WHERE p.name = :name")
    Phase findByName(@Param("name") String name);

    /**
     * Find a phase based on ID.
     * @param id the ID
     * @return a phase
     */
    @Query("SELECT p FROM Phase p WHERE p.id = :id")
    Phase findById(@Param("id") Integer id);

    /**
     * Find active phases.
     * @param type the type
     * @return a phase
     */
    @Query("SELECT p FROM Phase p WHERE p.active = 1")
    List<Phase> findActive();

    /**
     * Find phases by role.
     * @param role the role
     * @return a phase
     */
    @Query("SELECT p FROM Phase p WHERE p.role = :role")
    List<Phase> findByRole(@Param("role") String role);
}
