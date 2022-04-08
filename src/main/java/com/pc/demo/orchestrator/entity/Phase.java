package com.pc.demo.orchestrator.entity;

import java.io.Serializable;

import javax.persistence.Cacheable;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import org.hibernate.annotations.Type;

import lombok.Data;

@Data
@Entity
@Table(name = "phases", schema = "orch_master")
@Cacheable
public class Phase implements Serializable {
	/**
	 * Default serial version UID.
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * The primary key.
	 */
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(name = "id")
	private Integer id;

	/**
	 * The phase.
	 */
	@Column(name = "phase", nullable = false)
	private Integer phase;

	/**
	 * The phase name.
	 */
	@Column(name = "phase_name", nullable = false)
	private String name;

	/**
	 * The phase configuration.
	 */
	@Column(name = "configuration")
	private String configuration;

	/**
	 * Is the phase exclusive. No sub phases.
	 */
	@Column(name = "exclusive", nullable = false)
	@Type(type = "org.hibernate.type.NumericBooleanType")
	private Boolean exclusive;

	/**
	 * Will sub phases execute in parallel.
	 */
	@Column(name = "sub_phases_in_parallel", nullable = false)
	@Type(type = "org.hibernate.type.NumericBooleanType")
	private Boolean subPhasesInParallel;

	/**
	 * Will plugins execute in parallel.
	 */
	@Column(name = "plugins_in_parallel", nullable = false)
	@Type(type = "org.hibernate.type.NumericBooleanType")
	private Boolean pluginsInParallel;

	/**
	 * The plugin batch size.
	 */
	@Column(name = "plugins_batch_size")
	private Integer pluginsBatchSize;

	/**
	 * The time in milli seconds by which processing should complete.
	 */
	@Column(name = "timeout")
	private Long timeout;

    /**
     * Active status.
     */
	@Column(name = "active", nullable = false)
	@Type(type = "org.hibernate.type.NumericBooleanType")
	private Boolean active;

    /**
     * Role based restrictions.
     */
	@Column(name = "role", nullable = true)
	private String role;
	
	@Column(name = "evidence")
	private boolean evidence;
	
	@Column(name = "is_bot")
	private boolean isBot;
	
	/**
	 * Copy Constructor
	 */
	public Phase(Phase phaseCopy){
		this.id=phaseCopy.id;
		this.phase=phaseCopy.phase;
		this.name=phaseCopy.name;
		this.configuration=phaseCopy.configuration;
		this.exclusive=phaseCopy.exclusive;
		this.subPhasesInParallel=phaseCopy.subPhasesInParallel;
		this.pluginsInParallel=phaseCopy.pluginsInParallel;
		this.pluginsBatchSize=phaseCopy.pluginsBatchSize;
		this.timeout=phaseCopy.timeout;
		this.active=phaseCopy.active;
		this.role=phaseCopy.role;
		this.evidence=phaseCopy.evidence;
		this.isBot=phaseCopy.isBot;
	}
	
	 /**
     * Default Constructor
     */
    public Phase(){
    	
    }

    @Override
    public String toString() {
        return "Phase [id=" + id + ", phase=" + phase + ", name=" + name
                + ", configuration=" + configuration + ", exclusive="
                + exclusive + ", subPhasesInParallel=" + subPhasesInParallel
                + ", pluginsInParallel=" + pluginsInParallel
                + ", pluginsBatchSize=" + pluginsBatchSize + ", timeout="
                + timeout + ", active=" + active + ", role=" + role + ", isBot="+isBot+"]";
    }
}
