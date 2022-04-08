package com.pc.demo.orchestrator.entity.impl;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.pc.demo.orchestrator.entity.Phase;
import com.pc.demo.orchestrator.repository.PhaseRepository;
import com.pc.demo.orchestrator.service.entity.PhaseService;

@Service
public class PhaseServiceImpl implements PhaseService {

	@Autowired
	private PhaseRepository phaseRepository;

	@Override
	public void create(Phase phase) {
		phaseRepository.save(phase);
	}

	@Override
	public void modify(Phase phase) {
		phaseRepository.save(phase);
	}

	@Override
	public void delete(Phase phase) {
		phaseRepository.save(phase);
	}

	@Override
	public List<Phase> findAll() {
		List<Phase> list = new ArrayList<>();
		phaseRepository.findAll().iterator().forEachRemaining(list::add);
		return list;
	}

	@Override
	public List<Phase> findActive() {
		List<Phase> list = new ArrayList<>();
		phaseRepository.findActive().iterator().forEachRemaining(list::add);
		return list;
	}

	@Override
	public Phase find(Integer id) {
		return phaseRepository.findById(id);
	}

	@Override
	public Phase find(String name) {
		return phaseRepository.findByName(name);
	}
}
