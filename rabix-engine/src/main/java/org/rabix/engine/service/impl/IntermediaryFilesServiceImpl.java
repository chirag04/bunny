package org.rabix.engine.service.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.rabix.bindings.helper.FileValueHelper;
import org.rabix.bindings.model.FileValue;
import org.rabix.bindings.model.Job;
import org.rabix.engine.service.IntermediaryFilesHandler;
import org.rabix.engine.service.IntermediaryFilesService;
import org.rabix.engine.service.LinkRecordService;
import org.rabix.engine.store.repository.IntermediaryFilesRepository;
import org.rabix.engine.store.repository.IntermediaryFilesRepository.IntermediaryFileEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;

public class IntermediaryFilesServiceImpl implements IntermediaryFilesService {

  private final static Logger logger = LoggerFactory.getLogger(IntermediaryFilesServiceImpl.class);

  private IntermediaryFilesRepository intermediaryFilesRepository;
  private LinkRecordService linkRecordService;
  private IntermediaryFilesHandler fileHandler;
  
  @Inject
  protected IntermediaryFilesServiceImpl(LinkRecordService linkRecordService, IntermediaryFilesHandler handler, IntermediaryFilesRepository intermediaryFilesRepository) {
    this.linkRecordService = linkRecordService;
    this.fileHandler = handler;
    this.intermediaryFilesRepository = intermediaryFilesRepository;
  }

  public void decrementFiles(UUID rootId, Set<String> checkFiles) {
    for (String path : checkFiles) {
      intermediaryFilesRepository.decrement(rootId, path);
    }
  }
  
  @Override
  public void handleUnusedFiles(UUID rootId){
    fileHandler.handleUnusedFiles(rootId, getUnusedFiles(rootId));
  }

  @Override
  public void handleJobFailed(Job job) {

  }
  
  private Map<String, Integer> convertToMap(List<IntermediaryFileEntity> filesForRootId) {
    Map<String, Integer> result = new HashMap<>();
    for(IntermediaryFileEntity f: filesForRootId) {
      result.put(f.getFilename(), f.getCount());
    }
    return result;
  }

  public void extractPathsFromFileValue(Set<String> paths, FileValue file) {
    paths.add(file.getPath());
    if(file.getSecondaryFiles()!=null)
      for(FileValue f: file.getSecondaryFiles()) {
        extractPathsFromFileValue(paths, f);
      }
  }

  public void addOrIncrement(UUID rootId, FileValue file, Integer usage) {
    Set<String> paths = new HashSet<String>();
    extractPathsFromFileValue(paths, file);
    for(String path: paths) {
        intermediaryFilesRepository.increment(rootId, path);
    }
  }
  
  protected Set<String> getUnusedFiles(UUID rootId) {
    List<IntermediaryFileEntity> filesForRootIdList = intermediaryFilesRepository.get(rootId);
    Map<String, Integer> filesForRootId = convertToMap(filesForRootIdList);
    Set<String> unusedFiles = new HashSet<String>();
    for(Iterator<Map.Entry<String, Integer>> it = filesForRootId.entrySet().iterator(); it.hasNext();) {
      Entry<String, Integer> entry = it.next();
      if(entry.getValue() <= 0) {
        unusedFiles.add(entry.getKey());
        intermediaryFilesRepository.delete(rootId, entry.getKey());
        it.remove();
      }
    }
    return unusedFiles;
  }

  @Override
  public void increment(UUID rootId, Object o) {
    Set<FileValue> files = new HashSet<FileValue>(FileValueHelper.getFilesFromValue(o));
    for(FileValue file: files){
      addOrIncrement(rootId, file, 1);
    }
  }

  @Override
  public void decrement(UUID rootId, Object o) {
    List<FileValue> filesFromValue = FileValueHelper.getFilesFromValue(o);
    Set<String> paths = new HashSet<>();
    filesFromValue.stream().forEach(f->{
      paths.add(f.getPath());
      f.getSecondaryFiles().stream().forEach(s->paths.add(s.getPath()));
    });
    decrementFiles(rootId, paths);
  }

  @Override
  public void handleJobDeleted(Job job) {
    decrement(job.getRootId(), job.getOutputs());
  }

}