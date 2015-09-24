package com.netflix.aegisthus.util;

import com.google.common.base.Throwables;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class JobKiller extends Thread {
    private static final Logger log = LoggerFactory.getLogger(JobKiller.class);
    private final Job job;

    public JobKiller(Job job) {
        this.job = job;
    }

    @Override
    public void run() {
        try {
            if (job.getJobState() == JobStatus.State.RUNNING) {
                job.killJob();
            } else {
                log.info("Job {} was already killed.", job.getJobID());
            }
        } catch (IOException | InterruptedException e) {
            log.error("Error killing job {}", job.getJobID(), e);
            Throwables.propagate(e);
        }
    }
}
