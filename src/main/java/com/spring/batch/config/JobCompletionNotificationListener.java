package com.spring.batch.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;

@Slf4j
public class JobCompletionNotificationListener extends JobExecutionListenerSupport
{
    @Override
    public void afterJob(JobExecution jobExecution)
    {
        if(jobExecution.getStatus().equals(BatchStatus.COMPLETED))
            log.info("===Job completed====");
    }
}
