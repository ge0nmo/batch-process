package com.spring.batch.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

@RequestMapping("/api/v1/jobs")
@RequiredArgsConstructor
@RestController
public class JobController
{
    private final JobLauncher jobLauncher;
    private final Job job;
    private final JobRepository jobRepository;

    private final String TEMP_STORAGE = "C:\\Users\\123\\Desktop\\기타\\batch-processing-demo\\src\\main\\resources\\";


    @PostMapping("/importData")
    public void importCsvToDbJob(@RequestParam("file") MultipartFile multipartFile)
    {
        // file -> path we don't know
        // copy the file to some storage in your VM : get the file path
        // or copy the file to DB : get the file path

        try {
            String originalFilename = multipartFile.getOriginalFilename();
            String filePath = TEMP_STORAGE + originalFilename;

            File fileToImport = new File(filePath);
            multipartFile.transferTo(fileToImport);

            JobParameters jobParameter = new JobParametersBuilder()
                    .addString("fullPathFileName", filePath)
                    .addLong("startAt", System.currentTimeMillis()).toJobParameters();

            JobExecution execution = jobLauncher.run(job, jobParameter);

            if(execution.getExitStatus().getExitCode().equals(ExitStatus.COMPLETED.getExitCode()))
            {
                // delete the file from the TEMP_STORAGE
                Files.deleteIfExists(Paths.get(filePath));
            }

        } catch (JobExecutionAlreadyRunningException | JobRestartException | JobInstanceAlreadyCompleteException | JobParametersInvalidException e) {
            e.printStackTrace();
        } catch (IOException e)
        {
            throw new RuntimeException(e);
        }

    }
}
