package com.spring.batch.config;

import com.spring.batch.entity.Customer;
import com.spring.batch.listener.StepSkipListener;
import com.spring.batch.repository.CustomerRepository;
import lombok.AllArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.SkipListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;

import java.io.File;

@Configuration
@AllArgsConstructor
@EnableBatchProcessing
public class SpringBatchConfig
{
    private final JobBuilderFactory jobBuilderFactory;

    private final StepBuilderFactory stepBuilderFactory;

    private final CustomerRepository customerRepository;

    private final CustomerItemWriter customerItemWriter;

    @Bean
    @StepScope
    public FlatFileItemReader<Customer> itemReader(@Value("#{jobParameters[fullPathFileName]}") String pathToFile)
    {
        FlatFileItemReader<Customer> itemReader = new FlatFileItemReader();

        itemReader.setResource(new FileSystemResource(new File(pathToFile))); // No need to hardcode the file path now
        itemReader.setName("csvReader");
        itemReader.setLinesToSkip(1);
        itemReader.setLineMapper(lineMapper());

        return itemReader;
    }

    private LineMapper<Customer> lineMapper()
    {
        DefaultLineMapper<Customer> lineMapper = new DefaultLineMapper<>();

        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames("id", "firstName", "lastName", "email", "gender", "contactNo", "country", "dob", "age");

        //this will map the csv file to the customer object
        BeanWrapperFieldSetMapper<Customer> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Customer.class);

        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);

        return lineMapper;
    }

    @Bean
    public CustomerProcessor processor()
    {
        return new CustomerProcessor();
    }

    @Bean
    public RepositoryItemWriter<Customer> writer()
    {
        RepositoryItemWriter<Customer> writer = new RepositoryItemWriter<>();
        writer.setRepository(customerRepository);
        writer.setMethodName("save");
        return writer;
    }

    @Bean
    public Step step1(FlatFileItemReader<Customer> itemReader)
    {
        return stepBuilderFactory.get("csv-step").<Customer, Customer>chunk(10)
                .reader(itemReader)
                .processor(processor())
                .writer(writer())
                .faultTolerant()
                .listener(skipListener())
                .taskExecutor(taskExecutor())
                .build();
    }

    @Bean
    public Job runJob(FlatFileItemReader<Customer> itemReader)
    {
        return jobBuilderFactory
                .get("importCustomer")
                .flow(step1(itemReader)) //job can have multiple steps using .next()
                .end()
                .build();
    }

    @Bean
    public TaskExecutor taskExecutor()
    {
        SimpleAsyncTaskExecutor asyncTaskExecutor = new SimpleAsyncTaskExecutor();
        asyncTaskExecutor.setConcurrencyLimit(10); // 10 thread will run concurrently

        return asyncTaskExecutor;
    }

    @Bean
    public SkipPolicy skipPolicy()
    {
        return new ExceptionSkipPolicy();
    }

    @Bean
    public SkipListener skipListener()
    {
        return new StepSkipListener();
    }
}
