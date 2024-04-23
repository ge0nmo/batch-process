package com.spring.batch.config;

import com.spring.batch.entity.Customer;
import com.spring.batch.repository.CustomerRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

@Slf4j
@RequiredArgsConstructor
@Component
public class CustomerItemWriter implements ItemWriter<Customer>
{
    private final CustomerRepository customerRepository;


    @Override
    public void write(Chunk<? extends Customer> chunk) throws Exception
    {
        log.info("Writer Thread = {}", Thread.currentThread().getName());
        customerRepository.saveAll(chunk);
    }
}
