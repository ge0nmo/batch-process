package com.spring.batch.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spring.batch.entity.Customer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.SkipListener;

@Slf4j
public class StepSkipListener implements SkipListener<Customer, Number>
{
    @Override
    public void onSkipInRead(Throwable t)
    {
        log.info("A failure on read {}", t.getMessage());
    }

    @Override
    public void onSkipInWrite(Number item, Throwable t)
    {
        log.info("A failure on write {}", t.getMessage());
    }

    @SneakyThrows
    @Override
    public void onSkipInProcess(Customer item, Throwable t)
    {
        log.info("Item {} was skipped due to the exception {}", new ObjectMapper().writeValueAsString(item), t.getMessage());
    }
}
