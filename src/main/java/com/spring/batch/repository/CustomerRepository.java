package com.spring.batch.repository;

import com.spring.batch.entity.Customer;
import org.springframework.data.jpa.repository.JpaRepository;

public interface CustomerRepository extends JpaRepository<Customer, Long>
{

}
