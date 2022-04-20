package com.demo.spring_cache_ignite.repository;

import com.demo.spring_cache_ignite.model.Person;
import com.demo.spring_cache_ignite.repository.jdbc.CacheJdbcStore;
import org.apache.ignite.cache.query.TextQuery;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class PersonRepository {

    @Autowired
    private CacheJdbcStore cacheJdbcStore;

    public Person createPerson(Person person) {
        return cacheJdbcStore.createPerson(person);
    }

    public Person findById(Long id) {
        return cacheJdbcStore.getPerson(id);
    }

    public List<Person> findByFirstNameAndLastName(String firstName, String lastName) {
        return cacheJdbcStore.findByFirstNameAndLastName(firstName, lastName);
    }

    public List<Person> getPersonList() {
        return cacheJdbcStore.getPersonList();
    }

    public List<Person> textSearch(String text) {
        return cacheJdbcStore.textSearch(text);
    }

    public List<Person> fuzzySearch(String text) {
        return cacheJdbcStore.fuzzySearch(text);
    }

    public List<Person> fuzzySearchOnSpecificField(String text, String fieldName) {
        return cacheJdbcStore.fuzzySearchOnSpecificField(text, fieldName);
    }

    public List<Person> scanQuerySearch(String text) {
        return cacheJdbcStore.scanQuerySearch(text);
    }

    public List<Person> sqlQuerySearchPersonFirstName(String text) {
        return cacheJdbcStore.sqlQuerySearchPersonFirstName(text);
    }

    public List<Person> sqlQuerySearchPersonLastName(String text) {
        return cacheJdbcStore.sqlQuerySearchPersonLastName(text);
    }
}