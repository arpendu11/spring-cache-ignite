package com.demo.spring_cache_ignite.repository;

import com.demo.spring_cache_ignite.model.Contact;
import com.demo.spring_cache_ignite.repository.jdbc.CacheJdbcStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class ContactRepository {

    @Autowired
    private CacheJdbcStore cacheJdbcStore;

    public Contact createContact(Contact contact) {
        return cacheJdbcStore.createContact(contact);
    }

    public Contact getContact(Long id) {
        return cacheJdbcStore.getContact(id);
    }

    public Contact findByLocation(String location) {
        return cacheJdbcStore.findByLocation(location);
    }

    public List<Contact> getContactList() {
        return cacheJdbcStore.getContactList();
    }
}
