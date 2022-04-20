package com.demo.spring_cache_ignite.controller;

import com.demo.spring_cache_ignite.model.Contact;
import com.demo.spring_cache_ignite.repository.ContactRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Objects;

@RestController
@RequestMapping("/contact")
public class ContactController {

    @Autowired
    ContactRepository repository;

    @PostMapping
    public Contact add(@RequestBody Contact contact) {
        contact.init();
        Contact existingContact = repository.findByLocation(contact.getLocation());
        if (Objects.isNull(existingContact)) {
            return repository.createContact(contact);
        }
        return existingContact;
    }

    @GetMapping("/{id}")
    public Contact findById(@PathVariable("id") Long id) {
        return repository.getContact(id);
    }

    @GetMapping("/location/{location}")
    public Contact findByLocation(@PathVariable("location") String location) {
        return repository.findByLocation(location);
    }
}
