package com.demo.spring_cache_ignite.init;

import com.demo.spring_cache_ignite.model.Contact;
import com.demo.spring_cache_ignite.model.Person;
import com.demo.spring_cache_ignite.repository.ContactRepository;
import com.demo.spring_cache_ignite.repository.PersonRepository;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Objects;

@Slf4j
@Component
public class AppInit implements ApplicationListener<ApplicationReadyEvent> {

    @Autowired
    private PersonRepository personRepo;

    @Autowired
    private ContactRepository contactRepo;

    @Override
    public void onApplicationEvent(ApplicationReadyEvent applicationReadyEvent) {
        log.info("Application started");
        ObjectMapper mapper = new ObjectMapper();
        TypeReference<List<Person>> typeReferencePerson = new TypeReference<List<Person>>(){};
        InputStream inputStreamPerson = TypeReference.class.getResourceAsStream("/json/person.json");
        try {
            List<Person> personList = mapper.readValue(inputStreamPerson, typeReferencePerson);
            personList.forEach(p -> {
                Person person = personRepo.findById(p.getId());
                if (Objects.isNull(person)) {
                    personRepo.createPerson(p);
                }
            });
            log.info("Person Saved!");
        } catch (IOException e){
            log.error("Unable to save Person: " + e.getMessage());
        }

        TypeReference<List<Contact>> typeReferenceContact = new TypeReference<List<Contact>>(){};
        InputStream inputStreamContact = TypeReference.class.getResourceAsStream("/json/contact.json");
        try {
            List<Contact> contactList = mapper.readValue(inputStreamContact, typeReferenceContact);
            contactList.forEach(c -> {
                Contact contact = contactRepo.getContact(c.getId());
                if (Objects.isNull(contact)) {
                    contactRepo.createContact(c);
                }
            });
            log.info("Contact Saved!");
        } catch (IOException e){
            log.error("Unable to dispatch transactions to Kafka Topic: " + e.getMessage());
        }
    }
}
