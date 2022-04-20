package com.demo.spring_cache_ignite.controller;

import com.demo.spring_cache_ignite.model.Person;
import com.demo.spring_cache_ignite.repository.PersonRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Slf4j
@RestController
@RequestMapping("/person")
public class PersonController {

    @Autowired
    PersonRepository repository;

    @PostMapping
    public Person add(@RequestBody Person person) {
        person.init();
        return repository.createPerson(person);
    }

    @PutMapping
    public Person update(@RequestBody Person person) {
        return repository.createPerson(person);
    }

    @GetMapping("/{id}")
    public Person findById(@PathVariable("id") Long id) {
        return repository.findById(id);
    }

    @GetMapping("/{firstName}/{lastName}")
    public List<Person> findByName(@PathVariable("firstName") String firstName,
                                   @PathVariable("lastName") String lastName) {
        return repository.findByFirstNameAndLastName(firstName, lastName);
    }

    @GetMapping("/all")
    public List<Person> findAll() {
        return repository.getPersonList();
    }

    @GetMapping("/search/{text}")
    @ResponseStatus(HttpStatus.OK)
    public List<Person> searchPerson(@PathVariable("text") String text) {
        return repository.textSearch(text);
    }

    @GetMapping("/fuzzy/search/{text}")
    @ResponseStatus(HttpStatus.OK)
    public List<Person> fuzzySearchPerson(@PathVariable("text") String text) {
        return repository.fuzzySearch(text);
    }

    @GetMapping("/fuzzy/search/{text}/{fieldName}")
    @ResponseStatus(HttpStatus.OK)
    public List<Person> fuzzySearchPerson(@PathVariable("text") String text, @PathVariable("fieldName") String fieldName) {
        return repository.fuzzySearchOnSpecificField(text, fieldName);
    }

    @GetMapping("/scan/search/{text}")
    @ResponseStatus(HttpStatus.OK)
    public List<Person> scanQuerySearchPerson(@PathVariable("text") String text) {
        return repository.scanQuerySearch(text);
    }

//    @GetMapping("/sql/search/firstName/{text}")
//    @ResponseStatus(HttpStatus.OK)
//    public List<Person> sqlQuerySearchPersonByFirstName(@PathVariable("text") String text) {
//        return repository.sqlQuerySearchPersonFirstName(text);
//    }
//
//    @GetMapping("/sql/search/lastName/{text}")
//    @ResponseStatus(HttpStatus.OK)
//    public List<Person> sqlQuerySearchPersonByLastName(@PathVariable("text") String text) {
//        return repository.sqlQuerySearchPersonLastName(text);
//    }
}
