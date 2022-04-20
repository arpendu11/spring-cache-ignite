package com.demo.spring_cache_ignite.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.ignite.cache.query.annotations.QueryGroupIndex;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.query.annotations.QueryTextField;

import javax.persistence.*;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@ToString
@NoArgsConstructor
@AllArgsConstructor
@Data
@Table(name="PERSON",
        uniqueConstraints=@UniqueConstraint(columnNames={"ID"}))
@QueryGroupIndex.List(@QueryGroupIndex(name="person_idx"))
public class Person implements Serializable {

    private static final AtomicLong ID_GEN = new AtomicLong();

    @Id
    @Column(name = "ID")
    @QuerySqlField(index = true)
    private Long id;

    @Column(name = "FIRST_NAME")
    @JsonProperty("first_name")
    @QueryTextField
    @QuerySqlField(index = true)
    @QuerySqlField.Group(name = "person_idx", order = 0)
    private String firstName;

    @Column(name = "LAST_NAME")
    @JsonProperty("last_name")
    @QueryTextField
    @QuerySqlField(index = true)
    @QuerySqlField.Group(name = "person_idx", order = 1)
    private String lastName;

    @Column(name = "GENDER")
    private Gender gender;

    @Column(name = "BIRTH_DATE")
    @JsonProperty("birth_date")
    private String birthDate;

    @Column(name = "COUNTRY")
    private String country;

    @Column(name = "CITY")
    private String city;

    @Column(name = "ADDRESS")
    private String address;

    @Embedded
    private List<Contact> contacts = new ArrayList<>();

    public void init() {
        this.id = ID_GEN.incrementAndGet();
    }
}
