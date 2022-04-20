package com.demo.spring_cache_ignite.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.query.annotations.QueryTextField;

import javax.persistence.*;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

@ToString
@Data
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "CONTACT",
        uniqueConstraints=@UniqueConstraint(columnNames={"ID"}))
public class Contact implements Serializable {

    private static final AtomicLong ID_GEN = new AtomicLong();

    @Id
    @Column(name = "ID")
    @QuerySqlField(index = true)
    private Long id;

    @Column(name = "TYPE")
    @QuerySqlField(index = true)
    private ContactType type;

    @Column(name = "LOCATION")
    @QueryTextField
    @QuerySqlField(index = true)
    private String location;

    @Column(name = "PERSON_ID")
    @JsonProperty("person_id")
    @QuerySqlField(index = true)
    private Long personId;

    public void init() {
        this.id = ID_GEN.incrementAndGet();
    }
}
