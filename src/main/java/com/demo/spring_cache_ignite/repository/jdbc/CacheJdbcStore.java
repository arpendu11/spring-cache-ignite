package com.demo.spring_cache_ignite.repository.jdbc;

import com.demo.spring_cache_ignite.config.factory.MyCacheStoreSessionListenerFactory;
import com.demo.spring_cache_ignite.constant.IgniteConstants;
import com.demo.spring_cache_ignite.model.Contact;
import com.demo.spring_cache_ignite.model.Gender;
import com.demo.spring_cache_ignite.model.Person;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.transactions.Transaction;
import org.springframework.stereotype.Component;

import javax.cache.Cache;
import javax.cache.configuration.FactoryBuilder;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Component
public class CacheJdbcStore {

    private final Ignite ignite = Ignition.start(IgniteConstants.IGNITE_CONFIG_XML_ABSOLUTE_PATH);
    private final IgniteCache<Long, Contact> contactIgniteCache;
    private final IgniteCache<Long, Person> personIgniteCache;

    public CacheJdbcStore() throws IOException {
        ignite.cluster().active(true);
        Ignition.setClientMode(true);

        CacheConfiguration<Long, Contact> contactCache = new CacheConfiguration<>("ContactCache");
        contactCache.setIndexedTypes(Long.class, Contact.class);
        contactCache.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        contactCache.setCacheMode(CacheMode.REPLICATED);
        contactCache.setWriteBehindEnabled(true);
        contactCache.setReadThrough(true);
        contactCache.setWriteThrough(true);
        contactCache.setStatisticsEnabled(true);
        contactCache.setSqlSchema("PUBLIC");
        contactCache.setCacheStoreFactory(FactoryBuilder.factoryOf(CacheJdbcContactStore.class));
        contactCache.setCacheStoreSessionListenerFactories(new MyCacheStoreSessionListenerFactory());
        contactIgniteCache = ignite.getOrCreateCache(contactCache);
        contactIgniteCache.loadCache(null);

        CacheConfiguration<Long, Person> personCache = new CacheConfiguration<>("PersonCache");
        personCache.setIndexedTypes(Long.class, Person.class);
        personCache.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        personCache.setCacheMode(CacheMode.REPLICATED);
        personCache.setWriteBehindEnabled(true);
        personCache.setReadThrough(true);
        personCache.setWriteThrough(true);
        personCache.setStatisticsEnabled(true);
        personCache.setSqlSchema("PUBLIC");
        personCache.setCacheStoreFactory(FactoryBuilder.factoryOf(CacheJdbcPersonStore.class));
        personCache.setCacheStoreSessionListenerFactories(new MyCacheStoreSessionListenerFactory());
        QueryEntity qryEntity = new QueryEntity();
        qryEntity.setKeyType(Integer.class.getName());
        qryEntity.setValueType(Person.class.getName());
        qryEntity.setKeyFieldName("id");
        Set<String> keyFields = new HashSet<>();
        keyFields.add("id");
        qryEntity.setKeyFields(keyFields);
        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put("id", "java.lang.Integer");
        fields.put("first_name", "java.lang.String");
        fields.put("last_name", "java.lang.String");
        qryEntity.setFields(fields);
        personCache.setQueryEntities(Collections.singletonList(qryEntity));
        personIgniteCache = ignite.getOrCreateCache(personCache);
        personIgniteCache.loadCache(null);
    }

    public Contact createContact(Contact contact) {
        Contact result;

        try (Transaction tx = Ignition.ignite().transactions().txStart()) {
            result = contactIgniteCache.getAndPut(contact.getId(), contact);
            tx.commit();
        }

        return result;
    }

    public Contact getContact(Long id) {
        Contact result;

        try (Transaction tx = Ignition.ignite().transactions().txStart()) {
            result = contactIgniteCache.get(id);
            tx.commit();
        }

        return result;
    }

    public Contact findByLocation(String location) {

        TextQuery<Long, Contact> textQuery = new TextQuery<>(Contact.class, "location:" + location);
        try (QueryCursor<Cache.Entry<Long, Contact>> cursor = contactIgniteCache.query(textQuery)) {
            for (Cache.Entry<Long, Contact> e : cursor)
                return e.getValue();
        }

        return null;
    }

    public List<Contact> getContactList() {
        List<Contact> result = new ArrayList<>();

        SqlQuery<Long, Contact> sql = new SqlQuery(Contact.class, "id IS NOT NULL");
        try (QueryCursor<Cache.Entry<Long, Contact>> cursor = contactIgniteCache.query(sql)) {
            for (Cache.Entry<Long, Contact> e : cursor)
                result.add(e.getValue());
        }

        return result;
    }

    public Person createPerson(Person person) {
        Person result;

        try (Transaction tx = Ignition.ignite().transactions().txStart()) {
            result = personIgniteCache.getAndPut(person.getId(), person);
            tx.commit();
        }

        return result;
    }

    public Person getPerson(Long id) {
        Person result;

        try (Transaction tx = Ignition.ignite().transactions().txStart()) {
            result = personIgniteCache.get(id);
            tx.commit();
        }

        return result;
    }

    public List<Person> findByFirstNameAndLastName(String firstName, String lastName) {
        List<Person> result = new ArrayList<>();

        ScanQuery<Long, Person> scanQuery = new ScanQuery<>((k, v) ->
                v.getFirstName().equalsIgnoreCase(firstName)
                        && v.getLastName().equalsIgnoreCase(lastName));
        try (QueryCursor<Cache.Entry<Long, Person>> cursor = personIgniteCache.query(scanQuery)) {
            for (Cache.Entry<Long, Person> e : cursor)
                result.add(e.getValue());
        }

        return result;
    }

    public List<Person> getPersonList() {
        List<Person> result = new ArrayList<>();

        SqlQuery<Long, Person> sql = new SqlQuery(Person.class, "id IS NOT NULL");
        try (QueryCursor<Cache.Entry<Long, Person>> cursor = personIgniteCache.query(sql)) {
            for (Cache.Entry<Long, Person> e : cursor)
                result.add(e.getValue());
        }

        return result;
    }

    public List<Person> textSearch(String text) {
        log.info("Search the index to find person where either first name or last name contains the text: {}", text);
        TextQuery<Long, Person> textQuery = new TextQuery<>(Person.class, text);
        return searchPerson(textQuery);
    }

    public List<Person> fuzzySearch(String text) {
        log.info("Fuzzy search on index to find person where either first name or last name contains the text: {}", text);
        TextQuery<Long, Person> textQuery = new TextQuery<>(Person.class, text + "~");
        return searchPerson(textQuery);
    }

    public List<Person> fuzzySearchOnSpecificField(String text, String fieldName) {
        log.info("Fuzzy search on index to find person with the given field: {} and text: {}", fieldName, text);
        TextQuery<Long, Person> textQuery = new TextQuery<>(Person.class, fieldName + ":" + text + "~");
        return searchPerson(textQuery);
    }

    public List<Person> searchPerson(TextQuery<Long, Person> textQuery) {
        List<Person> personList = new ArrayList<>();
        log.info("Query :: " + textQuery.getText());
        try (QueryCursor<Cache.Entry<Long, Person>> cursor = personIgniteCache.query(textQuery)) {
            for (Cache.Entry<Long, Person> entry : cursor) {
                personList.add(entry.getValue());
            }
        }
        return personList;
    }

    public List<Person> scanQuerySearch(String text) {
        log.info("ScanQuery search to find person whose first name contain the given text: {}", text);
        ScanQuery<Long, Person> scanQuery = new ScanQuery<>((k, v) -> v.getFirstName().equalsIgnoreCase(text));
        List<Person> players = new ArrayList<>();
        log.info("Query :: " + scanQuery);
        try (QueryCursor<Cache.Entry<Long, Person>> cursor = personIgniteCache.query(scanQuery)) {
            for (Cache.Entry<Long, Person> entry : cursor) {
                players.add(entry.getValue());
            }
        }
        return players;
    }

    public List<Person> sqlQuerySearchPersonFirstName(String text) {
        log.info("SqlQuery search to find person whose first name contain the given text: {}", text);
        Collection<QueryEntity> queryEntities = personIgniteCache.getConfiguration(CacheConfiguration.class).getQueryEntities();
        String tableName = queryEntities.stream().findFirst().orElseThrow(() ->
                new IllegalArgumentException("PersonCache doesn't exists")).getTableName();
        SqlFieldsQuery sqlQuery = new SqlFieldsQuery("select * from " + tableName.toLowerCase() + " where first_name='" + text + "'");
        log.info("Query :: " + sqlQuery.getSql());
        List<List<?>> records = personIgniteCache.query(sqlQuery).getAll();
        return records.stream().map(objects -> {
            log.info("objects retrieved from Cache: {}", objects);
            Person person = new Person();
            person.setId((Long) objects.get(0));
            person.setFirstName(String.valueOf(objects.get(1)));
            person.setLastName(String.valueOf(objects.get(2)));
            person.setGender(Gender.valueOf(String.valueOf(objects.get(3))));
            person.setBirthDate(String.valueOf(objects.get(4)));
            person.setCountry(String.valueOf(objects.get(5)));
            person.setCity(String.valueOf(objects.get(6)));
            person.setAddress(String.valueOf(objects.get(7)));
            return person;
        }).collect(Collectors.toList());
    }

    public List<Person> sqlQuerySearchPersonLastName(String text) {
        log.info("SqlQuery search to find person whose last name contain the given text: {}", text);
        Collection<QueryEntity> queryEntities = personIgniteCache.getConfiguration(CacheConfiguration.class).getQueryEntities();
        String tableName = queryEntities.stream().findFirst().orElseThrow(() ->
                new IllegalArgumentException("PersonCache doesn't exists")).getTableName();
        SqlFieldsQuery sqlQuery = new SqlFieldsQuery("select * from " + tableName.toLowerCase() + " where last_name='" + text + "'");
        log.info("Query :: " + sqlQuery.getSql());
        List<List<?>> records = personIgniteCache.query(sqlQuery).getAll();
        return records.stream().map(objects -> {
            log.info("objects retrieved from Cache: {}", objects);
            Person person = new Person();
            person.setId((Long) objects.get(0));
            person.setFirstName(String.valueOf(objects.get(1)));
            person.setLastName(String.valueOf(objects.get(2)));
            person.setGender(Gender.valueOf(String.valueOf(objects.get(3))));
            person.setBirthDate(String.valueOf(objects.get(4)));
            person.setCountry(String.valueOf(objects.get(5)));
            person.setCity(String.valueOf(objects.get(6)));
            person.setAddress(String.valueOf(objects.get(7)));
            return person;
        }).collect(Collectors.toList());
    }

    /**
     * Compute Grid Stuff (Only here for Demo!)
     */
    public Double calculateAverge(List<List<Double>> numLists) {
        Collection<Double> distributedAverage = ignite.compute().apply(
                (IgniteClosure<List<Double>, Double>) numList -> {
                    Double sum = 0.0;
                    Double count = (double) numList.size();
                    double average = 0.0;
                    StringBuilder numListString = new StringBuilder();

                    for (Double num : numList) {
                        numListString.append(String.valueOf(num));
                        numListString.append(" ");
                        sum += num;
                    }

                    average = sum / count;

                    log.info(">>> Average of: {}", numListString.toString());
                    log.info(">>>  is: {}", average);

                    return average;
                },

                // Job parameters. Ignite will create as many jobs as there are parameters.
                numLists
        );

        Double avgSum = 0.0;
        Double count = (double) numLists.size();

        // Add up individual averages received from remote nodes
        for (Double avg : distributedAverage) {
            avgSum += avg;
        }

        log.info(">>> Avg Sum: {}; Avg Count: {}", avgSum, count);

        return avgSum / count;
    }
}
