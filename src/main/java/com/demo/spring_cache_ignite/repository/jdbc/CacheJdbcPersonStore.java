package com.demo.spring_cache_ignite.repository.jdbc;

import com.demo.spring_cache_ignite.model.Contact;
import com.demo.spring_cache_ignite.model.ContactType;
import com.demo.spring_cache_ignite.model.Gender;
import com.demo.spring_cache_ignite.model.Person;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.cache.store.CacheStoreSession;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.resources.CacheStoreSessionResource;

import javax.cache.Cache;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Slf4j
public class CacheJdbcPersonStore extends CacheStoreAdapter<Long, Person> {

    /**
     * Store session.
     */
    @CacheStoreSessionResource
    private CacheStoreSession ses;

    /**
     * {@inheritDoc}
     */
    @Override
    public Person load(Long key) {
        Connection conn = ses.attachment();

        try (PreparedStatement st = Objects.requireNonNull(conn).prepareStatement("select * from person where id = ?")) {
            st.setLong(1, key);

            ResultSet rs = st.executeQuery();

            if (rs.next()) {
                Contact contact = new Contact();

                try(PreparedStatement st1 = conn.prepareStatement("select * from contact where person_id = ? LIMIT 1")) {
                    st1.setLong(1, key);

                    ResultSet rs1 = st1.executeQuery();

                    while (rs1.next()) {
                        contact.setId(rs1.getLong("id"));
                        contact.setType(ContactType.valueOf(rs1.getString("type")));
                        contact.setLocation(rs1.getString("location"));
                        contact.setPersonId(rs1.getLong("person_id"));
                    }
                } catch (SQLException e) {
                    throw new CacheLoaderException("Failed to load object [key=" + key + ']', e);
                }


                Person person = new Person();
                person.setId(rs.getLong("id"));
                person.setFirstName(rs.getString("first_name"));
                person.setLastName(rs.getString("last_name"));
                person.setGender(Gender.valueOf(rs.getString("gender")));
                person.setBirthDate(rs.getString("birth_date"));
                person.setCountry(rs.getString("country"));
                person.setCity(rs.getString("city"));
                person.setAddress(rs.getString("address"));
                List<Contact> contacts = new ArrayList<>();
                contacts.add(contact);
                person.setContacts(contacts);

                return person;
            } else {
                return null;
            }
        } catch (SQLException e) {
            throw new CacheLoaderException("Failed to load object [key=" + key + ']', e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void write(Cache.Entry<? extends Long, ? extends Person> entry) {
        Long key = entry.getKey();
        Person val = entry.getValue();

        try {
            Connection conn = ses.attachment();

            int updated;

            // Try update first. If it does not work, then try insert.
            // Some databases would allow these to be done in one 'upsert' operation.
            try (PreparedStatement st = Objects.requireNonNull(conn).prepareStatement(
                    "update person set " +
                            "id = ?, " +
                            "first_name = ?, " +
                            "last_name = ?, " +
                            "gender = ?, " +
                            "birth_date = ?, " +
                            "country = ?, " +
                            "city = ?, " +
                            "address = ? " +
                            "where id = ?")) {
                st.setLong(1, val.getId());
                st.setString(2, val.getFirstName());
                st.setString(3, val.getLastName());
                st.setString(4, String.valueOf(val.getGender()));
                st.setString(5, val.getBirthDate());
                st.setString(6, val.getCountry());
                st.setString(7, val.getCity());
                st.setString(8, val.getAddress());
                st.setLong(9, val.getId());

                updated = st.executeUpdate();
            }

            // If update failed, try to insert.
            if (updated == 0) {
                try (PreparedStatement st = conn.prepareStatement(
                        "insert into person (" +
                                "id, " +
                                "first_name, " +
                                "last_name, " +
                                "gender, " +
                                "birth_date, " +
                                "country, " +
                                "city, " +
                                "address) " +
                                "values (?, ?, ?, ?, ?, ?, ?, ?)")) {
                    st.setLong(1, val.getId());
                    st.setString(2, val.getFirstName());
                    st.setString(3, val.getLastName());
                    st.setString(4, String.valueOf(val.getGender()));
                    st.setString(5, val.getBirthDate());
                    st.setString(6, val.getCountry());
                    st.setString(7, val.getCity());
                    st.setString(8, val.getAddress());

                    st.executeUpdate();
                }
            }
        } catch (SQLException e) {
            throw new CacheWriterException("Failed to write object [key=" + key + ", val=" + val + ']', e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(Object key) {
        Connection conn = ses.attachment();

        try (PreparedStatement st = Objects.requireNonNull(conn).prepareStatement("delete from person where id=?")) {
            st.setLong(1, (Long) key);

            st.executeUpdate();
        } catch (SQLException e) {
            throw new CacheWriterException("Failed to delete object [key=" + key + ']', e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void loadCache(IgniteBiInClosure<Long, Person> clo, Object... args) {
        Connection conn = ses.attachment();

        try (PreparedStatement stmt = Objects.requireNonNull(conn).prepareStatement("select * from person")) {

            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                Contact contact = new Contact();

                try(PreparedStatement st1 = conn.prepareStatement("select * from contact where person_id = ? LIMIT 1")) {
                    st1.setLong(1, rs.getLong("id"));

                    ResultSet rs1 = st1.executeQuery();

                    while (rs1.next()) {
                        contact.setId(rs1.getLong("id"));
                        contact.setType(ContactType.valueOf(rs1.getString("type")));
                        contact.setLocation(rs1.getString("location"));
                        contact.setPersonId(rs1.getLong("person_id"));
                    }
                } catch (SQLException e) {
                    throw new CacheLoaderException("Failed to load object [key=" + rs.getLong("id") + ']', e);
                }


                Person person = new Person();
                person.setId(rs.getLong("id"));
                person.setFirstName(rs.getString("first_name"));
                person.setLastName(rs.getString("last_name"));
                person.setGender(Gender.valueOf(rs.getString("gender")));
                person.setBirthDate(rs.getString("birth_date"));
                person.setCountry(rs.getString("country"));
                person.setCity(rs.getString("city"));
                person.setAddress(rs.getString("address"));
                List<Contact> contacts = new ArrayList<>();
                contacts.add(contact);
                person.setContacts(contacts);

                clo.apply(person.getId(), person);
            }
        } catch (SQLException e) {
            throw new CacheLoaderException("Failed to load values from cache store.", e);
        }
    }
}
