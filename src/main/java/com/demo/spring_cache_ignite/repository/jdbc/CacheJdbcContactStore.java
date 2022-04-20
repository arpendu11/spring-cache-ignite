package com.demo.spring_cache_ignite.repository.jdbc;

import com.demo.spring_cache_ignite.model.Contact;
import com.demo.spring_cache_ignite.model.ContactType;
import org.apache.ignite.cache.store.CacheStore;
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

/**
 * Example of {@link CacheStore} implementation that uses JDBC
 * transaction with cache transactions and maps {@link Long} to {@link Contact}.
 */
public class CacheJdbcContactStore extends CacheStoreAdapter<Long, Contact> {

    /**
     * Store session.
     */
    @CacheStoreSessionResource
    private CacheStoreSession ses;

    /**
     * {@inheritDoc}
     */
    @Override
    public Contact load(Long key) {
        Connection conn = ses.attachment();

        try (PreparedStatement st = conn.prepareStatement("select * from contact where id = ?")) {
            st.setLong(1, key);

            ResultSet rs = st.executeQuery();

            if (rs.next()) {
                Contact contact = new Contact();
                contact.setId(rs.getLong("id"));
                contact.setType(ContactType.valueOf(rs.getString("type")));
                contact.setLocation(rs.getString("location"));
                contact.setPersonId(rs.getLong("person_id"));

                return contact;
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
    public void write(Cache.Entry<? extends Long, ? extends Contact> entry) {
        Long key = entry.getKey();
        Contact val = entry.getValue();

        try {
            Connection conn = ses.attachment();

            int updated;

            // Try update first. If it does not work, then try insert.
            // Some databases would allow these to be done in one 'upsert' operation.
            try (PreparedStatement st = conn.prepareStatement(
                    "update contact set " +
                            "id = ?, " +
                            "type = ?, " +
                            "location = ?, " +
                            "person_id = ? " +
                            "where id = ?")) {
                st.setLong(1, val.getId());
                st.setString(2, String.valueOf(val.getType()));
                st.setString(3, val.getLocation());
                st.setLong(4, val.getPersonId());
                st.setLong(5, val.getId());

                updated = st.executeUpdate();
            }

            // If update failed, try to insert.
            if (updated == 0) {
                try (PreparedStatement st = conn.prepareStatement(
                        "insert into contact (" +
                                "id, " +
                                "type, " +
                                "location, " +
                                "person_id) " +
                                "values (?, ?, ?, ?)")) {
                    st.setLong(1, val.getId());
                    st.setString(2, String.valueOf(val.getType()));
                    st.setString(3, val.getLocation());
                    st.setLong(4, val.getPersonId());

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

        try (PreparedStatement st = conn.prepareStatement("delete from contact where id=?")) {
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
    public void loadCache(IgniteBiInClosure<Long, Contact> clo, Object... args) {
        Connection conn = ses.attachment();

        try (PreparedStatement stmt = conn.prepareStatement("select * from contact")) {

            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                Contact contact = new Contact();
                contact.setId(rs.getLong("id"));
                contact.setType(ContactType.valueOf(rs.getString("type")));
                contact.setLocation(rs.getString("location"));
                contact.setPersonId(rs.getLong("person_id"));

                clo.apply(contact.getId(), contact);
            }
        } catch (SQLException e) {
            throw new CacheLoaderException("Failed to load values from cache store.", e);
        }
    }
}
