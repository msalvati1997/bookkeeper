package org.apache.bookkeeper.meta;

import static org.mockito.Mockito.mock;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.discover.RegistrationManager.RegistrationListener;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.stats.StatsLogger;

public abstract class BookieDriverMock implements MetadataBookieDriver {
    @Override
    public MetadataBookieDriver initialize(ServerConfiguration conf, RegistrationListener listener,
            StatsLogger statsLogger) throws MetadataException {
        return this;
    }

    @Override
    public RegistrationManager getRegistrationManager() {
        return mock(RegistrationManager.class);
    }

    @Override
    public LedgerManagerFactory getLedgerManagerFactory() throws MetadataException {
        return mock(LedgerManagerFactory.class);
    }

    @Override
    public LayoutManager getLayoutManager() {
        return mock(LayoutManager.class);
    }

    @Override
    public void close() {

    }
}


