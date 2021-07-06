package org.apache.bookkeeper.meta;

import static org.mockito.Mockito.mock;

import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.discover.RegistrationClient;
import org.apache.bookkeeper.meta.MetadataClientDriver.SessionStateListener;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.stats.StatsLogger;

public abstract class ClientDriverMock implements MetadataClientDriver {

    @Override
    public MetadataClientDriver initialize(ClientConfiguration conf, ScheduledExecutorService scheduler,
            StatsLogger statsLogger, Optional<Object> ctx) throws MetadataException {
        return this;
    }

    @Override
    public RegistrationClient getRegistrationClient() {
        return mock(RegistrationClient.class);
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

    @Override
    public void setSessionStateListener(SessionStateListener sessionStateListener) {
    }
}

