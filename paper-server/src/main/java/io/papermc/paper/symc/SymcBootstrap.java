package io.papermc.paper.symc;

import io.nats.client.Connection;
import io.nats.client.Nats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Symc 运行时主入口 — 自建多线程实例 + M7 真实网络 sync(NATS client)。
 *
 * <p>每个 region pod 起一个 SymcBootstrap:
 * <pre>
 *   SymcBootstrap.start("region-1", "nats://nats-server:4222")
 *     ↓
 *   - ScheduledExecutorService(4 daemon threads, "symc-worker-region-1")
 *   - io.nats.client.Connection(NATS pub/sub)
 *   - 3 个 symc component 共享两者
 * </pre>
 *
 * <p>NATS 关闭顺序: stop() → executor shutdown + nats connection close
 */
public final class SymcBootstrap {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(SymcBootstrap.class);

    private static volatile SymcBootstrap INSTANCE;

    private final ScheduledExecutorService scheduler;
    private final Connection nats;
    private final SymcWriteAuthorityManager writeAuthority;
    private final SymcCooperationRequest cooperation;
    private final SymcAntiCheatHook anticheat;
    private final String regionId;

    private SymcBootstrap(String regionId, String natsUrl) throws Exception {
        this.regionId = regionId;
        this.scheduler = Executors.newScheduledThreadPool(4, r -> {
            Thread t = new Thread(r, "symc-worker-" + regionId);
            t.setDaemon(true);
            return t;
        });
        // NATS connection(同步连,超时 5s)
        this.nats = Nats.connect(natsUrl);
        LOG.info("[symc] NATS connected: {}", natsUrl);

        this.writeAuthority = new SymcWriteAuthorityManager(regionId, scheduler);
        this.cooperation = new SymcCooperationRequest(regionId, scheduler, nats);
        this.anticheat = new SymcAntiCheatHook(regionId, scheduler);

        Runtime.getRuntime().addShutdownHook(new Thread(SymcBootstrap::stop, "symc-shutdown"));
        LOG.info("[symc] runtime started region={} threads=4 nats={}", regionId, natsUrl);
    }

    public static SymcBootstrap start(String regionId, String natsUrl) throws Exception {
        if (INSTANCE != null) {
            throw new IllegalStateException("[symc] already started, call stop() first");
        }
        INSTANCE = new SymcBootstrap(regionId, natsUrl);
        return INSTANCE;
    }

    public static void stop() {
        SymcBootstrap old = INSTANCE;
        if (old != null) {
            old.scheduler.shutdownNow();
            try {
                if (!old.scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    LOG.warn("[symc] scheduler did not terminate in 5s");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            try {
                old.nats.close();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            INSTANCE = null;
            LOG.info("[symc] runtime stopped region={}", old.regionId);
        }
    }

    public static SymcBootstrap get() {
        SymcBootstrap b = INSTANCE;
        if (b == null) throw new IllegalStateException("[symc] not started, call start() first");
        return b;
    }

    public static boolean isRunning() { return INSTANCE != null; }

    public SymcWriteAuthorityManager writeAuthority() { return writeAuthority; }
    public SymcCooperationRequest cooperation() { return cooperation; }
    public SymcAntiCheatHook anticheat() { return anticheat; }
    public ScheduledExecutorService scheduler() { return scheduler; }
    public Connection nats() { return nats; }
    public String regionId() { return regionId; }
}
