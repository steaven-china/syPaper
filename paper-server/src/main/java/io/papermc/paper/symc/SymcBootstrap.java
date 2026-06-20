package io.papermc.paper.symc;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Symc 运行时主入口 — 自建多线程实例,跟 Paper EventBus 解耦后跑。
 *
 * <p>每个 region pod 起一个 SymcBootstrap 实例:
 * <pre>
 *   Paper startup
 *     ↓
 *   SymcBootstrap.start(regionId)
 *     ↓
 *   ScheduledExecutorService (4 daemon threads, "symc-worker-N")
 *     ↓
 *   3 个 symc component 共享线程池,各自独立 queue
 *     ↓
 *   Bukkit @EventHandler 收到后 submit() 异步处理(不阻塞主 tick)
 * </pre>
 *
 * <p>设计:
 * <ul>
 *   <li>单例(每个 region pod 一份),线程池 daemon 化,Paper 关闭时不阻塞</li>
 *   <li>4 线程:WriteAuthority / Cooperation / AntiCheat / scheduled-tasks</li>
 *   <li>Bukkit @EventHandler 保留(钩入点不变),但处理逻辑全异步</li>
 *   <li>支持热重启:stop() 后再 start() 重建</li>
 * </ul>
 */
public final class SymcBootstrap {
    private static final Logger LOG = Logger.getLogger(SymcBootstrap.class.getName());
    private static volatile SymcBootstrap INSTANCE;

    /** 4 线程池:WriteAuthority / Cooperation / AntiCheat / scheduled tasks */
    private final ScheduledExecutorService scheduler;

    private final SymcWriteAuthorityManager writeAuthority;
    private final SymcCooperationRequest cooperation;
    private final SymcAntiCheatHook anticheat;
    private final String regionId;

    private SymcBootstrap(String regionId) {
        this.regionId = regionId;
        this.scheduler = Executors.newScheduledThreadPool(4, r -> {
            Thread t = new Thread(r, "symc-worker-" + regionId);
            t.setDaemon(true); // Paper 关闭时不阻塞 JVM 退出
            return t;
        });
        this.writeAuthority = new SymcWriteAuthorityManager(regionId, scheduler);
        this.cooperation = new SymcCooperationRequest(regionId, scheduler);
        this.anticheat = new SymcAntiCheatHook(regionId, scheduler);
        LOG.info("[symc] runtime started region=" + regionId + " threads=4");
    }

    public static SymcBootstrap start(String regionId) {
        if (INSTANCE != null) {
            throw new IllegalStateException("[symc] already started, call stop() first");
        }
        INSTANCE = new SymcBootstrap(regionId);
        return INSTANCE;
    }

    public static void stop() {
        SymcBootstrap old = INSTANCE;
        if (old != null) {
            old.scheduler.shutdownNow();
            try {
                if (!old.scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    LOG.warning("[symc] scheduler did not terminate in 5s");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            INSTANCE = null;
            LOG.info("[symc] runtime stopped region=" + old.regionId);
        }
    }

    public static SymcBootstrap get() {
        SymcBootstrap b = INSTANCE;
        if (b == null) {
            throw new IllegalStateException("[symc] not started, call start() first");
        }
        return b;
    }

    public static boolean isRunning() {
        return INSTANCE != null;
    }

    public SymcWriteAuthorityManager writeAuthority() { return writeAuthority; }
    public SymcCooperationRequest cooperation() { return cooperation; }
    public SymcAntiCheatHook anticheat() { return anticheat; }
    public ScheduledExecutorService scheduler() { return scheduler; }
    public String regionId() { return regionId; }
}
