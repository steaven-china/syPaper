package io.papermc.paper.symc;

import org.bukkit.Chunk;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.world.ChunkLoadEvent;
import org.bukkit.event.world.ChunkUnloadEvent;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * D2 写权漂移管理器 — chunk 单写权 + cold/hot 副本 + 双写过渡。
 *
 * <p>由 {@link SymcBootstrap} 构造,接收调度线程池。
 * @EventHandler 保留(钩入点不变),但处理逻辑全部 submit() 到线程池异步执行。
 */
public final class SymcWriteAuthorityManager implements Listener {

    private static final Logger LOG = Logger.getLogger(SymcWriteAuthorityManager.class.getName());

    private final Map<Long, AuthorityState> authorityMap = new ConcurrentHashMap<>();
    private final Set<Long> bufferZoneChunks = ConcurrentHashMap.newKeySet();
    private final String regionId;
    private final ScheduledExecutorService scheduler;

    public SymcWriteAuthorityManager(@NotNull String regionId,
                                     @NotNull ScheduledExecutorService scheduler) {
        this.regionId = regionId;
        this.scheduler = scheduler;
        LOG.info("[symc] WriteAuthorityManager started for region=" + regionId);
    }

    @EventHandler
    public void onChunkLoad(@NotNull ChunkLoadEvent event) {
        // 主线程接 event → submit 到线程池,异步处理
        // 不阻塞 Paper tick 线程
        final int cx = event.getChunk().getX();
        final int cz = event.getChunk().getZ();
        scheduler.submit(() -> handleChunkLoadAsync(cx, cz));
    }

    @EventHandler
    public void onChunkUnload(@NotNull ChunkUnloadEvent event) {
        final int cx = event.getChunk().getX();
        final int cz = event.getChunk().getZ();
        scheduler.submit(() -> handleChunkUnloadAsync(cx, cz));
    }

    // ---- 异步处理(在线程池跑) ----

    private void handleChunkLoadAsync(int cx, int cz) {
        long key = chunkKey(cx, cz);
        if (bufferZoneChunks.contains(key)) {
            authorityMap.put(key, AuthorityState.HOT_REPLICA);
            LOG.fine("[symc] chunk " + key + " → HOT_REPLICA (buffer zone)");
            return;
        }
        boolean gotAuthority = tryAcquireWriteAuthority(cx, cz);
        authorityMap.put(key, gotAuthority ? AuthorityState.PRIMARY : AuthorityState.HOT_REPLICA);
        LOG.fine("[symc] chunk " + key + " → " + authorityMap.get(key));
    }

    private void handleChunkUnloadAsync(int cx, int cz) {
        long key = chunkKey(cx, cz);
        AuthorityState removed = authorityMap.remove(key);
        if (removed == AuthorityState.PRIMARY) {
            releaseWriteAuthority(cx, cz);
        }
        LOG.fine("[symc] chunk " + key + " unloaded (was " + removed + ")");
    }

    // TODO M7: 真实网络 sync
    private boolean tryAcquireWriteAuthority(int cx, int cz) { return true; }
    private void releaseWriteAuthority(int cx, int cz) {}

    public void markBufferZone(int cx, int cz, boolean inBuffer) {
        long key = chunkKey(cx, cz);
        if (inBuffer) bufferZoneChunks.add(key); else bufferZoneChunks.remove(key);
    }

    public AuthorityState getAuthorityState(int cx, int cz) {
        return authorityMap.getOrDefault(chunkKey(cx, cz), AuthorityState.COLD);
    }

    /** 调度一个 1Hz 周期任务(温场刷新 / drift 检查) */
    public ScheduledFuture<?> schedulePeriodic(Runnable task, long periodMs) {
        return scheduler.scheduleAtFixedRate(task, periodMs, periodMs, TimeUnit.MILLISECONDS);
    }

    static long chunkKey(int cx, int cz) { return ((long)cx << 32) | ((long)cz & 0xFFFFFFFFL); }

    public enum AuthorityState { COLD, HOT_REPLICA, PRIMARY }
}
