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
import java.util.logging.Logger;

/**
 * D2 写权漂移管理器 — chunk 单写权 + cold/hot 副本 + 双写过渡。
 */
public final class SymcWriteAuthorityManager implements Listener {

    private static final Logger LOG = Logger.getLogger(SymcWriteAuthorityManager.class.getName());
    private final Map<Long, AuthorityState> authorityMap = new ConcurrentHashMap<>();
    private final Set<Long> bufferZoneChunks = ConcurrentHashMap.newKeySet();
    private final String regionId;

    public SymcWriteAuthorityManager(@NotNull String regionId) {
        this.regionId = regionId;
        LOG.info("[symc] WriteAuthorityManager started for region=" + regionId);
    }

    @EventHandler
    public void onChunkLoad(@NotNull ChunkLoadEvent event) {
        Chunk chunk = event.getChunk();
        long key = chunkKey(chunk.getX(), chunk.getZ());
        if (bufferZoneChunks.contains(key)) {
            authorityMap.put(key, AuthorityState.HOT_REPLICA);
            return;
        }
        authorityMap.put(key, tryAcquireWriteAuthority(chunk.getX(), chunk.getZ())
                ? AuthorityState.PRIMARY : AuthorityState.HOT_REPLICA);
    }

    @EventHandler
    public void onChunkUnload(@NotNull ChunkUnloadEvent event) {
        Chunk chunk = event.getChunk();
        long key = chunkKey(chunk.getX(), chunk.getZ());
        if (authorityMap.remove(key) == AuthorityState.PRIMARY) {
            releaseWriteAuthority(chunk.getX(), chunk.getZ());
        }
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

    static long chunkKey(int cx, int cz) { return ((long)cx << 32) | ((long)cz & 0xFFFFFFFFL); }

    public enum AuthorityState { COLD, HOT_REPLICA, PRIMARY }
}
