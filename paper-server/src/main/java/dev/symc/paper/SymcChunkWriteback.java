package dev.symc.paper;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import org.bukkit.Bukkit;
import org.bukkit.Chunk;
import org.bukkit.Material;
import org.bukkit.World;
import org.bukkit.block.Block;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.zip.GZIPInputStream;

/**
 * M12: Chunk write-back — region-b 收到 region-a 的 chunk publish 后,真正应用到本地 world。
 *
 * <p>M11 v5 已经做了 publish 端 + subscribe 端 + apply deferred placeholder。
 * M12 把 apply 补全:解析 NBT-like payload,遍历 blocks,主线程 setType。
 *
 * <p>设计要点:
 * <ul>
 *   <li>异步接收 NATS message(M11 dispatcher 线程),parse + 解压
 *   <li>不直接改 world:调度到主线程(Bukkit.getScheduler.runTask)
 *   <li>避免回环:applyDelta 不再 publish(在 M11 v5 publish 时已 skip own region)
 *   <li>chunk load 触发去重:同一 (world,cx,cz) 在 5s 内只 apply 一次
 * </ul>
 */
public final class SymcChunkWriteback {

    private static final Logger LOG = LoggerFactory.getLogger(SymcChunkWriteback.class);

    private final String regionId;
    private final ScheduledExecutorService scheduler;
    private final Connection nats;
    private final ConcurrentMap<String, Long> recentlyApplied = new ConcurrentHashMap<>();
    private static final long APPLY_DEBOUNCE_MS = 5000;

    public SymcChunkWriteback(@NotNull String regionId,
                                @NotNull ScheduledExecutorService scheduler,
                                @NotNull Connection nats) {
        this.regionId = regionId;
        this.scheduler = scheduler;
        this.nats = nats;
        subscribe();
        LOG.info("[symc] ChunkWriteback v1 started region=" + regionId
                + " subscribe=symc.chunk.>");
    }

    private void subscribe() {
        Dispatcher d = nats.createDispatcher(msg -> handleIncoming(msg));
        d.subscribe("symc.chunk.>");
    }

    private void handleIncoming(@NotNull Message msg) {
        try {
            String subject = msg.getSubject();
            String[] parts = subject.split("\\.");
            if (parts.length < 4) return;
            String otherRegion = parts[2];
            if (otherRegion.equals(regionId)) return;

            if (subject.contains(".loaded.")) {
                String world = parts[parts.length - 3];
                int cx = Integer.parseInt(parts[parts.length - 2]);
                int cz = Integer.parseInt(parts[parts.length - 1]);
                String key = world + ":" + cx + ":" + cz;
                long now = System.currentTimeMillis();
                Long last = recentlyApplied.get(key);
                if (last != null && now - last < APPLY_DEBOUNCE_MS) {
                    return;
                }
                recentlyApplied.put(key, now);
                byte[] data = msg.getData();
                // 异步 parse + 解压,然后主线程 apply
                scheduler.submit(() -> parseAndApply(world, cx, cz, data, otherRegion));
            } else if (subject.contains(".unloaded.")) {
                String world = parts[parts.length - 3];
                int cx = Integer.parseInt(parts[parts.length - 2]);
                int cz = Integer.parseInt(parts[parts.length - 1]);
                // chunk unload 通知:本地 mark stale(未来访问时强制 reload)
                LOG.info("[symc] chunk unload notification: " + world + " (" + cx + "," + cz + ")");
            } else if (subject.contains(".delta.")) {
                // delta event 暂存以备 M13
                LOG.debug("[symc] delta from " + otherRegion);
            }
        } catch (Exception e) {
            LOG.warn("[symc] writeback handleIncoming failed: " + e.getMessage());
        }
    }

    private void parseAndApply(String worldName, int cx, int cz, byte[] data, String fromRegion) {
        // 1. 解压
        java.util.List<int[]> blocks;
        try (DataInputStream in = new DataInputStream(
                new GZIPInputStream(new ByteArrayInputStream(data)))) {
            int count = in.readInt();
            blocks = new java.util.ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                int x = in.readInt();
                int y = in.readShort();
                int z = in.readInt();
                int matOrdinal = in.readInt();
                blocks.add(new int[]{x, y, z, matOrdinal});
            }
        } catch (Exception e) {
            LOG.warn("[symc] parseAndApply gzip failed: " + e.getMessage());
            return;
        }
        if (blocks.isEmpty()) {
            return;
        }
        // 2. 主线程 apply
        Bukkit.getScheduler().runTask(
                Bukkit.getPluginManager().getPlugin("symc"),
                () -> applyToWorld(worldName, cx, cz, blocks, fromRegion)
        );
    }

    private void applyToWorld(String worldName, int cx, int cz, java.util.List<int[]> blocks, String fromRegion) {
        try {
            World world = Bukkit.getWorld(worldName);
            if (world == null) {
                LOG.warn("[symc] unknown world: " + worldName);
                return;
            }
            // 异步 load chunk 后再写(避免 IllegalStateException "Chunk not loaded")
            world.getChunkAtAsync(cx, cz).thenAccept(chunk -> {
                int written = 0;
                for (int[] b : blocks) {
                    int x = b[0], y = b[1], z = b[2];
                    int matOrdinal = b[3];
                    Material mat = ordinalToMaterial(matOrdinal);
                    if (mat == null) continue;
                    try {
                        Block block = chunk.getBlock(x & 15, y, z & 15);
                        block.setType(mat, false); // false = no physics,avoid chain events
                        written++;
                    } catch (Exception e) {
                        // 单个 block 失败不影响其他
                    }
                }
                chunk.unload(true); // force save
                LOG.info("[symc] chunk WRITE from region=" + fromRegion + " "
                        + worldName + " (" + cx + "," + cz + ")"
                        + " blocks=" + blocks.size() + " written=" + written);
            });
        } catch (Exception e) {
            LOG.warn("[symc] applyToWorld failed: " + e.getMessage());
        }
    }

    /**
     * Material ordinal -> Material 映射。
     * 注意:ordinal 不稳定(注册表顺序),生产应改用 Material.matchMaterial(name)
     * 或自定义 ID 表。但单 cluster 内,ordinal 在 server 间一致。
     */
    private static Material ordinalToMaterial(int ordinal) {
        Material[] values = Material.values();
        if (ordinal < 0 || ordinal >= values.length) return null;
        return values[ordinal];
    }
}
