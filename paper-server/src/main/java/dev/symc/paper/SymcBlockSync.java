package dev.symc.paper;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import org.bukkit.Bukkit;
import org.bukkit.Chunk;
import org.bukkit.World;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.block.BlockBreakEvent;
import org.bukkit.event.block.BlockPlaceEvent;
import org.bukkit.event.world.ChunkLoadEvent;
import org.bukkit.event.world.ChunkUnloadEvent;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.zip.GZIPOutputStream;

/**
 * M11 v5: 区块级同步(Chunk-level) — 替代 v4 的 block-level delta。
 *
 * <p>为什么 chunk-level 而不是 block-level:
 * <ul>
 *   <li><b>事件频率</b>:1 setblock 在 Bukkit 触发 7+ physics events(中心 + 6 邻居),链式反应
 *   <li><b>v4 实测</b>:region-b 收到 490 events/sec,rate limit(100/sec)频繁触发
 *   <li><b>chunk 粒度</b>:1 chunk load 1 event,1 chunk unload 1 event,稳定可控
 *   <li><b>权威性</b>:chunk snapshot = 一致状态,vs delta 是中间态
 * </ul>
 *
 * <p>NATS 主题:
 * <ul>
 *   <li>{@code symc.chunk.{region}.loaded.{world}.{x}.{z}} — 整 chunk NBT 同步(此 region load)
 *   <li>{@code symc.chunk.{region}.unloaded.{world}.{x}.{z}} — 通知其他 region 卸载副本
 *   <li>{@code symc.chunk.delta.{region}.{world}.{x}.{y}.{z}} — 实时单 block 变化(可选,默认开)
 * </ul>
 *
 * <p>限流:chunk sync 不限流(每个 chunk load 1 次);block delta 限流 1000/sec/region
 *
 * <p>序列化:用 NBT-like binary format(自写,避开 Mojang 内部 NBT 引用):
 * <ul>
 *   <li>block_count: int32
 *   <li>每个 block: x(int32) + y(int16) + z(int32) + material_id(int32)  (16 bytes)
 *   <li>GZIP 压缩
 * </ul>
 */
public final class SymcBlockSync implements Listener {

    private static final Logger LOG = LoggerFactory.getLogger(SymcBlockSync.class);

    private final String regionId;
    private final ScheduledExecutorService scheduler;
    private final Connection nats;

    private final ConcurrentMap<Long, Integer> rateLimitPerSec = new ConcurrentHashMap<>();
    private static final int MAX_DELTA_PER_SEC = 1000;

    public SymcBlockSync(@NotNull String regionId,
                         @NotNull ScheduledExecutorService scheduler,
                         @NotNull Connection nats) {
        this.regionId = regionId;
        this.scheduler = scheduler;
        this.nats = nats;
        subscribe();
        LOG.info("[symc] BlockSync v5 started region=" + regionId
                + " chunk.subscribe=symc.chunk.> block.subscribe=symc.chunk.delta.>");
    }

    // ---- Chunk-level events(M11 v5 主路径) ----

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onChunkLoad(@NotNull ChunkLoadEvent event) {
        final Chunk chunk = event.getChunk();
        scheduler.submit(() -> publishChunkSnapshot(chunk, "loaded"));
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onChunkUnload(@NotNull ChunkUnloadEvent event) {
        final Chunk chunk = event.getChunk();
        final String world = chunk.getWorld().getName();
        final int cx = chunk.getX();
        final int cz = chunk.getZ();
        // Unload 不传数据,只通知其他 region
        String subject = "symc.chunk." + regionId + ".unloaded." + world + "." + cx + "." + cz;
        try {
            nats.publish(subject, ("unload:" + world + ":" + cx + ":" + cz).getBytes(StandardCharsets.UTF_8));
            LOG.info("[symc] chunk unload notification: " + world + " (" + cx + "," + cz + ")");
        } catch (Exception e) {
            LOG.warn("[symc] chunk unload publish failed: " + e.getMessage());
        }
    }

    private void publishChunkSnapshot(@NotNull Chunk chunk, @NotNull String action) {
        try {
            String world = chunk.getWorld().getName();
            int cx = chunk.getX();
            int cz = chunk.getZ();

            // 序列化 chunk:每个非空 block 16 bytes
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(new GZIPOutputStream(baos))) {
                int blockCount = 0;
                // 用 temporary buffer 收集 blocks,然后写 header
                ByteArrayOutputStream blockBuf = new ByteArrayOutputStream();
                try (DataOutputStream bOut = new DataOutputStream(blockBuf)) {
                    int minY = chunk.getWorld().getMinHeight();
                    int maxY = chunk.getWorld().getMaxHeight();
                    for (int x = 0; x < 16; x++) {
                        for (int z = 0; z < 16; z++) {
                            for (int y = minY; y < maxY; y++) {
                                org.bukkit.block.Block b = chunk.getBlock(x, y, z);
                                if (b.isEmpty()) continue;
                                bOut.writeInt(x + (cx << 4));
                                bOut.writeShort(y);
                                bOut.writeInt(z + (cz << 4));
                                // material id:用 ordinal 简单序列化(注册表顺序,可能跨版本不兼容但单 cluster 一致)
                                bOut.writeInt(b.getType().ordinal());
                                blockCount++;
                            }
                        }
                    }
                }
                out.writeInt(blockCount);
                out.write(blockBuf.toByteArray());
            }
            byte[] payload = baos.toByteArray();

            String subject = "symc.chunk." + regionId + "." + action + "."
                    + world + "." + cx + "." + cz;
            nats.publish(subject, payload);
            LOG.info("[symc] chunk " + action + ": " + world + " (" + cx + "," + cz + ")"
                    + " blocks=" + (payload.length > 0 ? "(see bytes)" : "0")
                    + " bytes=" + payload.length
                    + " subject=" + subject);
        } catch (Exception e) {
            LOG.warn("[symc] chunk " + action + " publish failed: " + e.getMessage());
        }
    }

    // ---- Block-level delta(M11 v5 保留,用于低延迟单 block 同步) ----

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBlockPlace(@NotNull BlockPlaceEvent event) {
        publishDelta(event.getBlock(), "place", event.getPlayer().getName());
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBlockBreak(@NotNull BlockBreakEvent event) {
        publishDelta(event.getBlock(), "break", event.getPlayer().getName());
    }

    private void publishDelta(@NotNull org.bukkit.block.Block block, String action, String player) {
        // 限流 (1k/sec 比 v4 100/sec 宽)
        long sec = System.currentTimeMillis() / 1000;
        int count = rateLimitPerSec.merge(sec, 1, Integer::sum);
        if (count > MAX_DELTA_PER_SEC) {
            // rate limit 触发时只 warn 每秒 1 次(不每 event warn)
            return;
        }
        rateLimitPerSec.entrySet().removeIf(e -> sec - e.getKey() > 5);

        // delta 用 chunk key 而非 block 位置(subject 更短)
        String world = block.getWorld().getName();
        int cx = block.getChunk().getX();
        int cz = block.getChunk().getZ();
        String subject = "symc.chunk.delta." + regionId + "."
                + world + "." + cx + "." + cz;
        String json = "{\"a\":\"" + action + "\","
                + "\"x\":" + block.getX() + ","
                + "\"y\":" + block.getY() + ","
                + "\"z\":" + block.getZ() + ","
                + "\"m\":\"" + block.getType().name() + "\","
                + "\"p\":\"" + player.replace("\"", "\\\"") + "\","
                + "\"t\":" + System.currentTimeMillis() + "}";
        try {
            nats.publish(subject, json.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            LOG.warn("[symc] delta publish failed: " + e.getMessage());
        }
    }

    // ---- Subscribe(other region 的 chunk + delta) ----

    private void subscribe() {
        Dispatcher d = nats.createDispatcher(msg -> handleIncoming(msg));
        // chunk load + unload + delta(全部 symc.chunk.> 模式)
        d.subscribe("symc.chunk.>");
    }

    private void handleIncoming(@NotNull Message msg) {
        try {
            String subject = msg.getSubject();
            // 跳过自己 region
            // subject 格式: symc.chunk.{regionId}.{action}.{world}.{x}.{z}
            String[] parts = subject.split("\\.");
            if (parts.length < 4) return;
            String otherRegion = parts[2];
            if (otherRegion.equals(regionId)) return;

            if (subject.contains(".unloaded.")) {
                // chunk unload 通知,主线程调度(异步 mark for unload)
                String world = parts.length > 4 ? parts[parts.length - 3] : "";
                int cx = parts.length > 5 ? Integer.parseInt(parts[parts.length - 2]) : 0;
                int cz = parts.length > 4 ? Integer.parseInt(parts[parts.length - 1]) : 0;
                Bukkit.getScheduler().runTask(
                        Bukkit.getPluginManager().getPlugin("symc"),
                        () -> onRemoteChunkUnload(world, cx, cz, otherRegion)
                );
            } else if (subject.contains(".loaded.")) {
                // chunk loaded:解压 + 应用到本地 world
                byte[] data = msg.getData();
                String world = parts.length > 4 ? parts[parts.length - 3] : "";
                int cx = parts.length > 5 ? Integer.parseInt(parts[parts.length - 2]) : 0;
                int cz = parts.length > 4 ? Integer.parseInt(parts[parts.length - 1]) : 0;
                Bukkit.getScheduler().runTask(
                        Bukkit.getPluginManager().getPlugin("symc"),
                        () -> onRemoteChunkLoaded(world, cx, cz, data, otherRegion)
                );
            } else if (subject.contains(".delta.")) {
                // delta:实时 block 变化
                String json = new String(msg.getData(), StandardCharsets.UTF_8);
                Bukkit.getScheduler().runTask(
                        Bukkit.getPluginManager().getPlugin("symc"),
                        () -> onRemoteDelta(json, otherRegion)
                );
            }
        } catch (Exception e) {
            LOG.warn("[symc] chunk handleIncoming failed: " + e.getMessage());
        }
    }

    private void onRemoteChunkLoaded(@NotNull String worldName, int cx, int cz,
                                    @NotNull byte[] data, @NotNull String fromRegion) {
        try {
            World world = Bukkit.getWorld(worldName);
            if (world == null) {
                LOG.warn("[symc] unknown world: " + worldName);
                return;
            }
            // 解压 + 应用
            // (简化版:不实际写本地 chunk,只 log。生产需要:
            //  1. 异步加载 chunk via world.getChunkAtAsync(cx, cz)
            //  2. 解析 NBT
            //  3. 写入每个 block
            //  4. markChunkDirty + refresh)
            LOG.info("[symc] chunk load from region=" + fromRegion + " "
                    + worldName + " (" + cx + "," + cz + ")"
                    + " size=" + data.length + " bytes"
                    + " (apply deferred to M12 - chunk write requires main thread chunk API)");
        } catch (Exception e) {
            LOG.warn("[symc] onRemoteChunkLoaded failed: " + e.getMessage());
        }
    }

    private void onRemoteChunkUnload(@NotNull String worldName, int cx, int cz,
                                      @NotNull String fromRegion) {
        // TODO: mark local mirror chunk as stale, force re-load on next access
        LOG.info("[symc] chunk unload from region=" + fromRegion + " "
                + worldName + " (" + cx + "," + cz + ")");
    }

    private void onRemoteDelta(@NotNull String json, @NotNull String fromRegion) {
        try {
            String action = extract(json, "a");
            int x = Integer.parseInt(extract(json, "x"));
            int y = Integer.parseInt(extract(json, "y"));
            int z = Integer.parseInt(extract(json, "z"));
            String material = extract(json, "m");

            // 从 subject 解析 world + chunk
            // subject 格式: symc.chunk.delta.{region}.{world}.{cx}.{cz}
            // 实际 x/z 是绝对坐标,从 world + cx,cz 推 world
            // 这里简化:用 delta 消息里直接发 action="delta" 标记
            // world 不在 delta 里(从 subject 推),所以这里不全 apply
            // 完整版需要 subject 解析 or json 加 world 字段
            LOG.debug("[symc] delta from region=" + fromRegion
                    + " " + action + " " + material + " at (" + x + "," + y + "," + z + ")");
        } catch (Exception e) {
            LOG.warn("[symc] onRemoteDelta failed: " + e.getMessage());
        }
    }

    static String extract(String json, String key) {
        String k = "\"" + key + "\":";
        int i = json.indexOf(k);
        if (i < 0) return "";
        int v = i + k.length();
        while (v < json.length() && json.charAt(v) == ' ') v++;
        if (v >= json.length()) return "";
        if (json.charAt(v) == '"') {
            int end = json.indexOf('"', v + 1);
            if (end < 0) return "";
            return json.substring(v + 1, end);
        } else {
            int end = v;
            while (end < json.length() && "0123456789-.".indexOf(json.charAt(end)) >= 0) end++;
            return json.substring(v, end);
        }
    }
}
