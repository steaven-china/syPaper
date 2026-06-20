package io.papermc.paper.symc;

import org.bukkit.block.Block;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.block.BlockRedstoneEvent;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Logger;

/**
 * D-extra §5.1.1 跨区协作请求协议。
 *
 * <p>由 {@link SymcBootstrap} 构造,接收调度线程池。@EventHandler 异步处理。
 *
 * <p><b>三种请求类型</b>:
 * <ul>
 *   <li>{@code COMPUTATION} — 跨区运算,邻居 region 提供结果</li>
 *   <li>{@code STATE_QUERY} — 只读副本查询,拿邻居 region 的 chunk 快照</li>
 *   <li>{@code WRITE_AUTHORITY_TRANSFER} — 写权转移,cold→hot→primary 漂移</li>
 * </ul>
 */
public final class SymcCooperationRequest implements Listener {

    private static final Logger LOG = Logger.getLogger(SymcCooperationRequest.class.getName());

    private final Map<UUID, PendingRequest> pending = new ConcurrentHashMap<>();
    private final String regionId;
    private final ScheduledExecutorService scheduler;

    public SymcCooperationRequest(@NotNull String regionId,
                                   @NotNull ScheduledExecutorService scheduler) {
        this.regionId = regionId;
        this.scheduler = scheduler;
        LOG.info("[symc] CooperationRequest handler started for region=" + regionId);
    }

    @EventHandler
    public void onRedstone(@NotNull BlockRedstoneEvent event) {
        // 主线程接 event → submit 异步处理
        final Block block = event.getBlock();
        final int oldLevel = event.getOldCurrent();
        final int newLevel = event.getNewCurrent();
        final int x = block.getX(), y = block.getY(), z = block.getZ();

        scheduler.submit(() -> handleRedstoneAsync(x, y, z, oldLevel, newLevel));
    }

    private void handleRedstoneAsync(int x, int y, int z, int oldLevel, int newLevel) {
        if (Math.abs(newLevel - oldLevel) < 8) return;

        // TODO M7: 用 chunk→region 映射表,判断是否真的在边界
        // 当前 stub:直接发
        CooperationRequest req = new CooperationRequest(
                RequestType.COMPUTATION,
                x, y, z,
                "redstone_pulse", "level_change=" + oldLevel + "→" + newLevel,
                System.currentTimeMillis()
        );
        submit(req);
    }

    public void submit(@NotNull CooperationRequest request) {
        pending.put(request.eventId(), new PendingRequest(request, System.currentTimeMillis()));
        // TODO M7: 发送到高速服务(NATS publish → symc.cooperation.{regionId})
        LOG.fine("[symc] CooperationRequest " + request.eventId() +
                " type=" + request.type() + " submitted");
    }

    public PendingRequest poll(@NotNull UUID eventId) {
        return pending.remove(eventId);
    }

    public int pendingCount() { return pending.size(); }

    // ---- 数据类 ----

    public record CooperationRequest(
            @NotNull RequestType type,
            int x, int y, int z,
            @NotNull String eventName,
            @NotNull String payload,
            long timestamp
    ) {
        public UUID eventId() { return UUID.randomUUID(); }
    }

    public record PendingRequest(
            @NotNull CooperationRequest request,
            long submitTimeMs
    ) {}

    public enum RequestType {
        /** 跨区运算请求 */
        COMPUTATION,
        /** 只读副本状态查询 */
        STATE_QUERY,
        /** 写权转移(cold→hot→primary) */
        WRITE_AUTHORITY_TRANSFER
    }
}
