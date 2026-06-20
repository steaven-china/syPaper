package io.papermc.paper.symc;

import org.bukkit.block.Block;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.block.BlockRedstoneEvent;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * D-extra §5.1.1 跨区协作请求协议。
 *
 * <p>当红石/水流/实体跨越 region 边界时,CompositeEvent 被包装为
 * CooperationRequest,通过高速服务(NATS/Redis)通告邻居 region。
 * 接收方判断是否参与、升级为主 region 权威、或拒绝。
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

    public SymcCooperationRequest(@NotNull String regionId) {
        this.regionId = regionId;
        LOG.info("[symc] CooperationRequest handler started for region=" + regionId);
    }

    @EventHandler
    public void onRedstone(@NotNull BlockRedstoneEvent event) {
        Block block = event.getBlock();
        int oldLevel = event.getOldCurrent();
        int newLevel = event.getNewCurrent();

        // 跨区红石检测:信号变化且强度跨过阈值 → 可能影响邻居 region
        if (Math.abs(newLevel - oldLevel) >= 8 && isBoundaryBlock(block)) {
            CooperationRequest req = new CooperationRequest(
                    RequestType.COMPUTATION,
                    block.getX(), block.getY(), block.getZ(),
                    "redstone_pulse", "level_change=" + oldLevel + "→" + newLevel,
                    0
            );
            submit(req);
        }
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

    private boolean isBoundaryBlock(Block block) {
        // TODO M7: 读 chunk→region 映射表,判断 block 是否在边界 2 chunk 内(缓冲带)
        return false; // stub — 当前先返回 false
    }

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
