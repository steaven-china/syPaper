package io.papermc.paper.symc;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.client.Nats;
import org.bukkit.block.Block;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.block.BlockRedstoneEvent;
import org.jetbrains.annotations.NotNull;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Logger;

/**
 * D-extra §5.1.1 跨区协作请求协议(M7 真实网络 sync via NATS)。
 *
 * <p>NATS topic 约定:
 * <ul>
 *   <li><b>subject</b>: {@code symc.cooperation.{regionId}} — region 收自己 topic 上的请求
 *   <li><b>payload</b>: JSON(CooperationRequest)
 *   <li><b>应答</b>: 暂走 fire-and-forget(M7 简单),未来可加 reply subject
 * </ul>
 *
 * <p>三种请求类型:
 * <ul>
 *   <li>{@code COMPUTATION} — 跨区运算,邻居 region 提供结果
 *   <li>{@code STATE_QUERY} — 只读副本查询
 *   <li>{@code WRITE_AUTHORITY_TRANSFER} — 写权漂移
 * </ul>
 */
public final class SymcCooperationRequest implements Listener {

    private static final Logger LOG = Logger.getLogger(SymcCooperationRequest.class.getName());

    private final Map<UUID, PendingRequest> pending = new ConcurrentHashMap<>();
    private final String regionId;
    private final ScheduledExecutorService scheduler;
    private final Connection nats;

    public SymcCooperationRequest(@NotNull String regionId,
                                   @NotNull ScheduledExecutorService scheduler,
                                   @NotNull Connection nats) {
        this.regionId = regionId;
        this.scheduler = scheduler;
        this.nats = nats;
        subscribe();
        LOG.info("[symc] CooperationRequest started region=" + regionId
                + " subject=symc.cooperation." + regionId);
    }

    @EventHandler
    public void onRedstone(@NotNull BlockRedstoneEvent event) {
        final Block block = event.getBlock();
        final int oldLevel = event.getOldCurrent();
        final int newLevel = event.getNewCurrent();
        final int x = block.getX(), y = block.getY(), z = block.getZ();
        scheduler.submit(() -> handleRedstoneAsync(x, y, z, oldLevel, newLevel));
    }

    private void handleRedstoneAsync(int x, int y, int z, int oldLevel, int newLevel) {
        if (Math.abs(newLevel - oldLevel) < 8) return;

        CooperationRequest req = new CooperationRequest(
                RequestType.COMPUTATION,
                x, y, z,
                "redstone_pulse", "level_change=" + oldLevel + "→" + newLevel,
                System.currentTimeMillis()
        );
        publish(req);
    }

    /** 真正发到 NATS subject */
    public void publish(@NotNull CooperationRequest request) {
        pending.put(request.eventId(), new PendingRequest(request, System.currentTimeMillis()));
        try {
            byte[] payload = toJson(request);
            nats.publish(subjectName(), payload);
            LOG.fine("[symc] published " + request.eventId()
                    + " type=" + request.type() + " subject=" + subjectName());
        } catch (Exception e) {
            LOG.warning("[symc] publish failed: " + e.getMessage());
        }
    }

    /** 订阅自己的 subject,收到消息时 submit 到线程池处理 */
    private void subscribe() {
        Dispatcher d = nats.createDispatcher(msg -> handleIncoming(msg));
        d.subscribe(subjectName());
    }

    private void handleIncoming(@NotNull Message msg) {
        try {
            CooperationRequest req = fromJson(new String(msg.getData(), StandardCharsets.UTF_8));
            LOG.fine("[symc] received " + req.eventId() + " type=" + req.type());
            // TODO M7: 根据 type 分发到具体处理(computation/state_query/write_authority_transfer)
            // 当前 stub:记录到 pending 然后丢弃
        } catch (Exception e) {
            LOG.warning("[symc] parse incoming failed: " + e.getMessage());
        }
    }

    public PendingRequest poll(@NotNull UUID eventId) {
        return pending.remove(eventId);
    }

    public int pendingCount() { return pending.size(); }

    public String subjectName() {
        return "symc.cooperation." + regionId;
    }

    // ---- 序列化(简化 JSON,手写不用 jackson 减少 dep) ----

    static String toJson(CooperationRequest r) {
        return "{\"type\":\"" + r.type() + "\","
                + "\"x\":" + r.x() + ","
                + "\"y\":" + r.y() + ","
                + "\"z\":" + r.z() + ","
                + "\"event\":\"" + r.eventName() + "\","
                + "\"payload\":\"" + r.payload() + "\","
                + "\"ts\":" + r.timestamp() + "}";
    }

    static CooperationRequest fromJson(String s) {
        // stub:反序列化逻辑 M7 后续补
        return new CooperationRequest(RequestType.COMPUTATION, 0, 0, 0, "stub", "stub", 0);
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
        COMPUTATION, STATE_QUERY, WRITE_AUTHORITY_TRANSFER
    }
}
