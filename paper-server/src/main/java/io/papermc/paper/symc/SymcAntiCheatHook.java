package io.papermc.paper.symc;

import org.bukkit.Location;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.player.PlayerMoveEvent;
import org.jetbrains.annotations.NotNull;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.logging.Logger;

/**
 * D4 反作弊基线 — 主 region 权威 + 邻居抽查 + 架构层信号时序检测。
 *
 * <p>每个 CompositeEvent 带三个字段:
 * <ul>
 *   <li>{@code OriginRegion} — 谁发的</li>
 *   <li>{@code OriginTick} — 哪个 tick 发的</li>
 *   <li>{@code CausalityHash} — 已知状态下"能不能发生"的指纹</li>
 * </ul>
 *
 * <p>接收 region 校验:事件在已知状态下能不能发生?
 * 不能 → 触发 {@code AntiCheatAnomalyEvent} → 插件决定 ban 不 ban。
 */
public final class SymcAntiCheatHook implements Listener {

    private static final Logger LOG = Logger.getLogger(SymcAntiCheatHook.class.getName());
    private static final MessageDigest SHA256;

    static {
        try {
            SHA256 = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not available", e);
        }
    }

    private final String regionId;

    public SymcAntiCheatHook(@NotNull String regionId) {
        this.regionId = regionId;
        LOG.info("[symc] AntiCheatHook started for region=" + regionId);
    }

    @EventHandler
    public void onPlayerMove(@NotNull PlayerMoveEvent event) {
        Player player = event.getPlayer();
        Location from = event.getFrom();
        Location to = event.getTo();
        if (to == null) return;

        // 8 格以上移动 → 校验时序异常
        double dist = from.distance(to);
        if (dist >= 8.0) {
            byte[] hash = causalityHash(player.getName(), from, to);
            if (!validateCausality(hash, player)) {
                // TODO M7: 触发 AntiCheatAnomalyEvent → 插件订阅
                LOG.warning("[symc] ANTI-CHEAT anomaly: player=" + player.getName() +
                        " moved " + String.format("%.1f", dist) + " blocks in one tick");
            }
        }
    }

    byte[] causalityHash(@NotNull String player, @NotNull Location from, @NotNull Location to) {
        SHA256.reset();
        SHA256.update(player.getBytes());
        SHA256.update(longToBytes((long)from.getX()));
        SHA256.update(longToBytes((long)from.getY()));
        SHA256.update(longToBytes((long)from.getZ()));
        SHA256.update(longToBytes((long)to.getX()));
        SHA256.update(longToBytes((long)to.getY()));
        SHA256.update(longToBytes((long)to.getZ()));
        return SHA256.digest();
    }

    boolean validateCausality(byte[] hash, @NotNull Player player) {
        // TODO M7: 向邻居 region 查询:player 在 OriginTick 的状态能不能产生这个移动?
        // 当前 stub:始终返回 true
        return true;
    }

    private static byte[] longToBytes(long v) {
        return new byte[]{
                (byte)(v >>> 56), (byte)(v >>> 48), (byte)(v >>> 40), (byte)(v >>> 32),
                (byte)(v >>> 24), (byte)(v >>> 16), (byte)(v >>> 8), (byte)v
        };
    }
}
