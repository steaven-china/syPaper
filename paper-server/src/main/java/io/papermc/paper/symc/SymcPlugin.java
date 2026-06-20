package io.papermc.paper.symc;

import org.bukkit.plugin.java.JavaPlugin;

/**
 * Symc Bukkit plugin 入口 — 让 Paper 加载 symc 钩子。
 *
 * <p>Paper 启动时调用 {@link #onEnable()}:
 * <ol>
 *   <li>读 config(region-id, nats-url)</li>
 *   <li>启 {@link SymcBootstrap}(线程池 + NATS connection)</li>
 *   <li>把 3 个 listener 注册到 Bukkit EventBus</li>
 * </ol>
 *
 * <p>Paper 关闭时调用 {@link #onDisable()} 优雅停机。
 */
public final class SymcPlugin extends JavaPlugin {

    @Override
    public void onEnable() {
        // 1. 读 config(默认 region-id=default, nats-url=localhost:4222)
        saveDefaultConfig();
        String regionId = getConfig().getString("region-id", "default");
        String natsUrl = getConfig().getString("nats-url", "nats://localhost:4222");

        // 2. 启 SymcBootstrap
        try {
            SymcBootstrap.start(regionId, natsUrl);
        } catch (Exception e) {
            getLogger().severe("[symc] bootstrap failed: " + e.getMessage());
            getServer().getPluginManager().disablePlugin(this);
            return;
        }

        // 3. 注册 3 个 listener 到 Bukkit EventBus
        SymcBootstrap symc = SymcBootstrap.get();
        getServer().getPluginManager().registerEvents(symc.writeAuthority(), this);
        getServer().getPluginManager().registerEvents(symc.cooperation(), this);
        getServer().getPluginManager().registerEvents(symc.anticheat(), this);

        getLogger().info("[symc] plugin enabled: region=" + regionId + " nats=" + natsUrl);
    }

    @Override
    public void onDisable() {
        SymcBootstrap.stop();
        getLogger().info("[symc] plugin disabled");
    }
}
